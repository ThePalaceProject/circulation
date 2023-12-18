from __future__ import annotations

import argparse
import csv
import datetime
import json
import logging
import re
import time
import urllib.parse
from threading import RLock
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

import dateutil
import flask
import isbnlib
from dependency_injector.wiring import Provide, inject
from flask_babel import lazy_gettext as _
from requests import Response
from requests.structures import CaseInsensitiveDict
from sqlalchemy import select
from sqlalchemy.orm import Query, Session
from sqlalchemy.orm.exc import StaleDataError

from api.circulation import (
    BaseCirculationAPI,
    BaseCirculationApiSettings,
    BaseCirculationEbookLoanSettings,
    CirculationInternalFormatsMixin,
    DeliveryMechanismInfo,
    FulfillmentInfo,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
)
from api.circulation_exceptions import *
from api.circulation_exceptions import CannotFulfill
from api.selftest import HasCollectionSelfTests, SelfTestResult
from core.analytics import Analytics
from core.config import CannotLoadConfiguration, Configuration
from core.connection_config import ConnectionSetting
from core.coverage import BibliographicCoverageProvider
from core.integration.base import (
    HasChildIntegrationConfiguration,
    integration_settings_update,
)
from core.integration.goals import Goals
from core.integration.settings import (
    BaseSettings,
    ConfigurationFormItem,
    ConfigurationFormItemType,
    FormField,
)
from core.metadata_layer import (
    CirculationData,
    ContributorData,
    FormatData,
    IdentifierData,
    LinkData,
    MeasurementData,
    Metadata,
    ReplacementPolicy,
    SubjectData,
    TimestampData,
)
from core.model import (
    Classification,
    Collection,
    Contributor,
    Credential,
    DataSource,
    DeliveryMechanism,
    Edition,
    ExternalIntegration,
    Hyperlink,
    Identifier,
    IntegrationConfiguration,
    LicensePool,
    LicensePoolDeliveryMechanism,
    Measurement,
    MediaTypes,
    Patron,
    Representation,
    Subject,
)
from core.monitor import CollectionMonitor, IdentifierSweepMonitor, TimelineMonitor
from core.scripts import InputScript, Script
from core.service.container import Services
from core.util import base64
from core.util.datetime_helpers import strptime_utc, utc_now
from core.util.http import HTTP, BadResponseException
from core.util.log import LoggerMixin


class OverdriveConstants:
    PRODUCTION_SERVERS = "production"
    TESTING_SERVERS = "testing"

    # The formats we care about.
    FORMATS = "ebook-epub-open,ebook-epub-adobe,ebook-pdf-adobe,ebook-pdf-open,audiobook-overdrive".split(
        ","
    )

    # These are not real Overdrive formats; we use them internally so
    # we can distinguish between (e.g.) using "audiobook-overdrive"
    # to get into Overdrive Read, and using it to get a link to a
    # manifest file.
    MANIFEST_INTERNAL_FORMATS = {
        "audiobook-overdrive-manifest",
        "ebook-overdrive-manifest",
    }

    # These formats can be delivered either as manifest files or as
    # links to websites that stream the content.
    STREAMING_FORMATS = [
        "ebook-overdrive",
        "audiobook-overdrive",
    ]

    # When associating an Overdrive account with a library, it's
    # necessary to also specify an "ILS name" obtained from
    # Overdrive. Components that don't authenticate patrons (such as
    # the metadata wrangler) don't need to set this value.
    ILS_NAME_KEY = "ils_name"
    ILS_NAME_DEFAULT = "default"


class OverdriveSettings(ConnectionSetting, BaseCirculationApiSettings):
    """The basic Overdrive configuration"""

    external_account_id: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Library ID"),
            type=ConfigurationFormItemType.TEXT,
            description="The library identifier.",
            required=True,
        ),
    )
    overdrive_website_id: str = FormField(
        form=ConfigurationFormItem(
            label=_("Website ID"),
            type=ConfigurationFormItemType.TEXT,
            description="The web site identifier.",
            required=True,
        )
    )
    overdrive_client_key: str = FormField(
        form=ConfigurationFormItem(
            label=_("Client Key"),
            type=ConfigurationFormItemType.TEXT,
            description="The Overdrive client key.",
            required=True,
        )
    )
    overdrive_client_secret: str = FormField(
        form=ConfigurationFormItem(
            label=_("Client Secret"),
            type=ConfigurationFormItemType.TEXT,
            description="The Overdrive client secret.",
            required=True,
        )
    )

    overdrive_server_nickname: str = FormField(
        default=OverdriveConstants.PRODUCTION_SERVERS,
        form=ConfigurationFormItem(
            label=_("Server family"),
            type=ConfigurationFormItemType.SELECT,
            required=False,
            description="Unless you hear otherwise from Overdrive, your integration should use their production servers.",
            options={
                OverdriveConstants.PRODUCTION_SERVERS: ("Production"),
                OverdriveConstants.TESTING_SERVERS: _("Testing"),
            },
        ),
    )


class OverdriveLibrarySettings(BaseCirculationEbookLoanSettings):
    ils_name: str = FormField(
        default=OverdriveConstants.ILS_NAME_DEFAULT,
        form=ConfigurationFormItem(
            label=_("ILS Name"),
            description=_(
                "When multiple libraries share an Overdrive account, Overdrive uses a setting called 'ILS Name' to determine which ILS to check when validating a given patron."
            ),
        ),
    )


class OverdriveChildSettings(BaseSettings):
    external_account_id: str | None = FormField(
        form=ConfigurationFormItem(
            label=_("Library ID"),
            required=True,
        )
    )


class OverdriveAPI(
    PatronActivityCirculationAPI[OverdriveSettings, OverdriveLibrarySettings],
    CirculationInternalFormatsMixin,
    HasCollectionSelfTests,
    HasChildIntegrationConfiguration[OverdriveSettings, OverdriveChildSettings],
    OverdriveConstants,
):
    SET_DELIVERY_MECHANISM_AT = BaseCirculationAPI.FULFILL_STEP

    # Create a lookup table between common DeliveryMechanism identifiers
    # and Overdrive format types.
    epub = Representation.EPUB_MEDIA_TYPE
    pdf = Representation.PDF_MEDIA_TYPE
    adobe_drm = DeliveryMechanism.ADOBE_DRM
    no_drm = DeliveryMechanism.NO_DRM
    streaming_drm = DeliveryMechanism.STREAMING_DRM
    streaming_text = DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE
    streaming_audio = DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE
    overdrive_audiobook_manifest = MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE
    libby_drm = DeliveryMechanism.LIBBY_DRM

    # When a request comes in for a given DeliveryMechanism, what
    # do we tell Overdrive?
    delivery_mechanism_to_internal_format = {
        (epub, no_drm): "ebook-epub-open",
        (epub, adobe_drm): "ebook-epub-adobe",
        (pdf, no_drm): "ebook-pdf-open",
        (pdf, adobe_drm): "ebook-pdf-adobe",
        (streaming_text, streaming_drm): "ebook-overdrive",
        (streaming_audio, streaming_drm): "audiobook-overdrive",
        (overdrive_audiobook_manifest, libby_drm): "audiobook-overdrive-manifest",
    }

    # Once you choose a non-streaming format you're locked into it and can't
    # use other formats.
    LOCK_IN_FORMATS = [
        x
        for x in OverdriveConstants.FORMATS
        if x not in OverdriveConstants.STREAMING_FORMATS
        and x not in OverdriveConstants.MANIFEST_INTERNAL_FORMATS
    ]

    # TODO: This is a terrible choice but this URL should never be
    # displayed to a patron, so it doesn't matter much.
    DEFAULT_ERROR_URL = "http://librarysimplified.org/"

    # Map Overdrive's error messages to standard circulation manager
    # exceptions.
    ERROR_MESSAGE_TO_EXCEPTION = {
        "PatronHasExceededCheckoutLimit": PatronLoanLimitReached,
        "PatronHasExceededCheckoutLimit_ForCPC": PatronLoanLimitReached,
    }

    # An OverDrive defined constant indicating the "main" or parent account
    # associated with an OverDrive collection.
    OVERDRIVE_MAIN_ACCOUNT_ID = -1

    # A lock for threaded usage.
    lock = RLock()

    # Production and testing have different host names for some of the
    # API endpoints. This is configurable on the collection level.
    HOSTS = {
        OverdriveConstants.PRODUCTION_SERVERS: dict(
            host="https://api.overdrive.com",
            patron_host="https://patron.api.overdrive.com",
        ),
        OverdriveConstants.TESTING_SERVERS: dict(
            host="https://integration.api.overdrive.com",
            patron_host="https://integration-patron.api.overdrive.com",
        ),
    }

    # Production and testing setups use the same URLs for Client
    # Authentication and Patron Authentication, but we use the same
    # system as for other hostnames to give a consistent look to the
    # templates.
    for host in list(HOSTS.values()):
        host["oauth_patron_host"] = "https://oauth-patron.overdrive.com"
        host["oauth_host"] = "https://oauth.overdrive.com"

    # Each of these endpoint URLs has a slot to plug in one of the
    # appropriate servers. This will be filled in either by a call to
    # the endpoint() method (if there are other variables in the
    # template), or by the _do_get or _do_post methods (if there are
    # no other variables).
    TOKEN_ENDPOINT = "%(oauth_host)s/token"
    PATRON_TOKEN_ENDPOINT = "%(oauth_patron_host)s/patrontoken"

    LIBRARY_ENDPOINT = "%(host)s/v1/libraries/%(library_id)s"
    ADVANTAGE_LIBRARY_ENDPOINT = (
        "%(host)s/v1/libraries/%(parent_library_id)s/advantageAccounts/%(library_id)s"
    )
    ALL_PRODUCTS_ENDPOINT = (
        "%(host)s/v1/collections/%(collection_token)s/products?sort=%(sort)s"
    )
    METADATA_ENDPOINT = (
        "%(host)s/v1/collections/%(collection_token)s/products/%(item_id)s/metadata"
    )
    EVENTS_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products?lastUpdateTime=%(lastupdatetime)s&sort=%(sort)s&limit=%(limit)s"
    AVAILABILITY_ENDPOINT = "%(host)s/v2/collections/%(collection_token)s/products/%(product_id)s/availability"

    PATRON_INFORMATION_ENDPOINT = "%(patron_host)s/v1/patrons/me"
    CHECKOUTS_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s"
    FORMATS_ENDPOINT = (
        "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s/formats"
    )
    HOLDS_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds"
    HOLD_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds/%(product_id)s"
    ME_ENDPOINT = "%(patron_host)s/v1/patrons/me"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)

    # The formats that can be read by the default Library Simplified reader.
    DEFAULT_READABLE_FORMATS = {
        "ebook-epub-open",
        "ebook-epub-adobe",
        "ebook-pdf-open",
        "audiobook-overdrive",
    }

    # The formats that indicate the book has been fulfilled on an
    # incompatible platform and just can't be fulfilled on Simplified
    # in any format.
    INCOMPATIBLE_PLATFORM_FORMATS = {"ebook-kindle"}

    OVERDRIVE_READ_FORMAT = "ebook-overdrive"

    TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    @classmethod
    def settings_class(cls):
        return OverdriveSettings

    @classmethod
    def library_settings_class(cls):
        return OverdriveLibrarySettings

    @classmethod
    def child_settings_class(cls):
        return OverdriveChildSettings

    @classmethod
    def label(cls):
        return ExternalIntegration.OVERDRIVE

    @classmethod
    def description(cls):
        return "Integrate an Overdrive collection. For an Overdrive Advantage collection, select the consortium's Overdrive collection as the parent."

    def __init__(self, _db, collection):
        super().__init__(_db, collection)
        if collection.protocol != ExternalIntegration.OVERDRIVE:
            raise ValueError(
                "Collection protocol is %s, but passed into OverdriveAPI!"
                % collection.protocol
            )

        if collection.parent:
            # This is an Overdrive Advantage account.
            parent_settings = self.settings_load(
                collection.parent.integration_configuration
            )
            self.parent_library_id = parent_settings.external_account_id

            # We're going to inherit all of the Overdrive credentials
            # from the parent (the main Overdrive account), except for the
            # library ID, which we already set.
            self._settings = self.settings_load(
                collection.integration_configuration,
                collection.parent.integration_configuration,
            )
        else:
            self.parent_library_id = None
            self._settings = self.settings_load(collection.integration_configuration)

        self._library_id = self._settings.external_account_id
        if not self._library_id:
            raise ValueError(
                "Collection %s must have an external account ID" % collection.id
            )

        if not self._settings.overdrive_client_key:
            raise CannotLoadConfiguration("Overdrive client key is not configured")
        if not self._settings.overdrive_client_secret:
            raise CannotLoadConfiguration(
                "Overdrive client password/secret is not configured"
            )
        if not self._settings.overdrive_website_id:
            raise CannotLoadConfiguration("Overdrive website ID is not configured")

        self._server_nickname = self._settings.overdrive_server_nickname

        self._hosts = self._determine_hosts(server_nickname=self._server_nickname)

        # This is set by an access to .token, or by a call to
        # check_creds() or refresh_creds().
        self._token = None

        # This is set by an access to .collection_token
        self._collection_token = None
        self.overdrive_bibliographic_coverage_provider = (
            OverdriveBibliographicCoverageProvider(collection, api_class=self)
        )

    @property
    def settings(self) -> OverdriveSettings:
        return self._settings

    def _determine_hosts(self, *, server_nickname: str) -> dict[str, str]:
        # Figure out which hostnames we'll be using when constructing
        # endpoint URLs.
        if server_nickname not in self.HOSTS:
            server_nickname = OverdriveConstants.PRODUCTION_SERVERS

        return dict(self.HOSTS[server_nickname])

    def endpoint(self, url: str, **kwargs) -> str:
        """Create the URL to an Overdrive API endpoint.

        :param url: A template for the URL.
        :param kwargs: Arguments to be interpolated into the template.
           The server hostname will be interpolated automatically; you
           don't have to pass it in.
        """
        if not "%(" in url:
            # Nothing to interpolate.
            return url
        kwargs.update(self._hosts)
        return url % kwargs

    @property
    def token(self):
        if not self._token:
            self.check_creds()
        return self._token

    @property
    def collection_token(self):
        """Get the token representing this particular Overdrive collection.

        As a side effect, this will verify that the Overdrive
        credentials are working.
        """
        if not self._collection_token:
            self.check_creds()
            library = self.get_library()
            error = library.get("errorCode")
            if error:
                message = library.get("message")
                raise CannotLoadConfiguration(
                    "Overdrive credentials are valid but could not fetch library: %s"
                    % message
                )
            self._collection_token = library["collectionToken"]
        return self._collection_token

    @property
    def source(self):
        return DataSource.lookup(self._db, DataSource.OVERDRIVE)

    def ils_name(self, library):
        """Determine the ILS name to use for the given Library."""
        config = self.integration_configuration().for_library(library.id)
        if not config:
            return self.ILS_NAME_DEFAULT
        return self.library_settings_load(config).ils_name

    @property
    def advantage_library_id(self):
        """The library ID for this library, as we should look for it in
        certain API documents served by Overdrive.

        For ordinary collections (ie non-Advantage) with or without associated
        Advantage (ie child) collections shared among libraries, this will be
        equal to the OVERDRIVE_MAIN_ACCOUNT_ID.

        For Overdrive Advantage accounts, this will be the numeric
        value of the Overdrive library ID.
        """
        if self.parent_library_id is None:
            # This is not an Overdrive Advantage collection.
            #
            # Instead of looking for the library ID itself in these
            # documents, we should look for the constant main account id.
            return self.OVERDRIVE_MAIN_ACCOUNT_ID
        return int(self._library_id)

    def check_creds(self, force_refresh=False):
        """If the Bearer Token has expired, update it."""
        with self.lock:
            refresh_on_lookup = self.refresh_creds
            if force_refresh:
                refresh_on_lookup = lambda x: x

            credential = self.credential_object(refresh_on_lookup)
            if force_refresh:
                self.refresh_creds(credential)
            self._token = credential.credential

    def credential_object(self, refresh):
        """Look up the Credential object that allows us to use
        the Overdrive API.
        """
        return Credential.lookup(
            self._db,
            DataSource.OVERDRIVE,
            None,
            None,
            refresh,
            collection=self.collection,
        )

    def refresh_creds(self, credential):
        """Fetch a new Bearer Token and update the given Credential object."""
        response = self.token_post(
            self.TOKEN_ENDPOINT,
            dict(grant_type="client_credentials"),
            allowed_response_codes=[200],
        )
        data = response.json()
        self._update_credential(credential, data)
        self._token = credential.credential

    def get(
        self, url: str, extra_headers={}, exception_on_401=False
    ) -> tuple[int, CaseInsensitiveDict, bytes]:
        """Make an HTTP GET request using the active Bearer Token."""
        request_headers = dict(Authorization="Bearer %s" % self.token)
        request_headers.update(extra_headers)

        response: Response = self._do_get(
            url, request_headers, allowed_response_codes=["2xx", "3xx", "401", "404"]
        )
        status_code: int = response.status_code
        headers: CaseInsensitiveDict = response.headers
        content: bytes = response.content

        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise BadResponseException.from_response(
                    url,
                    "Something's wrong with the Overdrive OAuth Bearer Token!",
                    (status_code, headers, content),
                )
            else:
                # Refresh the token and try again.
                self.check_creds(True)
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    @property
    def token_authorization_header(self) -> str:
        s = b"%s:%s" % (self.client_key(), self.client_secret())
        return "Basic " + base64.standard_b64encode(s).strip()

    @property
    def fulfillment_authorization_header(self) -> str:
        is_test_mode = (
            True
            if self._server_nickname == OverdriveConstants.TESTING_SERVERS
            else False
        )
        try:
            client_credentials = Configuration.overdrive_fulfillment_keys(
                testing=is_test_mode
            )
        except CannotLoadConfiguration as e:
            raise CannotFulfill(*e.args)

        s = b"%s:%s" % (
            client_credentials["key"].encode(),
            client_credentials["secret"].encode(),
        )
        return "Basic " + base64.standard_b64encode(s).strip()

    def token_post(
        self,
        url: str,
        payload: dict[str, str],
        is_fulfillment=False,
        headers={},
        **kwargs,
    ) -> Response:
        """Make an HTTP POST request for purposes of getting an OAuth token."""
        headers = dict(headers)
        headers["Authorization"] = (
            self.token_authorization_header
            if not is_fulfillment
            else self.fulfillment_authorization_header
        )
        return self._do_post(url, payload, headers, **kwargs)

    @staticmethod
    def _update_credential(credential, overdrive_data):
        """Copy Overdrive OAuth data into a Credential object."""
        credential.credential = overdrive_data["access_token"]
        expires_in = overdrive_data["expires_in"] * 0.9
        credential.expires = utc_now() + datetime.timedelta(seconds=expires_in)

    @property
    def _library_endpoint(self) -> str:
        """Which URL should we go to to get information about this collection?

        If this is an ordinary Overdrive account, we get information
        from LIBRARY_ENDPOINT.

        If this is an Overdrive Advantage account, we get information
        from LIBRARY_ADVANTAGE_ENDPOINT.
        """
        args = dict(library_id=self._library_id)
        if self.parent_library_id:
            # This is an Overdrive advantage account.
            args["parent_library_id"] = self.parent_library_id
            endpoint = self.ADVANTAGE_LIBRARY_ENDPOINT
        else:
            endpoint = self.LIBRARY_ENDPOINT
        return self.endpoint(endpoint, **args)

    def get_library(self):
        """Get basic information about the collection, including
        a link to the titles in the collection.
        """
        url = self._library_endpoint
        with self.lock:
            representation, cached = Representation.get(
                self._db,
                url,
                self.get,
                exception_handler=Representation.reraise_exception,
            )
            return json.loads(representation.content)

    def get_advantage_accounts(self):
        """Find all the Overdrive Advantage accounts managed by this library.

        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        library = self.get_library()
        links = library.get("links", {})
        advantage = links.get("advantageAccounts")
        if not advantage:
            return []
        if advantage:
            # This library has Overdrive Advantage accounts, or at
            # least a link where some may be found.
            advantage_url = advantage.get("href")
            if not advantage_url:
                return
            representation, cached = Representation.get(
                self._db,
                advantage_url,
                self.get,
                exception_handler=Representation.reraise_exception,
            )
            return OverdriveAdvantageAccount.from_representation(representation.content)

    def all_ids(self):
        """Get IDs for every book in the system, with the most recently added
        ones at the front.
        """
        next_link = self._all_products_link
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link, "next")

            yield from page_inventory

    @property
    def _all_products_link(self) -> str:
        url = self.endpoint(
            self.ALL_PRODUCTS_ENDPOINT,
            collection_token=self.collection_token,
            sort="dateAdded:desc",
        )
        return self.make_link_safe(url)

    def _get_book_list_page(self, link, rel_to_follow="next", extractor_class=None):
        """Process a page of inventory whose circulation we need to check.

        Returns a 2-tuple: (availability_info, next_link).
        `availability_info` is a list of dictionaries, each containing
           basic availability and bibliographic information about
           one book.
        `next_link` is a link to the next page of results.
        """
        extractor_class = extractor_class or OverdriveRepresentationExtractor
        # We don't cache this because it changes constantly.
        status_code, headers, content = self.get(link, {})
        if isinstance(content, (bytes, str)):
            content = json.loads(content)

        # Find the link to the next page of results, if any.
        next_link = extractor_class.link(content, rel_to_follow)

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = extractor_class.availability_link_list(content)
        return availability_queue, next_link

    def recently_changed_ids(self, start, cutoff):
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start - self.EVENT_DELAY
        self.log.info("Asking for circulation changes since %s", last_update_time)
        last_update = last_update_time.strftime(self.TIME_FORMAT)

        next_link = self.endpoint(
            self.EVENTS_ENDPOINT,
            lastupdatetime=last_update,
            sort="popularity:desc",
            limit=self.PAGE_SIZE_LIMIT,
            collection_token=self.collection_token,
        )
        next_link = self.make_link_safe(next_link)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            yield from page_inventory

    def metadata_lookup(self, identifier):
        """Look up metadata for an Overdrive identifier."""
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier.identifier,
        )
        status_code, headers, content = self.get(url, {})
        if isinstance(content, (bytes, str)):
            content = json.loads(content)
        return content

    def metadata_lookup_obj(self, identifier):
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier,
        )
        status_code, headers, content = self.get(url, {})
        if isinstance(content, (bytes, str)):
            content = json.loads(content)
        return OverdriveRepresentationExtractor.book_info_to_metadata(content)

    @classmethod
    def make_link_safe(cls, url: str) -> str:
        """Turn a server-provided link into a link the server will accept!

        The {} part is completely obnoxious and I have complained about it to
        Overdrive.

        The availability part is to make sure we always use v2 of the
        availability API, even if Overdrive sent us a link to v1.
        """
        parts = list(urlsplit(url))
        parts[2] = quote(parts[2])
        endings = ("/availability", "/availability/")
        if parts[2].startswith("/v1/collections/") and any(
            parts[2].endswith(x) for x in endings
        ):
            parts[2] = parts[2].replace("/v1/collections/", "/v2/collections/", 1)
        query_string = parts[3]
        query_string = query_string.replace("+", "%2B")
        query_string = query_string.replace(":", "%3A")
        query_string = query_string.replace("{", "%7B")
        query_string = query_string.replace("}", "%7D")
        parts[3] = query_string
        return urlunsplit(tuple(parts))

    def _do_get(self, url: str, headers, **kwargs) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self.settings.max_retry_count
        kwargs["timeout"] = 120
        return HTTP.get_with_timeout(url, headers=headers, **kwargs)

    def _do_post(self, url: str, payload, headers, **kwargs) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self.settings.max_retry_count
        kwargs["timeout"] = 120
        return HTTP.post_with_timeout(url, payload, headers=headers, **kwargs)

    def website_id(self) -> bytes:
        return self.settings.overdrive_website_id.encode("utf-8")

    def client_key(self) -> bytes:
        return self.settings.overdrive_client_key.encode("utf-8")

    def client_secret(self) -> bytes:
        return self.settings.overdrive_client_secret.encode("utf-8")

    def library_id(self) -> str:
        return self._library_id

    def hosts(self) -> dict[str, str]:
        return dict(self._hosts)

    def _run_self_tests(self, _db):
        result = self.run_test(
            "Checking global Client Authentication privileges",
            self.check_creds,
            force_refresh=True,
        )
        yield result
        if not result.success:
            # There is no point in running the other tests if we
            # can't even get a token.
            return

        def _count_advantage():
            """Count the Overdrive Advantage accounts"""
            accounts = list(self.get_advantage_accounts())
            return "Found %d Overdrive Advantage account(s)." % len(accounts)

        yield self.run_test("Looking up Overdrive Advantage accounts", _count_advantage)

        def _count_books():
            """Count the titles in the collection."""
            url = self._all_products_link
            status, headers, body = self.get(url, {})
            body = json.loads(body)
            return "%d item(s) in collection" % body["totalItems"]

        yield self.run_test("Counting size of collection", _count_books)

        default_patrons = []
        for result in self.default_patrons(self.collection):
            if isinstance(result, SelfTestResult):
                yield result
                continue
            library, patron, pin = result
            task = (
                "Checking Patron Authentication privileges, using test patron for library %s"
                % library.name
            )
            yield self.run_test(task, self.get_patron_credential, patron, pin)

    def patron_request(
        self,
        patron,
        pin,
        url,
        extra_headers={},
        data=None,
        exception_on_401=False,
        method=None,
        is_fulfillment=False,
    ):
        """Make an HTTP request on behalf of a patron.

        If is_fulfillment==True, then the request will be performed in the context of our
        fulfillment client credentials. Otherwise, it will be performed in the context of
        the collection client credentials.

        The results are never cached.
        """
        patron_credential = self.get_patron_credential(
            patron, pin, is_fulfillment=is_fulfillment
        )
        headers = dict(Authorization="Bearer %s" % patron_credential.credential)
        headers.update(extra_headers)
        if method and method.lower() in ("get", "post", "put", "delete"):
            method = method.lower()
        else:
            if data:
                method = "post"
            else:
                method = "get"
        url = self.endpoint(url)
        response = HTTP.request_with_timeout(method, url, headers=headers, data=data)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise IntegrationException(
                    "Something's wrong with the patron OAuth Bearer Token!"
                )
            else:
                # Refresh the token and try again.
                self.refresh_patron_access_token(patron_credential, patron, pin)
                return self.patron_request(patron, pin, url, extra_headers, data, True)
        else:
            # This is commented out because it may expose patron
            # information.
            #
            # self.log.debug("%s: %s", url, response.status_code)
            return response

    def get_patron_credential(
        self, patron: Patron, pin: str | None, is_fulfillment=False
    ) -> Credential:
        """Create an OAuth token for the given patron.

        :param patron: The patron for whom to fetch the credential.
        :param pin: The patron's PIN or password.
        :param is_fulfillment: Boolean indicating whether we need a fulfillment credential.
        """

        def refresh(credential):
            return self.refresh_patron_access_token(
                credential, patron, pin, is_fulfillment=is_fulfillment
            )

        return Credential.lookup(
            self._db,
            DataSource.OVERDRIVE,
            "Fulfillment OAuth Token" if is_fulfillment else "OAuth Token",
            patron,
            refresh,
            collection=self.collection,
        )

    def scope_string(self, library):
        """Create the Overdrive scope string for the given library.

        This is used when setting up Patron Authentication, and when
        generating the X-Overdrive-Scope header used by SimplyE to set up
        its own Patron Authentication.
        """
        return "websiteid:{} authorizationname:{}".format(
            self.settings.overdrive_website_id,
            self.ils_name(library),
        )

    def refresh_patron_access_token(
        self, credential, patron, pin, is_fulfillment=False
    ):
        """Request an OAuth bearer token that allows us to act on
        behalf of a specific patron.

        Documentation: https://developer.overdrive.com/apis/patron-auth
        """
        payload = dict(
            grant_type="password",
            username=patron.authorization_identifier,
            scope=self.scope_string(patron.library),
        )
        if pin:
            # A PIN was provided.
            payload["password"] = pin
        else:
            # No PIN was provided. Depending on the library,
            # this might be fine. If it's not fine, Overdrive will
            # refuse to issue a token.
            payload["password_required"] = "false"
            payload["password"] = "[ignore]"
        response = self.token_post(
            self.PATRON_TOKEN_ENDPOINT, payload, is_fulfillment=is_fulfillment
        )
        if response.status_code == 200:
            self._update_credential(credential, response.json())
        elif response.status_code == 400:
            response = response.json()
            message = response["error"]
            error = response.get("error_description")
            if error:
                message += "/" + error
            diagnostic = None
            debug = message
            if error == "Requested record not found":
                debug = "The patron failed Overdrive's cross-check against the library's ILS."
            raise PatronAuthorizationFailedException(message, debug)
        return credential

    def checkout(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> LoanInfo:
        """Check out a book on behalf of a patron.

        :param patron: a Patron object for the patron who wants
            to check out the book.

        :param pin: The patron's alleged password.

        :param licensepool: Identifier of the book to be checked out is
            attached to this licensepool.

        :param delivery_mechanism: Represents the patron's desired book format.

        :return: a LoanInfo object.
        """

        identifier = licensepool.identifier
        overdrive_id = identifier.identifier
        headers = {"Content-Type": "application/json"}
        payload_dict = dict(fields=[dict(name="reserveId", value=overdrive_id)])
        payload = json.dumps(payload_dict)

        response = self.patron_request(
            patron, pin, self.CHECKOUTS_ENDPOINT, extra_headers=headers, data=payload
        )
        data = response.json()
        if response.status_code == 400:
            return self._process_checkout_error(patron, pin, licensepool, data)
        else:
            # Try to extract the expiration date from the response.
            expires = self.extract_expiration_date(data)

        # Create the loan info.
        loan = LoanInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            None,
            expires,
            None,
        )
        return loan

    def _process_checkout_error(self, patron, pin, licensepool, error):
        """Handle an error received by the API checkout endpoint.

        :param patron: The Patron who tried to check out the book.
        :param pin: The Patron's PIN; used in case follow-up
            API requests are necessary.
        :param licensepool: LicensePool for the book that was to be borrowed.
        :param error: A dictionary representing the error response, parsed as JSON.
        """
        code = "Unknown Error"
        identifier = licensepool.identifier
        if isinstance(error, dict):
            code = error.get("errorCode", code)
        if code == "NoCopiesAvailable":
            # Clearly our info is out of date.
            self.update_licensepool(identifier.identifier)
            raise NoAvailableCopies()

        if code == "TitleAlreadyCheckedOut":
            # Client should have used a fulfill link instead, but
            # we can handle it.
            #
            # NOTE: It's very unlikely this will happen, but it could
            # happen if the patron borrows a book through Libby and
            # then immediately borrows the same book through SimplyE.
            loan = self.get_loan(patron, pin, identifier.identifier)
            expires = self.extract_expiration_date(loan)
            return LoanInfo(
                licensepool.collection,
                licensepool.data_source.name,
                identifier.type,
                identifier.identifier,
                None,
                expires,
                None,
            )

        if code in self.ERROR_MESSAGE_TO_EXCEPTION:
            exc_class = self.ERROR_MESSAGE_TO_EXCEPTION[code]
            raise exc_class()

        # All-purpose fallback
        raise CannotLoan(code)

    def checkin(self, patron, pin, licensepool):
        # Get the loan for this patron to see whether or not they
        # have a delivery mechanism recorded.
        loan = None
        loans = [l for l in patron.loans if l.license_pool == licensepool]
        if loans:
            loan = loans[0]
        if (
            loan
            and loan.fulfillment
            and loan.fulfillment.delivery_mechanism
            and loan.fulfillment.delivery_mechanism.drm_scheme
            == DeliveryMechanism.NO_DRM
        ):
            # This patron fulfilled this loan without DRM. That means we
            # should be able to find a loanEarlyReturnURL and hit it.
            if self.perform_early_return(patron, pin, loan):
                # No need for the fallback strategy.
                return

        # Our fallback strategy is to DELETE the checkout endpoint.
        # We do this if no loan can be found, no delivery mechanism is
        # recorded, the delivery mechanism uses DRM, we are unable to
        # locate the return URL, or we encounter a problem using the
        # return URL.
        #
        # The only case where this is likely to work is when the
        # loan exists but has not been locked to a delivery mechanism.
        overdrive_id = licensepool.identifier.identifier
        url = self.endpoint(self.CHECKOUT_ENDPOINT, overdrive_id=overdrive_id)
        return self.patron_request(patron, pin, url, method="DELETE")

    def perform_early_return(self, patron, pin, loan, http_get=None):
        """Ask Overdrive for a loanEarlyReturnURL for the given loan
        and try to hit that URL.

        :param patron: A Patron
        :param pin: Authorization PIN for the patron
        :param loan: A Loan object corresponding to the title on loan.
        :param http_get: You may pass in a mock of HTTP.get_with_timeout
            for use in tests.
        """
        mechanism = loan.fulfillment.delivery_mechanism
        internal_format = self.delivery_mechanism_to_internal_format.get(
            (mechanism.content_type, mechanism.drm_scheme)
        )
        if not internal_format:
            # Something's wrong in general, but in particular we don't know
            # which fulfillment link to ask for. Bail out.
            return False

        # Ask Overdrive for a link that can be used to fulfill the book
        # (but which may also contain an early return URL).
        url, media_type = self.get_fulfillment_link(
            patron, pin, loan.license_pool.identifier.identifier, internal_format
        )
        # The URL comes from Overdrive, so it probably doesn't need
        # interpolation, but just in case.
        url = self.endpoint(url)

        # Make a regular, non-authenticated request to the fulfillment link.
        http_get = http_get or HTTP.get_with_timeout
        response = http_get(url, allow_redirects=False)
        location = response.headers.get("location")

        # Try to find an early return URL in the Location header
        # sent from the fulfillment request.
        early_return_url = self._extract_early_return_url(location)
        if early_return_url:
            response = http_get(early_return_url)
            if response.status_code == 200:
                return True
        return False

    @classmethod
    def _extract_early_return_url(cls, location):
        """Extract an early return URL from the URL Overdrive sends to
        fulfill a non-DRMed book.

        :param location: A URL found in a Location header.
        """
        if not location:
            return None
        parsed = urllib.parse.urlparse(location)
        query = urllib.parse.parse_qs(parsed.query)
        urls = query.get("loanEarlyReturnUrl")
        if urls:
            return urls[0]

    def fill_out_form(self, **values):
        fields = []
        for k, v in list(values.items()):
            fields.append(dict(name=k, value=v))
        headers = {"Content-Type": "application/json; charset=utf-8"}
        return headers, json.dumps(dict(fields=fields))

    error_to_exception = {
        "TitleNotCheckedOut": NoActiveLoan,
    }

    def raise_exception_on_error(self, data, custom_error_to_exception={}):
        if not "errorCode" in data:
            return
        error = data["errorCode"]
        message = data.get("message") or ""
        for d in custom_error_to_exception, self.error_to_exception:
            if error in d:
                raise d[error](message)

    def get_loan(
        self, patron: Patron, pin: str | None, overdrive_id: str
    ) -> dict[str, Any]:
        """Get patron's loan information for the identified item.

        :param patron: A patron.
        :param pin: An optional PIN/password for the patron.
        :param overdrive_id: The OverDrive identifier for an item.
        :return: Information about the loan.
        """
        url = f"{self.CHECKOUTS_ENDPOINT}/{overdrive_id.upper()}"
        data = self.patron_request(patron, pin, url, is_fulfillment=True).json()
        self.raise_exception_on_error(data)
        return data

    def get_hold(self, patron, pin, overdrive_id):
        url = self.endpoint(self.HOLD_ENDPOINT, product_id=overdrive_id.upper())
        data = self.patron_request(patron, pin, url).json()
        self.raise_exception_on_error(data)
        return data

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> FulfillmentInfo:
        """Get the actual resource file to the patron."""
        internal_format = self.internal_format(delivery_mechanism)
        if licensepool.identifier.identifier is None:
            self.log.error(
                f"Cannot fulfill licensepool with no identifier. Licensepool.id: {licensepool.id}"
            )
            raise CannotFulfill()
        try:
            result = self.get_fulfillment_link(
                patron, pin, licensepool.identifier.identifier, internal_format
            )
            if isinstance(result, FulfillmentInfo):
                # The fulfillment process was short-circuited, probably
                # by the creation of an OverdriveManifestFulfillmentInfo.
                return result

            url, media_type = result
            if internal_format in self.STREAMING_FORMATS:
                media_type += DeliveryMechanism.STREAMING_PROFILE
        except FormatNotAvailable as e:
            # It's possible the available formats for this book have changed and we
            # have an inaccurate delivery mechanism. Try to update the formats, but
            # reraise the error regardless.
            self.log.info(
                "Overdrive id %s was not available as %s, getting updated formats"
                % (licensepool.identifier.identifier, internal_format)
            )

            try:
                self.update_formats(licensepool)
            except Exception as e2:
                self.log.error(
                    "Could not update formats for Overdrive ID %s"
                    % licensepool.identifier.identifier
                )

            raise e

        # In case we are a non-drm asset, we should just redirect the client to the asset directly
        fulfillment_force_redirect = internal_format in [
            "ebook-epub-open",
            "ebook-pdf-open",
        ]

        return FulfillmentInfo(
            licensepool.collection,
            licensepool.data_source.name,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
            content_link=url,
            content_type=media_type,
            content=None,
            content_expires=None,
            content_link_redirect=fulfillment_force_redirect,
        )

    def get_fulfillment_link(
        self, patron: Patron, pin: str | None, overdrive_id: str, format_type: str
    ) -> OverdriveManifestFulfillmentInfo | tuple[str, str]:
        """Get the link to the ACSM or manifest for an existing loan."""
        try:
            loan = self.get_loan(patron, pin, overdrive_id)
        except PatronAuthorizationFailedException as e:
            message = f"Error authenticating patron for fulfillment: {e.args[0]}"
            raise CannotFulfill(message, *e.args[1:]) from e

        if not loan:
            raise NoActiveLoan("Could not find active loan for %s" % overdrive_id)
        download_link = None
        if not loan.get("isFormatLockedIn") and format_type in self.LOCK_IN_FORMATS:
            # The format is not locked in. Lock it in.
            # This will happen the first time someone tries to fulfill
            # a loan with a lock-in format (basically Adobe-gated formats)
            response = self.lock_in_format(patron, pin, overdrive_id, format_type)
            if response.status_code not in (201, 200):
                if response.status_code == 400:
                    message = response.json().get("message")
                    if (
                        message
                        == "The selected format may not be available for this title."
                    ):
                        raise FormatNotAvailable(
                            "This book is not available in the format you requested."
                        )
                else:
                    raise CannotFulfill("Could not lock in format %s" % format_type)
            response = response.json()
            try:
                download_link = self.extract_download_link(
                    response, self.DEFAULT_ERROR_URL
                )
            except OSError as e:
                # Get the loan fresh and see if that solves the problem.
                loan = self.get_loan(patron, pin, overdrive_id)

        # TODO: Verify that the asked-for format type is the same as the
        # one in the loan.

        if format_type and not download_link:
            download_link = self.get_download_link(
                loan, format_type, self.DEFAULT_ERROR_URL
            )
            if not download_link:
                raise CannotFulfill(
                    "No download link for {}, format {}".format(
                        overdrive_id, format_type
                    )
                )

        if download_link:
            if format_type in self.MANIFEST_INTERNAL_FORMATS:
                # The client must authenticate using its own
                # credentials to fulfill this URL; we can't do it.
                scope_string = self.scope_string(patron.library)
                fulfillment_access_token = self.get_patron_credential(
                    patron,
                    pin,
                    is_fulfillment=True,
                ).credential
                return OverdriveManifestFulfillmentInfo(
                    self.collection,
                    download_link,
                    overdrive_id,
                    scope_string,
                    fulfillment_access_token,
                )

            return self.get_fulfillment_link_from_download_link(
                patron, pin, download_link
            )

        raise CannotFulfill(
            f"Cannot obtain a download link for patron {patron!r}, overdrive_id {overdrive_id}, format_type {format_type}"
        )

    def get_fulfillment_link_from_download_link(
        self, patron, pin, download_link, fulfill_url=None
    ) -> tuple[str, str]:
        # If this for Overdrive's streaming reader, and the link expires,
        # the patron can go back to the circulation manager fulfill url
        # again to get a new one.
        if not fulfill_url and flask.request:
            fulfill_url = flask.request.url
        else:
            fulfill_url = ""
        download_link = download_link.replace("{odreadauthurl}", fulfill_url)
        download_response = self.patron_request(patron, pin, download_link)
        return self.extract_content_link(download_response.json())

    def extract_content_link(self, content_link_gateway_json):
        link = content_link_gateway_json["links"]["contentlink"]
        return link["href"], link["type"]

    def lock_in_format(self, patron, pin, overdrive_id, format_type):
        overdrive_id = overdrive_id.upper()
        headers, document = self.fill_out_form(
            reserveId=overdrive_id, formatType=format_type
        )
        url = self.endpoint(self.FORMATS_ENDPOINT, overdrive_id=overdrive_id)
        return self.patron_request(patron, pin, url, headers, document)

    @classmethod
    def extract_data_from_checkout_response(
        cls, checkout_response_json, format_type, error_url
    ):
        expires = cls.extract_expiration_date(checkout_response_json)
        return expires, cls.get_download_link(
            checkout_response_json, format_type, error_url
        )

    @classmethod
    def extract_data_from_hold_response(cls, hold_response_json):
        position = hold_response_json["holdListPosition"]
        placed = cls._extract_date(hold_response_json, "holdPlacedDate")
        return position, placed

    @classmethod
    def extract_expiration_date(cls, data):
        return cls._extract_date(data, "expires")

    @classmethod
    def _extract_date(cls, data, field_name):
        if not isinstance(data, dict):
            return None
        if not field_name in data:
            return None
        try:
            return strptime_utc(data[field_name], cls.TIME_FORMAT)
        except ValueError as e:
            # Wrong format
            return None

    def get_patron_information(self, patron, pin):
        data = self.patron_request(patron, pin, self.ME_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_checkouts(self, patron: Patron, pin: str | None) -> dict[str, Any]:
        """Get information for the given patron's loans.

        :param patron: A patron.
        :param pin: An optional PIN/password for the patron.
        :return: Information about the patron's loans.
        """
        data = self.patron_request(
            patron, pin, self.CHECKOUTS_ENDPOINT, is_fulfillment=True
        ).json()
        self.raise_exception_on_error(data)
        return data

    def get_patron_holds(self, patron, pin):
        data = self.patron_request(patron, pin, self.HOLDS_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data

    @classmethod
    def _pd(cls, d):
        """Stupid method to parse a date.

        TIME_FORMAT mentions "Z" for Zulu time, which is the same as
        UTC.
        """
        if not d:
            return d
        return strptime_utc(d, cls.TIME_FORMAT)

    def patron_activity(self, patron, pin):
        try:
            loans = self.get_patron_checkouts(patron, pin)
            holds = self.get_patron_holds(patron, pin)
        except PatronAuthorizationFailedException as e:
            # This frequently happens because Overdrive performs
            # checks for blocked or expired accounts upon initial
            # authorization, where the circulation manager would let
            # the 'authorization' part succeed and block the patron's
            # access afterwards.
            #
            # It's common enough that it's hardly worth mentioning, but it
            # could theoretically be the sign of a larger problem.
            self.log.info(
                "Overdrive authentication failed, assuming no loans.", exc_info=e
            )
            loans = {}
            holds = {}

        for checkout in loans.get("checkouts", []):
            loan_info = self.process_checkout_data(checkout, self.collection)
            yield loan_info

        for hold in holds.get("holds", []):
            overdrive_identifier = hold["reserveId"].lower()
            start = self._pd(hold.get("holdPlacedDate"))
            end = self._pd(hold.get("holdExpires"))
            position = hold.get("holdListPosition")
            if position is not None:
                position = int(position)
            if "checkout" in hold.get("actions", {}):
                # This patron needs to decide whether to check the
                # book out. By our reckoning, the patron's position is
                # 0, not whatever position Overdrive had for them.
                position = 0
            yield HoldInfo(
                self.collection,
                DataSource.OVERDRIVE,
                Identifier.OVERDRIVE_ID,
                overdrive_identifier,
                start_date=start,
                end_date=end,
                hold_position=position,
            )

    @classmethod
    def process_checkout_data(cls, checkout: dict[str, Any], collection: Collection):
        """Convert one checkout from Overdrive's list of checkouts
        into a LoanInfo object.

        :return: A LoanInfo object if the book can be fulfilled
            by the default Library Simplified client, and None otherwise.
        """
        overdrive_identifier = checkout["reserveId"].lower()
        start = cls._pd(checkout.get("checkoutDate"))
        end = cls._pd(checkout.get("expires"))

        usable_formats = []

        # If a format is already locked in, it will be in formats.
        for format in checkout.get("formats", []):
            format_type = format.get("formatType")
            if format_type in cls.FORMATS:
                usable_formats.append(format_type)

        # If a format hasn't been selected yet, available formats are in actions.
        actions = checkout.get("actions", {})
        format_action = actions.get("format", {})
        format_fields = format_action.get("fields", [])
        for field in format_fields:
            if field.get("name", "") == "formatType":
                format_options = field.get("options", [])
                for format_type in format_options:
                    if format_type in cls.FORMATS:
                        usable_formats.append(format_type)

        if not usable_formats:
            # Either this book is not available in any format readable
            # by the default client, or the patron previously chose to
            # fulfill it in a format not readable by the default
            # client. Either way, we cannot fulfill this loan and we
            # shouldn't show it in the list.
            return None

        locked_to = None
        if len(usable_formats) == 1:
            # Either the book has been locked into a specific format,
            # or only one usable format is available. We don't know
            # which case we're looking at, but for our purposes the
            # book is locked -- unless, of course, what Overdrive
            # considers "one format" corresponds to more than one
            # format on our side.
            [overdrive_format] = usable_formats

            internal_formats = list(
                OverdriveRepresentationExtractor.internal_formats(overdrive_format)
            )

            if len(internal_formats) == 1:
                [(media_type, drm_scheme)] = internal_formats
                # Make it clear that Overdrive will only deliver the content
                # in one specific media type.
                locked_to = DeliveryMechanismInfo(
                    content_type=media_type, drm_scheme=drm_scheme
                )

        return LoanInfo(
            collection,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            overdrive_identifier,
            start_date=start,
            end_date=end,
            locked_to=locked_to,
        )

    def default_notification_email_address(self, patron, pin):
        """Find the email address this patron wants to use for hold
        notifications.

        :return: The email address Overdrive has on record for
           this patron's hold notifications, or None if there is
           no such address.
        """

        # We're calling the superclass implementation, but we have no
        # intention of actually using the result. This is a
        # per-library default that trashes all of its input, and
        # Overdrive has a better solution.
        trash_everything_address = super().default_notification_email_address(
            patron, pin
        )

        # Instead, we will ask _Overdrive_ if this patron has a
        # preferred email address for notifications.
        address = None
        response = self.patron_request(patron, pin, self.PATRON_INFORMATION_ENDPOINT)
        if response.status_code == 200:
            data = response.json()
            address = data.get("lastHoldEmail")

            # Great! Except, it's possible that this address is the
            # 'trash everything' address, because we _used_ to send
            # that address to Overdrive. If so, ignore it.
            if address == trash_everything_address:
                address = None
        else:
            self.log.error(
                "Unable to get patron information for %s: %s",
                patron.authorization_identifier,
                response.content,
            )
        return address

    def place_hold(self, patron, pin, licensepool, notification_email_address):
        """Place a book on hold.

        :return: A HoldData object, if a hold was successfully placed,
            or the book was already on hold.
        :raise: A CirculationException explaining why no hold
            could be placed.
        """
        if not notification_email_address:
            notification_email_address = self.default_notification_email_address(
                patron, pin
            )
        overdrive_id = licensepool.identifier.identifier
        form_fields = dict(reserveId=overdrive_id)
        if notification_email_address:
            form_fields["emailAddress"] = notification_email_address
        else:
            form_fields["ignoreHoldEmail"] = True

        headers, document = self.fill_out_form(**form_fields)
        response = self.patron_request(
            patron, pin, self.HOLDS_ENDPOINT, headers, document
        )
        return self.process_place_hold_response(response, patron, pin, licensepool)

    def process_place_hold_response(self, response, patron, pin, licensepool):
        """Process the response to a HOLDS_ENDPOINT request.

        :return: A HoldData object, if a hold was successfully placed,
            or the book was already on hold.
        :raise: A CirculationException explaining why no hold
            could be placed.
        """

        def make_holdinfo(hold_response):
            # Create a HoldInfo object by combining data passed into
            # the enclosing method with the data from a hold response
            # (either creating a new hold or fetching an existing
            # one).
            position, start_date = self.extract_data_from_hold_response(hold_response)
            return HoldInfo(
                licensepool.collection,
                licensepool.data_source.name,
                licensepool.identifier.type,
                licensepool.identifier.identifier,
                start_date=start_date,
                end_date=None,
                hold_position=position,
            )

        family = response.status_code // 100

        if family == 4:
            error = response.json()
            if not error or not "errorCode" in error:
                raise CannotHold()
            code = error["errorCode"]
            if code == "AlreadyOnWaitList":
                # The book is already on hold, so this isn't an exceptional
                # condition. Refresh the queue info and act as though the
                # request was successful.
                hold = self.get_hold(patron, pin, licensepool.identifier.identifier)
                return make_holdinfo(hold)
            elif code == "NotWithinRenewalWindow":
                # The patron has this book checked out and cannot yet
                # renew their loan.
                raise CannotRenew()
            elif code == "PatronExceededHoldLimit":
                raise PatronHoldLimitReached()
            else:
                raise CannotHold(code)
        elif family == 2:
            # The book was successfuly placed on hold. Return an
            # appropriate HoldInfo.
            data = response.json()
            return make_holdinfo(data)
        else:
            # Some other problem happened -- we don't know what.  It's
            # not a 5xx error because the HTTP client would have been
            # turned that into a RemoteIntegrationException.
            raise CannotHold()

    def release_hold(self, patron, pin, licensepool):
        """Release a patron's hold on a book.

        :raises CannotReleaseHold: If there is an error communicating
            with Overdrive, or Overdrive refuses to release the hold for
            any reason.
        """
        url = self.endpoint(
            self.HOLD_ENDPOINT, product_id=licensepool.identifier.identifier
        )
        response = self.patron_request(patron, pin, url, method="DELETE")
        if response.status_code // 100 == 2 or response.status_code == 404:
            return True
        if not response.content:
            raise CannotReleaseHold()
        data = response.json()
        if not "errorCode" in data:
            raise CannotReleaseHold()
        if data["errorCode"] == "PatronDoesntHaveTitleOnHold":
            # There was never a hold to begin with, so we're fine.
            return True
        raise CannotReleaseHold(response.content)

    def circulation_lookup(self, book):
        if isinstance(book, str):
            book_id = book
            circulation_link = self.endpoint(
                self.AVAILABILITY_ENDPOINT,
                collection_token=self.collection_token,
                product_id=book_id,
            )
            book = dict(id=book_id)
        else:
            circulation_link = book["availability_link"]
            # Make sure we use v2 of the availability API,
            # even if Overdrive gave us a link to v1.
            circulation_link = self.make_link_safe(circulation_link)
        return book, self.get(circulation_link, {})

    def update_formats(self, licensepool):
        """Update the format information for a single book.

        Incidentally updates the metadata, just in case Overdrive has
        changed it.
        """
        info = self.metadata_lookup(licensepool.identifier)

        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(
            info, include_bibliographic=True, include_formats=True
        )
        if not metadata:
            # No work to be done.
            return

        edition, ignore = self._edition(licensepool)

        replace = ReplacementPolicy.from_license_source(self._db)
        metadata.apply(edition, self.collection, replace=replace)

    def update_licensepool(self, book_id):
        """Update availability information for a single book.

        If the book has never been seen before, a new LicensePool
        will be created for the book.

        The book's LicensePool will be updated with current
        circulation information. Bibliographic coverage will be
        ensured for the Overdrive Identifier, and a Work will be
        created for the LicensePool and set as presentation-ready.
        """
        # Retrieve current circulation information about this book
        try:
            book, (status_code, headers, content) = self.circulation_lookup(book_id)
        except Exception as e:
            status_code = None
            self.log.error("HTTP exception communicating with Overdrive", exc_info=e)

        # TODO: If you ask for a book that you know about, and
        # Overdrive says the book doesn't exist in the collection,
        # then it's appropriate to update an existing
        # LicensePool. However we shouldn't be creating a *brand new*
        # LicensePool for a book Overdrive says isn't in the
        # collection.
        if status_code not in (200, 404):
            self.log.error(
                "Could not get availability for %s: status code %s",
                book_id,
                status_code,
            )
            return None, None, False
        if isinstance(content, (bytes, str)):
            content = json.loads(content)
        book.update(content)

        # Update book_id now that we know we have new data.
        book_id = book["id"]
        license_pool, is_new = LicensePool.for_foreign_id(
            self._db,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            book_id,
            collection=self.collection,
        )
        if is_new or not license_pool.work:
            # Either this is the first time we've seen this book or its doesn't
            # have an associated work. Make sure its identifier has bibliographic coverage.
            self.overdrive_bibliographic_coverage_provider.ensure_coverage(
                license_pool.identifier, force=True
            )

        return self.update_licensepool_with_book_info(book, license_pool, is_new)

    # Alias for the CirculationAPI interface
    def update_availability(self, licensepool):
        return self.update_licensepool(licensepool.identifier.identifier)

    def _edition(self, licensepool):
        """Find or create the Edition that would be used to contain
        Overdrive metadata for the given LicensePool.
        """
        return Edition.for_foreign_id(
            self._db,
            self.source,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
        )

    def update_licensepool_with_book_info(self, book, license_pool, is_new_pool):
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Then, create an Edition and make sure it has bibliographic
        coverage. If the new Edition is the only candidate for the
        pool's presentation_edition, promote it to presentation
        status.
        """
        extractor = OverdriveRepresentationExtractor(self)
        circulation = extractor.book_info_to_circulation(book)
        license_pool, circulation_changed = circulation.apply(
            self._db, license_pool.collection
        )

        edition, is_new_edition = self._edition(license_pool)

        if is_new_pool:
            license_pool.open_access = False
            self.log.info("New Overdrive book discovered: %r", edition)
        return license_pool, is_new_pool, circulation_changed

    @classmethod
    def get_download_link(self, checkout_response, format_type, error_url):
        """Extract a download link from the given response.

        :param checkout_response: A JSON document describing a checkout-type
           response from the Overdrive API.
        :param format_type: The internal (Overdrive-facing) format type
           that should be retrieved. 'x-manifest' format types are treated
           as a variant of the 'x' format type -- Overdrive doesn't recognise
           'x-manifest' and uses 'x' for delivery of both streaming content
           and manifests.
        :param error_url: Value to interpolate for the {errorpageurl}
           URI template value. This is ignored if you're fetching a manifest;
           instead, the 'errorpageurl' variable is removed entirely.
        """
        link = None
        format = None
        available_formats = []
        if format_type in self.MANIFEST_INTERNAL_FORMATS:
            use_format_type = format_type.replace("-manifest", "")
            fetch_manifest = True
        else:
            use_format_type = format_type
            fetch_manifest = False
        for f in checkout_response.get("formats", []):
            this_type = f["formatType"]
            available_formats.append(this_type)
            if this_type == use_format_type:
                format = f
                break
        if not format:
            if any(
                x in set(available_formats) for x in self.INCOMPATIBLE_PLATFORM_FORMATS
            ):
                # The most likely explanation is that the patron
                # already had this book delivered to their Kindle.
                raise FulfilledOnIncompatiblePlatform(
                    "It looks like this loan was already fulfilled on another platform, most likely Amazon Kindle. We're not allowed to also send it to you as an EPUB."
                )
            else:
                # We don't know what happened -- most likely our
                # format data is bad.
                format_list = ", ".join(available_formats)
                msg = "Could not find specified format %s. Available formats: %s"
                raise NoAcceptableFormat(
                    msg % (use_format_type, ", ".join(available_formats))
                )

        return self.extract_download_link(format, error_url, fetch_manifest)

    @classmethod
    def extract_download_link(cls, format, error_url, fetch_manifest=False):
        """Extract a download link from the given format descriptor.

        :param format: A JSON document describing a specific format
           in which Overdrive makes a book available.
        :param error_url: Value to interpolate for the {errorpageurl}
           URI template value. This is ignored if you're fetching a manifest;
           instead, the 'errorpageurl' variable is removed entirely.
        :param fetch_manifest: If this is true, the download link will be
           modified to a URL that an authorized mobile client can use to fetch
           a manifest file.
        """

        format_type = format.get("formatType", "(unknown)")
        if not "linkTemplates" in format:
            raise OSError("No linkTemplates for format %s" % format_type)
        templates = format["linkTemplates"]
        if not "downloadLink" in templates:
            raise OSError("No downloadLink for format %s" % format_type)
        download_link_data = templates["downloadLink"]
        if not "href" in download_link_data:
            raise OSError("No downloadLink href for format %s" % format_type)
        download_link = download_link_data["href"]
        if download_link:
            if fetch_manifest:
                download_link = cls.make_direct_download_link(download_link)
            else:
                download_link = download_link.replace("{errorpageurl}", error_url)
            return download_link
        else:
            return None

    @classmethod
    def make_direct_download_link(cls, link):
        """Convert an Overdrive Read or Overdrive Listen link template to a
        direct-download link for the manifest.

        This means removing any templated arguments for Overdrive Read
        authentication URL and error URL; and adding a value for the
        'contentfile' argument.

        :param link: An Overdrive Read or Overdrive Listen template
            link.
        """
        # Remove any Overdrive Read authentication URL and error URL.
        for argument_name in ("odreadauthurl", "errorpageurl"):
            argument_re = re.compile(f"{argument_name}={{{argument_name}}}&?")
            link = argument_re.sub("", link)

        # Add the contentfile=true argument.
        if "?" not in link:
            link += "?contentfile=true"
        elif link.endswith("&") or link.endswith("?"):
            link += "contentfile=true"
        else:
            link += "&contentfile=true"
        return link


class OverdriveCirculationMonitor(CollectionMonitor, TimelineMonitor):
    """Maintain LicensePools for recently changed Overdrive titles. Create
    basic Editions for any new LicensePools that show up.
    """

    MAXIMUM_BOOK_RETRIES = 3
    SERVICE_NAME = "Overdrive Circulation Monitor"
    PROTOCOL = ExternalIntegration.OVERDRIVE
    OVERLAP = datetime.timedelta(minutes=1)

    @inject
    def __init__(
        self,
        _db,
        collection,
        api_class=OverdriveAPI,
        analytics: Analytics = Provide[Services.analytics.analytics],
    ):
        """Constructor."""
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)
        self.analytics = analytics

    def recently_changed_ids(self, start, cutoff):
        return self.api.recently_changed_ids(start, cutoff)

    def catch_up_from(self, start, cutoff, progress: TimestampData):
        """Find Overdrive books that changed recently.

        :progress: A TimestampData representing the time previously
            covered by this Monitor.
        """
        overdrive_data_source = DataSource.lookup(self._db, DataSource.OVERDRIVE)

        # Ask for changes between the last time covered by the Monitor
        # and the current time.
        total_books = 0
        for book in self.recently_changed_ids(start, cutoff):
            total_books += 1
            if not total_books % 100:
                self.log.info("%s books processed", total_books)
            if not book:
                continue

            # Attempt to create/update the book up to MAXIMUM_BOOK_RETRIES times.
            book_changed = False
            book_succeeded = False
            for attempt in range(OverdriveCirculationMonitor.MAXIMUM_BOOK_RETRIES):
                if book_succeeded:
                    break

                try:
                    _, _, is_changed = self.api.update_licensepool(book)
                    self._db.commit()
                    book_succeeded = True
                    book_changed = is_changed
                except StaleDataError as e:
                    self.log.exception("encountered stale data exception: ", exc_info=e)
                    self._db.rollback()
                    if attempt + 1 == OverdriveCirculationMonitor.MAXIMUM_BOOK_RETRIES:
                        progress.exception = e
                    else:
                        time.sleep(1)
                        self.log.warning(
                            f"retrying book {book} (attempt {attempt} of {OverdriveCirculationMonitor.MAXIMUM_BOOK_RETRIES})"
                        )

            if self.should_stop(start, book, book_changed):
                break

        progress.achievements = "Books processed: %d." % total_books

    def should_stop(self, start, api_description, is_changed):
        pass


class NewTitlesOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor the Overdrive collection for newly added titles.

    This catches any new titles that slipped through the cracks of the
    RecentOverdriveCollectionMonitor.
    """

    SERVICE_NAME = "Overdrive New Title Monitor"
    OVERLAP = datetime.timedelta(days=7)
    DEFAULT_START_TIME = OverdriveCirculationMonitor.NEVER

    def recently_changed_ids(self, start, cutoff):
        """Ignore the dates and return all IDs."""
        return self.api.all_ids()

    def should_stop(self, start, api_description, is_changed):
        if not start or start is self.NEVER:
            # This monitor has never run before. It should ask about
            # every single book.
            return False

        # We should stop if this book was added before our start time.
        date_added = api_description.get("date_added")
        if not date_added:
            # We don't know when this book was added -- shouldn't happen.
            return False

        try:
            date_added = dateutil.parser.parse(date_added)
        except ValueError as e:
            # The date format is unparseable -- shouldn't happen.
            self.log.error("Got invalid date: %s", date_added)
            return False

        self.log.info(
            "Date added: %s, start time: %s, result %s",
            date_added,
            start,
            date_added < start,
        )
        return date_added < start


class OverdriveCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Overdrive collection.
    """

    SERVICE_NAME = "Overdrive Collection Reaper"
    PROTOCOL = ExternalIntegration.OVERDRIVE

    def __init__(self, _db, collection, api_class=OverdriveAPI):
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier):
        self.api.update_licensepool(identifier.identifier)


class RecentOverdriveCollectionMonitor(OverdriveCirculationMonitor):
    """Monitor recently changed books in the Overdrive collection."""

    SERVICE_NAME = "Reverse Chronological Overdrive Collection Monitor"

    # Report successful completion upon finding this number of
    # consecutive books in the Overdrive results whose LicensePools
    # haven't changed since last time. Overdrive results are not in
    # strict chronological order, but if you see 100 consecutive books
    # that haven't changed, you're probably done.
    MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consecutive_unchanged_books = 0

    def should_stop(self, start, api_description, is_changed):
        if is_changed:
            self.consecutive_unchanged_books = 0
        else:
            self.consecutive_unchanged_books += 1
            if (
                self.consecutive_unchanged_books
                >= self.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS
            ):
                # We're supposed to stop this run after finding a
                # run of books that have not changed, and we have
                # in fact seen that many consecutive unchanged
                # books.
                self.log.info(
                    "Stopping at %d unchanged books.", self.consecutive_unchanged_books
                )
                return True
        return False


class OverdriveFormatSweep(IdentifierSweepMonitor):
    """Check the current formats of every Overdrive book
    in our collection.
    """

    SERVICE_NAME = "Overdrive Format Sweep"
    DEFAULT_BATCH_SIZE = 25
    PROTOCOL = ExternalIntegration.OVERDRIVE

    def __init__(self, _db, collection, api_class=OverdriveAPI):
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier):
        pools = identifier.licensed_through
        for pool in pools:
            self.api.update_formats(pool)
            # if there are multiple pools they should all have the same formats
            # so we break after processing the first one
            break


class OverdriveData:
    overdrive_client_key: str
    overdrive_client_secret: str
    overdrive_website_id: str
    overdrive_server_nickname: str = OverdriveConstants.PRODUCTION_SERVERS
    max_retry_count: int = 0


class OverdriveRepresentationExtractor(LoggerMixin):
    """Extract useful information from Overdrive's JSON representations."""

    def __init__(self, api):
        """Constructor.

        :param api: An OverdriveAPI object. This will be used when deciding
        which portions of a JSON representation are relevant to the active
        Overdrive collection.
        """
        self.library_id = api.advantage_library_id

    @classmethod
    def availability_link_list(cls, book_list):
        """:return: A list of dictionaries with keys `id`, `title`, `availability_link`."""
        l = []
        if not "products" in book_list:
            return []

        products = book_list["products"]
        for product in products:
            if not "id" in product:
                cls.logger().warning("No ID found in %r", product)
                continue
            book_id = product["id"]
            data = dict(
                id=book_id,
                title=product.get("title"),
                author_name=None,
                date_added=product.get("dateAdded"),
            )
            if "primaryCreator" in product:
                creator = product["primaryCreator"]
                if creator.get("role") == "Author":
                    data["author_name"] = creator.get("name")
            links = product.get("links", [])
            if "availability" in links:
                link = links["availability"]["href"]
                data["availability_link"] = OverdriveAPI.make_link_safe(link)
            else:
                logging.getLogger("Overdrive API").warning(
                    "No availability link for %s", book_id
                )
            l.append(data)
        return l

    @classmethod
    def link(self, page, rel):
        if "links" in page and rel in page["links"]:
            raw_link = page["links"][rel]["href"]
            link = OverdriveAPI.make_link_safe(raw_link)
        else:
            link = None
        return link

    format_data_for_overdrive_format = {
        "ebook-pdf-adobe": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.ADOBE_DRM),
        "ebook-pdf-open": (Representation.PDF_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
        "ebook-epub-adobe": (
            Representation.EPUB_MEDIA_TYPE,
            DeliveryMechanism.ADOBE_DRM,
        ),
        "ebook-epub-open": (Representation.EPUB_MEDIA_TYPE, DeliveryMechanism.NO_DRM),
        "audiobook-mp3": ("application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM),
        "music-mp3": ("application/x-od-media", DeliveryMechanism.OVERDRIVE_DRM),
        "ebook-overdrive": [
            (
                MediaTypes.OVERDRIVE_EBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_TEXT_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
            ),
        ],
        "audiobook-overdrive": [
            (
                MediaTypes.OVERDRIVE_AUDIOBOOK_MANIFEST_MEDIA_TYPE,
                DeliveryMechanism.LIBBY_DRM,
            ),
            (
                DeliveryMechanism.STREAMING_AUDIO_CONTENT_TYPE,
                DeliveryMechanism.STREAMING_DRM,
            ),
        ],
        "video-streaming": (
            DeliveryMechanism.STREAMING_VIDEO_CONTENT_TYPE,
            DeliveryMechanism.STREAMING_DRM,
        ),
        "ebook-kindle": (
            DeliveryMechanism.KINDLE_CONTENT_TYPE,
            DeliveryMechanism.KINDLE_DRM,
        ),
        "periodicals-nook": (
            DeliveryMechanism.NOOK_CONTENT_TYPE,
            DeliveryMechanism.NOOK_DRM,
        ),
    }

    # A mapping of the overdrive format name to end sample content type
    # Overdrive samples are not DRM protected so the links should be
    # stored as the end sample content type
    sample_format_to_content_type = {
        "ebook-overdrive": "text/html",
        "audiobook-wma": "audio/x-ms-wma",
        "audiobook-mp3": "audio/mpeg",
        "audiobook-overdrive": "text/html",
        "ebook-epub-adobe": "application/epub+zip",
        "magazine-overdrive": "text/html",
    }

    @classmethod
    def internal_formats(cls, overdrive_format):
        """Yield all internal formats for the given Overdrive format.

        Some Overdrive formats become multiple internal formats.

        :yield: A sequence of (content type, DRM system) 2-tuples
        """
        result = cls.format_data_for_overdrive_format.get(overdrive_format)
        if not result:
            return
        if isinstance(result, list):
            yield from result
        else:
            yield result

    ignorable_overdrive_formats: set[str] = set()

    overdrive_role_to_simplified_role = {
        "actor": Contributor.ACTOR_ROLE,
        "artist": Contributor.ARTIST_ROLE,
        "book producer": Contributor.PRODUCER_ROLE,
        "associated name": Contributor.ASSOCIATED_ROLE,
        "author": Contributor.AUTHOR_ROLE,
        "author of introduction": Contributor.INTRODUCTION_ROLE,
        "author of foreword": Contributor.FOREWORD_ROLE,
        "author of afterword": Contributor.AFTERWORD_ROLE,
        "contributor": Contributor.CONTRIBUTOR_ROLE,
        "colophon": Contributor.COLOPHON_ROLE,
        "adapter": Contributor.ADAPTER_ROLE,
        "etc.": Contributor.UNKNOWN_ROLE,
        "cast member": Contributor.ACTOR_ROLE,
        "collaborator": Contributor.COLLABORATOR_ROLE,
        "compiler": Contributor.COMPILER_ROLE,
        "composer": Contributor.COMPOSER_ROLE,
        "copyright holder": Contributor.COPYRIGHT_HOLDER_ROLE,
        "director": Contributor.DIRECTOR_ROLE,
        "editor": Contributor.EDITOR_ROLE,
        "engineer": Contributor.ENGINEER_ROLE,
        "executive producer": Contributor.EXECUTIVE_PRODUCER_ROLE,
        "illustrator": Contributor.ILLUSTRATOR_ROLE,
        "musician": Contributor.MUSICIAN_ROLE,
        "narrator": Contributor.NARRATOR_ROLE,
        "other": Contributor.UNKNOWN_ROLE,
        "performer": Contributor.PERFORMER_ROLE,
        "producer": Contributor.PRODUCER_ROLE,
        "translator": Contributor.TRANSLATOR_ROLE,
        "photographer": Contributor.PHOTOGRAPHER_ROLE,
        "lyricist": Contributor.LYRICIST_ROLE,
        "transcriber": Contributor.TRANSCRIBER_ROLE,
        "designer": Contributor.DESIGNER_ROLE,
    }

    overdrive_medium_to_simplified_medium = {
        "eBook": Edition.BOOK_MEDIUM,
        "Video": Edition.VIDEO_MEDIUM,
        "Audiobook": Edition.AUDIO_MEDIUM,
        "Music": Edition.MUSIC_MEDIUM,
        "Periodicals": Edition.PERIODICAL_MEDIUM,
    }

    DATE_FORMAT = "%Y-%m-%d"

    @classmethod
    def parse_roles(cls, id, rolestring):
        rolestring = rolestring.lower()
        roles = [x.strip() for x in rolestring.split(",")]
        if " and " in roles[-1]:
            roles = roles[:-1] + [x.strip() for x in roles[-1].split(" and ")]
        processed = []
        for x in roles:
            if x not in cls.overdrive_role_to_simplified_role:
                cls.logger().error("Could not process role %s for %s", x, id)
            else:
                processed.append(cls.overdrive_role_to_simplified_role[x])
        return processed

    def book_info_to_circulation(self, book):
        """Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_metadata() method.
        """
        # In Overdrive, 'reserved' books show up as books on
        # hold. There is no separate notion of reserved books.
        licenses_reserved = 0

        licenses_owned = None
        licenses_available = None
        patrons_in_hold_queue = None

        # TODO: The only reason this works for a NotFound error is the
        # circulation code sticks the known book ID into `book` ahead
        # of time. That's a code smell indicating that this system
        # needs to be refactored.
        if "reserveId" in book and not "id" in book:
            book["id"] = book["reserveId"]
        if not "id" in book:
            return None
        overdrive_id = book["id"]
        primary_identifier = IdentifierData(Identifier.OVERDRIVE_ID, overdrive_id)
        # TODO: We might be able to use this information to avoid the
        # need for explicit configuration of Advantage collections, or
        # at least to keep Advantage collections more up-to-date than
        # they would be otherwise, as a side effect of updating
        # regular Overdrive collections.

        # TODO: this would be the place to handle simultaneous use
        # titles -- these can be detected with
        # availabilityType="AlwaysAvailable" and have their
        # .licenses_owned set to LicensePool.UNLIMITED_ACCESS.
        # see http://developer.overdrive.com/apis/library-availability-new

        # TODO: Cost-per-circ titles
        # (availabilityType="LimitedAvailablility") can be handled
        # similarly, though those can abruptly become unavailable, so
        # UNLIMITED_ACCESS is probably not appropriate.

        error_code = book.get("errorCode")
        # TODO: It's not clear what other error codes there might be.
        # The current behavior will respond to errors other than
        # NotFound by leaving the book alone, but this might not be
        # the right behavior.
        if error_code == "NotFound":
            licenses_owned = 0
            licenses_available = 0
            patrons_in_hold_queue = 0
        elif book.get("isOwnedByCollections") is not False:
            # We own this book.
            licenses_owned = 0
            licenses_available = 0

            for account in self._get_applicable_accounts(book.get("accounts", [])):
                licenses_owned += int(account.get("copiesOwned", 0))
                licenses_available += int(account.get("copiesAvailable", 0))

            if "numberOfHolds" in book:
                if patrons_in_hold_queue is None:
                    patrons_in_hold_queue = 0
                patrons_in_hold_queue += book["numberOfHolds"]

        return CirculationData(
            data_source=DataSource.OVERDRIVE,
            primary_identifier=primary_identifier,
            licenses_owned=licenses_owned,
            licenses_available=licenses_available,
            licenses_reserved=licenses_reserved,
            patrons_in_hold_queue=patrons_in_hold_queue,
        )

    def _get_applicable_accounts(
        self, accounts: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Returns those accounts from the accounts array that apply the
        current overdrive collection context.

        If this is an overdrive parent collection, we want to return accounts
        associated with the main OverDrive "library" and any non-main account
        with sharing enabled.

        If this is a child OverDrive collection, then we return only the
        account associated with that child's OverDrive Advantage "library".
        Additionally, we want to exclude the account if it is "shared" since
        we will be counting it with the parent collection.
        """

        if self.library_id == OverdriveAPI.OVERDRIVE_MAIN_ACCOUNT_ID:
            # this is a parent collection
            filtered_result = filter(
                lambda account: account.get("id")
                == OverdriveAPI.OVERDRIVE_MAIN_ACCOUNT_ID
                or account.get("shared", False),
                accounts,
            )
        else:
            # this is child collection
            filtered_result = filter(
                lambda account: account.get("id") == self.library_id
                and not account.get("shared", False),
                accounts,
            )

        return list(filtered_result)

    @classmethod
    def image_link_to_linkdata(cls, link, rel):
        if not link or not "href" in link:
            return None
        href = link["href"]
        if "00000000-0000-0000-0000" in href:
            # This is a stand-in cover for preorders. It's better not
            # to have a cover at all -- we might be able to get one
            # later, or from another source.
            return None
        href = OverdriveAPI.make_link_safe(href)
        media_type = link.get("type", None)
        return LinkData(rel=rel, href=href, media_type=media_type)

    @classmethod
    def book_info_to_metadata(
        cls, book, include_bibliographic=True, include_formats=True
    ):
        """Turn Overdrive's JSON representation of a book into a Metadata
        object.

        Note:  The json data passed into this method is from a different file/stream
        from the json data that goes into the book_info_to_circulation() method.
        """
        if not "id" in book:
            return None
        overdrive_id = book["id"]
        primary_identifier = IdentifierData(Identifier.OVERDRIVE_ID, overdrive_id)

        # If we trust classification data, we'll give it this weight.
        # Otherwise we'll probably give it a fraction of this weight.
        trusted_weight = Classification.TRUSTED_DISTRIBUTOR_WEIGHT

        duration: int | None = None

        if include_bibliographic:
            title = book.get("title", None)
            sort_title = book.get("sortTitle")
            subtitle = book.get("subtitle", None)
            series = book.get("series", None)
            publisher = book.get("publisher", None)
            imprint = book.get("imprint", None)

            if "publishDate" in book:
                published = strptime_utc(book["publishDate"][:10], cls.DATE_FORMAT)
            else:
                published = None

            languages = [l["code"] for l in book.get("languages", [])]
            if "eng" in languages or not languages:
                language = "eng"
            else:
                language = sorted(languages)[0]

            contributors = []
            for creator in book.get("creators", []):
                sort_name = creator["fileAs"]
                display_name = creator["name"]
                role = creator["role"]
                roles = cls.parse_roles(overdrive_id, role) or [
                    Contributor.UNKNOWN_ROLE
                ]
                contributor = ContributorData(
                    sort_name=sort_name,
                    display_name=display_name,
                    roles=roles,
                    biography=creator.get("bioText", None),
                )
                contributors.append(contributor)

            subjects = []
            for sub in book.get("subjects", []):
                subject = SubjectData(
                    type=Subject.OVERDRIVE,
                    identifier=sub["value"],
                    weight=trusted_weight,
                )
                subjects.append(subject)

            for sub in book.get("keywords", []):
                subject = SubjectData(
                    type=Subject.TAG,
                    identifier=sub["value"],
                    # We don't use TRUSTED_DISTRIBUTOR_WEIGHT because
                    # we don't know where the tags come from --
                    # probably Overdrive users -- and they're
                    # frequently wrong.
                    weight=1,
                )
                subjects.append(subject)

            extra = dict()
            if "grade_levels" in book:
                # n.b. Grade levels are measurements of reading level, not
                # age appropriateness. We can use them as a measure of age
                # appropriateness in a pinch, but we weight them less
                # heavily than TRUSTED_DISTRIBUTOR_WEIGHT.
                for i in book["grade_levels"]:
                    subject = SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=i["value"],
                        weight=trusted_weight / 10,
                    )
                    subjects.append(subject)

            overdrive_medium = book.get("mediaType", None)
            if (
                overdrive_medium
                and overdrive_medium not in cls.overdrive_medium_to_simplified_medium
            ):
                cls.logger().error(
                    "Could not process medium %s for %s", overdrive_medium, overdrive_id
                )

            medium = cls.overdrive_medium_to_simplified_medium.get(
                overdrive_medium, Edition.BOOK_MEDIUM
            )

            measurements = []
            if "awards" in book:
                extra["awards"] = book.get("awards", [])
                num_awards = len(extra["awards"])
                measurements.append(
                    MeasurementData(Measurement.AWARDS, str(num_awards))
                )

            for name, subject_type in (
                ("ATOS", Subject.ATOS_SCORE),
                ("lexileScore", Subject.LEXILE_SCORE),
                ("interestLevel", Subject.INTEREST_LEVEL),
            ):
                if not name in book:
                    continue
                identifier = str(book[name])
                subjects.append(
                    SubjectData(
                        type=subject_type, identifier=identifier, weight=trusted_weight
                    )
                )

            for grade_level_info in book.get("gradeLevels", []):
                grade_level = grade_level_info.get("value")
                subjects.append(
                    SubjectData(
                        type=Subject.GRADE_LEVEL,
                        identifier=grade_level,
                        weight=trusted_weight,
                    )
                )

            identifiers = []
            links = []
            sample_hrefs = set()
            for format in book.get("formats", []):
                duration_str: str | None = format.get("duration")
                if duration_str is not None:
                    # Using this method only the last valid duration attribute is captured
                    # If there are multiple formats with different durations, the edition will ignore the rest
                    try:
                        hrs, mins, secs = duration_str.split(":")
                        duration = (int(hrs) * 3600) + (int(mins) * 60) + int(secs)
                    except Exception as ex:
                        cls.logger().error(
                            f"Duration ({duration_str}) could not be parsed: {ex}"
                        )

                for new_id in format.get("identifiers", []):
                    t = new_id["type"]
                    v = new_id["value"]
                    orig_v = v
                    type_key = None
                    if t == "ASIN":
                        type_key = Identifier.ASIN
                    elif t == "ISBN":
                        type_key = Identifier.ISBN
                        if len(v) == 10:
                            v = isbnlib.to_isbn13(v)
                        if v is None or not isbnlib.is_isbn13(v):
                            # Overdrive sometimes uses invalid values
                            # like "n/a" as placeholders. Ignore such
                            # values to avoid a situation where hundreds of
                            # books appear to have the same ISBN. ISBNs
                            # which fail check digit checks or are invalid
                            # also can occur. Log them for review.
                            cls.logger().info("Bad ISBN value provided: %s", orig_v)
                            continue
                    elif t == "DOI":
                        type_key = Identifier.DOI
                    elif t == "UPC":
                        type_key = Identifier.UPC
                    elif t == "PublisherCatalogNumber":
                        continue
                    if type_key and v:
                        identifiers.append(IdentifierData(type_key, v, 1))

                # Samples become links.
                if "samples" in format:
                    for sample_info in format["samples"]:
                        href = sample_info["url"]
                        # Have we already parsed this sample? Overdrive repeats samples per format
                        if href in sample_hrefs:
                            continue

                        # Every sample has its own format type
                        overdrive_format_name = sample_info.get("formatType")
                        if not overdrive_format_name:
                            # Malformed sample
                            continue
                        content_type = cls.sample_format_to_content_type.get(
                            overdrive_format_name
                        )
                        if not content_type:
                            # Unusable by us.
                            cls.logger().warning(
                                f"Did not find a sample format mapping for '{overdrive_format_name}': {href}"
                            )
                            continue

                        if Representation.is_media_type(content_type):
                            links.append(
                                LinkData(
                                    rel=Hyperlink.SAMPLE,
                                    href=href,
                                    media_type=content_type,
                                )
                            )
                            sample_hrefs.add(href)

            # A cover and its thumbnail become a single LinkData.
            if "images" in book:
                images = book["images"]
                image_data = cls.image_link_to_linkdata(
                    images.get("cover"), Hyperlink.IMAGE
                )
                for name in ["cover300Wide", "cover150Wide", "thumbnail"]:
                    # Try to get a thumbnail that's as close as possible
                    # to the size we use.
                    image = images.get(name)
                    thumbnail_data = cls.image_link_to_linkdata(
                        image, Hyperlink.THUMBNAIL_IMAGE
                    )
                    if not image_data:
                        image_data = cls.image_link_to_linkdata(image, Hyperlink.IMAGE)
                    if thumbnail_data:
                        break

                if image_data:
                    if thumbnail_data:
                        image_data.thumbnail = thumbnail_data
                    links.append(image_data)

            # Descriptions become links.
            short = book.get("shortDescription")
            full = book.get("fullDescription")
            if full:
                links.append(
                    LinkData(
                        rel=Hyperlink.DESCRIPTION,
                        content=full,
                        media_type="text/html",
                    )
                )

            if short and (not full or not full.startswith(short)):
                links.append(
                    LinkData(
                        rel=Hyperlink.SHORT_DESCRIPTION,
                        content=short,
                        media_type="text/html",
                    )
                )

            # Add measurements: rating and popularity
            if book.get("starRating") is not None and book["starRating"] > 0:
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.RATING, value=book["starRating"]
                    )
                )

            if book.get("popularity"):
                measurements.append(
                    MeasurementData(
                        quantity_measured=Measurement.POPULARITY,
                        value=book["popularity"],
                    )
                )

            metadata = Metadata(
                data_source=DataSource.OVERDRIVE,
                title=title,
                subtitle=subtitle,
                sort_title=sort_title,
                language=language,
                medium=medium,
                series=series,
                publisher=publisher,
                imprint=imprint,
                published=published,
                primary_identifier=primary_identifier,
                identifiers=identifiers,
                subjects=subjects,
                contributors=contributors,
                measurements=measurements,
                links=links,
                duration=duration,
            )
        else:
            metadata = Metadata(
                data_source=DataSource.OVERDRIVE,
                primary_identifier=primary_identifier,
            )

        if include_formats:
            formats = []
            for format in book.get("formats", []):
                format_id = format["id"]
                internal_formats = list(cls.internal_formats(format_id))
                if internal_formats:
                    for content_type, drm_scheme in internal_formats:
                        formats.append(FormatData(content_type, drm_scheme))
                elif format_id not in cls.ignorable_overdrive_formats:
                    cls.logger().error(
                        "Could not process Overdrive format %s for %s",
                        format_id,
                        overdrive_id,
                    )

            # Also make a CirculationData so we can write the formats,
            circulationdata = CirculationData(
                data_source=DataSource.OVERDRIVE,
                primary_identifier=primary_identifier,
                formats=formats,
            )

            metadata.circulation = circulationdata

        return metadata


class OverdriveAdvantageAccount:
    """Holder and parser for data associated with Overdrive Advantage."""

    def __init__(self, parent_library_id: str, library_id: str, name: str, token: str):
        """Constructor.

        :param parent_library_id: The library ID of the parent Overdrive
            account.
        :param library_id: The library ID of the Overdrive Advantage account.
        :param name: The name of the library whose Advantage account this is.
        :param token: The collection token for this Advantage account
        """
        self.parent_library_id = parent_library_id
        self.library_id = library_id
        self.name = name
        self.token = token

    @classmethod
    def from_representation(cls, content):
        """Turn the representation of an advantageAccounts link into a list of
        OverdriveAdvantageAccount objects.

        :param content: The data obtained by following an advantageAccounts
            link.
        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        data = json.loads(content)
        parent_id = str(data.get("id"))
        accounts = data.get("advantageAccounts", {})
        for account in accounts:
            name = account["name"]
            products_link = account["links"]["products"]["href"]
            library_id = str(account.get("id"))
            name = account.get("name")
            token = account.get("collectionToken")
            yield cls(
                parent_library_id=parent_id,
                library_id=library_id,
                name=name,
                token=token,
            )

    def to_collection(self, _db):
        """Find or create a Collection object for this Overdrive Advantage
        account.

        :return: a 2-tuple of Collections (primary Overdrive
            collection, Overdrive Advantage collection)
        """
        # First find the parent Collection.
        parent = _db.execute(
            select(Collection)
            .join(IntegrationConfiguration)
            .where(
                IntegrationConfiguration.protocol == ExternalIntegration.OVERDRIVE,
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                IntegrationConfiguration.settings_dict.contains(
                    {"external_account_id": self.parent_library_id}
                ),
            )
        ).scalar_one_or_none()
        if parent is None:
            # Without the parent's credentials we can't access the child.
            raise ValueError(
                "Cannot create a Collection whose parent does not already exist."
            )
        name = parent.name + " / " + self.name
        child = _db.execute(
            select(Collection)
            .join(IntegrationConfiguration)
            .where(
                Collection.parent_id == parent.id,
                IntegrationConfiguration.protocol == ExternalIntegration.OVERDRIVE,
                IntegrationConfiguration.goal == Goals.LICENSE_GOAL,
                IntegrationConfiguration.settings_dict.contains(
                    {"external_account_id" == self.library_id}
                ),
            )
        ).scalar_one_or_none()

        if child is None:
            # The child doesn't exist yet. Create it.
            child, _ = Collection.by_name_and_protocol(
                _db, name, ExternalIntegration.OVERDRIVE
            )
            child.parent = parent
            child_settings = OverdriveChildSettings.construct(
                external_account_id=self.library_id
            )
            integration_settings_update(
                OverdriveChildSettings, child.integration_configuration, child_settings
            )
        else:
            # Set or update the name of the collection to reflect the name of
            # the library, just in case that name has changed.
            child.integration_configuration.name = name

        return parent, child


class OverdriveBibliographicCoverageProvider(BibliographicCoverageProvider):
    """Fill in bibliographic metadata for Overdrive records.

    This will occasionally fill in some availability information for a
    single Collection, but we rely on Monitors to keep availability
    information up to date for all Collections.
    """

    SERVICE_NAME = "Overdrive Bibliographic Coverage Provider"
    DATA_SOURCE_NAME = DataSource.OVERDRIVE
    PROTOCOL = ExternalIntegration.OVERDRIVE
    INPUT_IDENTIFIER_TYPES = Identifier.OVERDRIVE_ID

    def __init__(self, collection, api_class=OverdriveAPI, **kwargs):
        """Constructor.

        :param collection: Provide bibliographic coverage to all
            Overdrive books in the given Collection.
        :param api_class: Instantiate this class with the given Collection,
            rather than instantiating OverdriveAPI.
        """
        super().__init__(collection, **kwargs)
        if isinstance(api_class, OverdriveAPI):
            # Use a previously instantiated OverdriveAPI instance
            # rather than creating a new one.
            self.api = api_class
        else:
            # A web application should not use this option because it
            # will put a non-scoped session in the mix.
            _db = Session.object_session(collection)
            self.api = api_class(_db, collection)

    def process_item(self, identifier):
        info = self.api.metadata_lookup(identifier)
        error = None
        if info.get("errorCode") == "NotFound":
            error = "ID not recognized by Overdrive: %s" % identifier.identifier
        elif info.get("errorCode") == "InvalidGuid":
            error = "Invalid Overdrive ID: %s" % identifier.identifier

        if error:
            return self.failure(identifier, error, transient=False)

        metadata = OverdriveRepresentationExtractor.book_info_to_metadata(info)

        if not metadata:
            e = "Could not extract metadata from Overdrive data: %r" % info
            return self.failure(identifier, e)

        self.metadata_pre_hook(metadata)
        return self.set_metadata(identifier, metadata)

    def metadata_pre_hook(self, metadata):
        """A hook method that allows subclasses to modify a Metadata
        object derived from Overdrive before it's applied.
        """
        return metadata


class GenerateOverdriveAdvantageAccountList(InputScript):
    """Generates a CSV containing the following fields:
    circulation manager
    collection
    client_key
    external_account_id
    library_token
    advantage_name
    advantage_id
    advantage_token
    already_configured
    """

    def __init__(self, _db=None, *args, **kwargs):
        super().__init__(_db, *args, **kwargs)
        self._data: list[list[str]] = list()

    def _create_overdrive_api(self, collection: Collection):
        return OverdriveAPI(_db=self._db, collection=collection)

    def do_run(self, *args, **kwargs):
        parsed = GenerateOverdriveAdvantageAccountList.parse_command_line(
            _db=self._db, *args, **kwargs
        )
        query: Query = Collection.by_protocol(
            self._db, protocol=ExternalIntegration.OVERDRIVE
        )
        for collection in query.filter(Collection.parent_id == None):
            api = self._create_overdrive_api(collection=collection)
            client_key = api.client_key().decode()
            client_secret = api.client_secret().decode()
            library_id = api.library_id()

            try:
                library_token = api.collection_token
                advantage_accounts = api.get_advantage_accounts()

                for aa in advantage_accounts:
                    existing_child_collections = query.filter(
                        Collection.parent_id == collection.id
                    )
                    already_configured_aa_libraries = [
                        OverdriveAPI.child_settings_load(
                            e.integration_configuration
                        ).external_account_id
                        for e in existing_child_collections
                    ]
                    self._data.append(
                        [
                            collection.name,
                            library_id,
                            client_key,
                            client_secret,
                            library_token,
                            aa.name,
                            aa.library_id,
                            aa.token,
                            aa.library_id in already_configured_aa_libraries,
                        ]
                    )
            except Exception as e:
                logging.error(
                    f"Could not connect to collection {collection.name}: reason: {str(e)}."
                )

        file_path = parsed.output_file_path[0]
        circ_manager_name = parsed.circulation_manager_name[0]
        self.write_csv(output_file_path=file_path, circ_manager_name=circ_manager_name)

    def write_csv(self, output_file_path: str, circ_manager_name: str):
        with open(output_file_path, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(
                [
                    "cm",
                    "collection",
                    "overdrive_library_id",
                    "client_key",
                    "client_secret",
                    "library_token",
                    "advantage_name",
                    "advantage_id",
                    "advantage_token",
                    "already_configured",
                ]
            )
            for i in self._data:
                i.insert(0, circ_manager_name)
                writer.writerow(i)

    @classmethod
    def arg_parser(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--output-file-path",
            help="The path of an output file",
            metavar="o",
            nargs=1,
        )

        parser.add_argument(
            "--circulation-manager-name",
            help="The name of the circulation-manager",
            metavar="c",
            nargs=1,
            required=True,
        )

        parser.add_argument(
            "--file-format",
            help="The file format of the output file",
            metavar="f",
            nargs=1,
            default="csv",
        )

        return parser


class OverdriveAdvantageAccountListScript(Script):
    def run(self):
        """Explain every Overdrive collection and, for each one, all of its
        Advantage collections.
        """
        collections = Collection.by_protocol(self._db, ExternalIntegration.OVERDRIVE)
        for collection in collections:
            self.explain_main_collection(collection)
            print()

    def explain_main_collection(self, collection):
        """Explain an Overdrive collection and all of its Advantage
        collections.
        """
        api = OverdriveAPI(self._db, collection)
        print("Main Overdrive collection: %s" % collection.name)
        print("\n".join(collection.explain()))
        print("A few of the titles in the main collection:")
        for i, book in enumerate(api.all_ids()):
            print("", book["title"])
            if i > 10:
                break
        advantage_accounts = list(api.get_advantage_accounts())
        print("%d associated Overdrive Advantage account(s)." % len(advantage_accounts))
        for advantage_collection in advantage_accounts:
            self.explain_advantage_collection(advantage_collection)
            print()

    def explain_advantage_collection(self, collection):
        """Explain a single Overdrive Advantage collection."""
        parent_collection, child = collection.to_collection(self._db)
        print(" Overdrive Advantage collection: %s" % child.name)
        print(" " + ("\n ".join(child.explain())))
        print(" A few of the titles in this Advantage collection:")
        child_api = OverdriveAPI(self._db, child)
        for i, book in enumerate(child_api.all_ids()):
            print(" ", book["title"])
            if i > 10:
                break


class OverdriveManifestFulfillmentInfo(FulfillmentInfo):
    def __init__(
        self, collection, content_link, overdrive_identifier, scope_string, access_token
    ):
        """Constructor.

        Most of the arguments to the superconstructor can be assumed,
        and none of them matter all that much, since this class
        overrides the normal process by which a FulfillmentInfo becomes
        a Flask response.
        """
        super().__init__(
            collection=collection,
            data_source_name=DataSource.OVERDRIVE,
            identifier_type=Identifier.OVERDRIVE_ID,
            identifier=overdrive_identifier,
            content_link=content_link,
            content_type=None,
            content=None,
            content_expires=None,
        )
        self.scope_string = scope_string
        self.access_token = access_token

    @property
    def as_response(self):
        headers = {
            "Location": self.content_link,
            "X-Overdrive-Scope": self.scope_string,
            "X-Overdrive-Patron-Authorization": f"Bearer {self.access_token}",
            "Content-Type": self.content_type or "text/plain",
        }
        return flask.Response("", 302, headers)
