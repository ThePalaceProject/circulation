from __future__ import annotations

import datetime
import json
import re
import urllib.parse
from collections.abc import Callable, Generator, Iterable, Mapping
from functools import partial
from json import JSONDecodeError
from threading import RLock
from typing import Any, NamedTuple, cast

import flask
from requests import Response
from requests.structures import CaseInsensitiveDict
from sqlalchemy.orm import Session

from palace.manager.api.circulation import (
    BaseCirculationAPI,
    CirculationInternalFormatsMixin,
    DeliveryMechanismInfo,
    FetchFulfillment,
    Fulfillment,
    HoldInfo,
    LoanInfo,
    PatronActivityCirculationAPI,
    RedirectFulfillment,
)
from palace.manager.api.circulation_exceptions import (
    AlreadyOnHold,
    CannotFulfill,
    CannotHold,
    CannotLoan,
    CannotReleaseHold,
    CannotRenew,
    FormatNotAvailable,
    FulfilledOnIncompatiblePlatform,
    NoAcceptableFormat,
    NoActiveLoan,
    NoAvailableCopies,
    PatronAuthorizationFailedException,
    PatronHoldLimitReached,
    PatronLoanLimitReached,
)
from palace.manager.api.overdrive.advantage import OverdriveAdvantageAccount
from palace.manager.api.overdrive.constants import (
    OVERDRIVE_LABEL,
    OVERDRIVE_MAIN_ACCOUNT_ID,
    OverdriveConstants,
)
from palace.manager.api.overdrive.coverage import OverdriveBibliographicCoverageProvider
from palace.manager.api.overdrive.fulfillment import OverdriveManifestFulfillment
from palace.manager.api.overdrive.representation import OverdriveRepresentationExtractor
from palace.manager.api.overdrive.settings import (
    OverdriveChildSettings,
    OverdriveLibrarySettings,
    OverdriveSettings,
)
from palace.manager.api.overdrive.util import _make_link_safe
from palace.manager.api.selftest import HasCollectionSelfTests, SelfTestResult
from palace.manager.core.config import CannotLoadConfiguration, Configuration
from palace.manager.core.exceptions import BasePalaceException, IntegrationException
from palace.manager.core.metadata_layer import ReplacementPolicy
from palace.manager.integration.base import HasChildIntegrationConfiguration
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Loan, Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import strptime_utc, utc_now
from palace.manager.util.http import HTTP, BadResponseException


class OverdriveToken(NamedTuple):
    token: str
    expires: datetime.datetime


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
    DEFAULT_ERROR_URL = "http://thepalaceproject.org/"

    # Map Overdrive's error messages to standard circulation manager
    # exceptions.
    ERROR_MESSAGE_TO_EXCEPTION = {
        "PatronHasExceededCheckoutLimit": PatronLoanLimitReached,
        "PatronHasExceededCheckoutLimit_ForCPC": PatronLoanLimitReached,
    }

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
    EVENTS_ENDPOINT = "%(host)s/v1/collections/%(collection_token)s/products?lastUpdateTime=%(lastupdatetime)s&limit=%(limit)s"
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
    def settings_class(cls) -> type[OverdriveSettings]:
        return OverdriveSettings

    @classmethod
    def library_settings_class(cls) -> type[OverdriveLibrarySettings]:
        return OverdriveLibrarySettings

    @classmethod
    def child_settings_class(cls) -> type[OverdriveChildSettings]:
        return OverdriveChildSettings

    @classmethod
    def label(cls) -> str:
        return OVERDRIVE_LABEL

    @classmethod
    def description(cls) -> str:
        return "Integrate an Overdrive collection. For an Overdrive Advantage collection, select the consortium's Overdrive collection as the parent."

    def __init__(self, _db: Session, collection: Collection) -> None:
        super().__init__(_db, collection)

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

        library_id = self._settings.external_account_id
        if not library_id:
            raise ValueError(
                "Collection %s must have an external account ID" % collection.id
            )
        self._library_id = library_id

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

        # This is set by access to ._client_oauth_token
        self._cached_client_oauth_token: OverdriveToken | None = None

        # This is set by an access to .collection_token
        self._collection_token: str | None = None
        self.overdrive_bibliographic_coverage_provider = (
            OverdriveBibliographicCoverageProvider(collection, api=self)
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

    def endpoint(self, url: str, **kwargs: str) -> str:
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
    def _client_oauth_token(self) -> str:
        """
        The client oauth bearer token used for authentication with
        Overdrive for this collection.

        This token is refreshed as needed and cached for reuse
        by this property.

        See: https://developer.overdrive.com/docs/api-security
             https://developer.overdrive.com/apis/client-auth
        """
        if (
            token := self._cached_client_oauth_token
        ) is not None and utc_now() < token.expires:
            return token.token

        return self._refresh_client_oauth_token().token

    def _refresh_client_oauth_token(self) -> OverdriveToken:
        with self.lock:
            response = self._do_post(
                self.TOKEN_ENDPOINT,
                dict(grant_type="client_credentials"),
                {"Authorization": self._collection_context_basic_auth_header},
                allowed_response_codes=[200],
            )
            data = response.json()
            access_token = data["access_token"]
            expires_in = data["expires_in"] * 0.9
            expires = utc_now() + datetime.timedelta(seconds=expires_in)
            self._cached_client_oauth_token = OverdriveToken(
                token=access_token, expires=expires
            )
            return self._cached_client_oauth_token

    @property
    def collection_token(self) -> str:
        """Get the token representing this particular Overdrive collection.

        As a side effect, this will verify that the Overdrive
        credentials are working.
        """
        collection_token = self._collection_token
        if not collection_token:
            library = self.get_library()
            error = library.get("errorCode")
            if error:
                message = library.get("message")
                raise CannotLoadConfiguration(
                    "Overdrive credentials are valid but could not fetch library: %s"
                    % message
                )
            collection_token = cast(str, library["collectionToken"])
            self._collection_token = collection_token
        return collection_token

    @property
    def source(self) -> DataSource:
        return DataSource.lookup(self._db, DataSource.OVERDRIVE, autocreate=True)

    def ils_name(self, library: Library) -> str:
        """Determine the ILS name to use for the given Library."""
        config = self.integration_configuration().for_library(library.id)
        if not config:
            return self.ILS_NAME_DEFAULT
        return self.library_settings_load(config).ils_name

    @property
    def advantage_library_id(self) -> int:
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
            return OVERDRIVE_MAIN_ACCOUNT_ID
        return int(self._library_id)

    def get(
        self,
        url: str,
        extra_headers: dict[str, str] | None = None,
        exception_on_401: bool = False,
    ) -> tuple[int, CaseInsensitiveDict[str], bytes]:
        """Make an HTTP GET request using the active Bearer Token."""
        request_headers = dict(Authorization="Bearer %s" % self._client_oauth_token)
        if extra_headers:
            request_headers.update(extra_headers)

        response: Response = self._do_get(
            url, request_headers, allowed_response_codes=["2xx", "3xx", "401", "404"]
        )
        status_code = response.status_code
        headers = response.headers
        content = response.content

        if status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise BadResponseException(
                    url,
                    "Something's wrong with the Overdrive OAuth Bearer Token!",
                    response,
                )
            else:
                # Force a refresh of the token and try again.
                self._refresh_client_oauth_token()
                return self.get(url, extra_headers, True)
        else:
            return status_code, headers, content

    @property
    def _collection_context_basic_auth_header(self) -> str:
        """
        Returns the Basic Auth header used to acquire an OAuth bearer token.

        This header contains the collection's credentials that were configured
        through the admin interface for this specific collection.
        """
        credentials = f"{self.client_key()}:{self.client_secret()}"
        return "Basic " + base64.standard_b64encode(credentials).strip()

    @property
    def _palace_context_basic_auth_header(self) -> str:
        """
        Returns the Basic Auth header used to acquire an OAuth bearer token.

        This header contains the Palace Project credentials passed into the
        Circulation Manager via environment variables. This is used to acquire
        a privileged token that has extra permissions for the Overdrive API.
        """
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
            raise CannotFulfill() from e

        s = b"%s:%s" % (
            client_credentials["key"].encode(),
            client_credentials["secret"].encode(),
        )
        return "Basic " + base64.standard_b64encode(s).strip()

    @staticmethod
    def _update_credential(
        credential: Credential, overdrive_data: dict[str, Any]
    ) -> None:
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

    def get_library(self) -> dict[str, Any]:
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
            return json.loads(representation.content)  # type: ignore[no-any-return]

    def get_advantage_accounts(self) -> Generator[OverdriveAdvantageAccount]:
        """Find all the Overdrive Advantage accounts managed by this library.

        :yield: A sequence of OverdriveAdvantageAccount objects.
        """
        library = self.get_library()
        links = library.get("links", {})
        advantage = links.get("advantageAccounts")
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
            yield from OverdriveAdvantageAccount.from_representation(
                representation.content
            )
        return

    def all_ids(self) -> Generator[dict[str, str]]:
        """Get IDs for every book in the system, with the most recently added
        ones at the front.
        """
        next_link: str | None = self._all_products_link
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
        return _make_link_safe(url)

    def _get_book_list_page(
        self,
        link: str,
        rel_to_follow: str = "next",
        extractor_class: type[OverdriveRepresentationExtractor] | None = None,
    ) -> tuple[list[dict[str, str]], str | None]:
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
        content_dict = json.loads(content)

        # Find the link to the next page of results, if any.
        next_link = extractor_class.link(content_dict, rel_to_follow)

        # Prepare to get availability information for all the books on
        # this page.
        availability_queue = extractor_class.availability_link_list(content_dict)
        return availability_queue, next_link

    def recently_changed_ids(
        self, start: datetime.datetime, cutoff: datetime.datetime | None
    ) -> Generator[dict[str, str]]:
        """Get IDs of books whose status has changed between the start time
        and now.
        """
        # `cutoff` is not supported by Overdrive, so we ignore it. All
        # we can do is get events between the start time and now.

        last_update_time = start - self.EVENT_DELAY
        self.log.info("Asking for circulation changes since %s", last_update_time)
        last_update = last_update_time.strftime(self.TIME_FORMAT)

        initial_next_link = self.endpoint(
            self.EVENTS_ENDPOINT,
            # From https://developer.overdrive.com/apis/search:
            # "**Note: When you search using the lastTitleUpdateTime or
            # lastUpdateTime parameters, your results will be automatically
            # sorted in ascending order (and all other sort options will be ignored)."
            lastupdatetime=last_update,
            limit=str(self.PAGE_SIZE_LIMIT),
            collection_token=self.collection_token,
        )
        next_link: str | None = _make_link_safe(initial_next_link)
        while next_link:
            page_inventory, next_link = self._get_book_list_page(next_link)
            # We won't be sending out any events for these books yet,
            # because we don't know if anything changed, but we will
            # be putting them on the list of inventory items to
            # refresh. At that point we will send out events.
            yield from page_inventory

    def metadata_lookup(self, identifier: Identifier) -> dict[str, Any]:
        """Look up metadata for an Overdrive identifier."""
        url = self.endpoint(
            self.METADATA_ENDPOINT,
            collection_token=self.collection_token,
            item_id=identifier.identifier,
        )
        status_code, headers, content = self.get(url)
        return json.loads(content)  # type: ignore[no-any-return]

    def _do_get(self, url: str, headers: dict[str, str], **kwargs: Any) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self.settings.max_retry_count
        kwargs["timeout"] = 120
        return HTTP.get_with_timeout(url, headers=headers, **kwargs)

    def _do_post(
        self, url: str, payload: dict[str, str], headers: dict[str, str], **kwargs: Any
    ) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self.settings.max_retry_count
        kwargs["timeout"] = 120
        return HTTP.post_with_timeout(url, data=payload, headers=headers, **kwargs)

    def website_id(self) -> str:
        return self.settings.overdrive_website_id

    def client_key(self) -> str:
        return self.settings.overdrive_client_key

    def client_secret(self) -> str:
        return self.settings.overdrive_client_secret

    def library_id(self) -> str:
        return self._library_id

    def hosts(self) -> dict[str, str]:
        return dict(self._hosts)

    def _run_self_tests(self, _db: Session) -> Generator[SelfTestResult]:
        result = self.run_test(
            "Checking global Client Authentication privileges",
            self._refresh_client_oauth_token,
        )
        yield result
        if not result.success:
            # There is no point in running the other tests if we
            # can't even get a token.
            return

        def _count_advantage() -> str:
            """Count the Overdrive Advantage accounts"""
            accounts = list(self.get_advantage_accounts())
            return "Found %d Overdrive Advantage account(s)." % len(accounts)

        yield self.run_test("Looking up Overdrive Advantage accounts", _count_advantage)

        def _count_books() -> str:
            """Count the titles in the collection."""
            url = self._all_products_link
            status, headers, body = self.get(url, {})
            json_data = json.loads(body)
            return "%d item(s) in collection" % json_data["totalItems"]

        yield self.run_test("Counting size of collection", _count_books)

        collection = self.collection
        if collection is not None:
            for default_patrons_result in self.default_patrons(collection):
                if isinstance(default_patrons_result, SelfTestResult):
                    yield default_patrons_result
                    continue
                library, patron, pin = default_patrons_result
                task = (
                    "Checking Patron Authentication privileges, using test patron for library %s"
                    % library.name
                )
                yield self.run_test(
                    task, self._get_patron_oauth_credential, patron, pin
                )

    def patron_request(
        self,
        patron: Patron,
        pin: str | None,
        url: str,
        extra_headers: dict[str, str] | None = None,
        data: str | None = None,
        exception_on_401: bool = False,
        method: str | None = None,
        palace_context: bool = False,
    ) -> Response:
        """
        Make an HTTP request on behalf of a patron to Overdrive's API.

        If palace_context == True, the request will be performed using privileged
        Palace Project credentials, which provide extended API access. Otherwise,
        it will use the collection's configured credentials.
        """
        patron_credential = self._get_patron_oauth_credential(
            patron, pin, palace_context=palace_context
        )
        headers = dict(Authorization="Bearer %s" % patron_credential.credential)
        if extra_headers:
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
                self._refresh_patron_oauth_token(
                    patron_credential, patron, pin, palace_context=palace_context
                )
                return self.patron_request(patron, pin, url, extra_headers, data, True)
        else:
            # This is commented out because it may expose patron
            # information.
            #
            # self.log.debug("%s: %s", url, response.status_code)
            return response

    def _get_patron_oauth_credential(
        self, patron: Patron, pin: str | None, palace_context: bool = False
    ) -> Credential:
        """Get an Overdrive OAuth token for the given patron.

        See: https://developer.overdrive.com/apis/patron-auth

        :param patron: The patron for whom to fetch the credential.
        :param pin: The patron's PIN or password.
        :param palace_context: Determines if the oauth token is fetched
           using the palace credentials or the collections credentials.
        """

        refresh = partial(
            self._refresh_patron_oauth_token,
            patron=patron,
            pin=pin,
            palace_context=palace_context,
        )

        return Credential.lookup(
            self._db,
            DataSource.OVERDRIVE,
            (
                "Palace Context Patron OAuth Token"
                if palace_context
                else "Collection Context Patron OAuth Token"
            ),
            patron,
            refresh,
            collection=self.collection,
        )

    def scope_string(self, library: Library) -> str:
        """Create the Overdrive scope string for the given library.

        This is used when setting up Patron Authentication, and when
        generating the X-Overdrive-Scope header used by apps to set up
        their own Patron Authentication.
        """
        return "websiteid:{} authorizationname:{}".format(
            self.settings.overdrive_website_id,
            self.ils_name(library),
        )

    def _refresh_patron_oauth_token(
        self,
        credential: Credential,
        patron: Patron,
        pin: str | None,
        palace_context: bool = False,
    ) -> Credential:
        """Request an OAuth bearer token that allows us to act on
        behalf of a specific patron.

        Documentation: https://developer.overdrive.com/apis/patron-auth
        """
        payload = dict(
            grant_type="password",
            scope=self.scope_string(patron.library),
        )
        if patron.authorization_identifier:
            payload["username"] = patron.authorization_identifier
        if pin:
            # A PIN was provided.
            payload["password"] = pin
        else:
            # No PIN was provided. Depending on the library,
            # this might be fine. If it's not fine, Overdrive will
            # refuse to issue a token.
            payload["password_required"] = "false"
            payload["password"] = "[ignore]"
        try:
            response = self._do_post(
                self.PATRON_TOKEN_ENDPOINT,
                payload,
                {
                    "Authorization": (
                        self._palace_context_basic_auth_header
                        if palace_context
                        else self._collection_context_basic_auth_header
                    ),
                },
                allowed_response_codes=["2xx"],
            )
        except BadResponseException as e:
            try:
                response_data = e.response.json()
            except JSONDecodeError:
                self.log.exception(
                    f"Error parsing Overdrive response. "
                    f"Status code: {e.response.status_code}. Response: {e.response.text}"
                )
                response_data = {}
            error_code = response_data.get("error")
            error_description = response_data.get(
                "error_description", "Failed to authenticate with Overdrive"
            )
            debug_message = (
                f"_refresh_patron_oauth_token failed. Status code: '{e.response.status_code}'. "
                f"Error: '{error_code}'. Description: '{error_description}'."
            )
            self.log.info(debug_message + f" Response: '{e.response.text}'")
            raise PatronAuthorizationFailedException(
                error_description, debug_message
            ) from e

        self._update_credential(credential, response.json())
        return credential

    def checkout(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism | None,
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
        loan = LoanInfo.from_license_pool(
            licensepool,
            end_date=expires,
        )
        return loan

    def _process_checkout_error(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        error: dict[str, Any] | str,
    ) -> LoanInfo:
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
            return LoanInfo.from_license_pool(
                licensepool,
                end_date=expires,
            )

        if code in self.ERROR_MESSAGE_TO_EXCEPTION:
            exc_class = self.ERROR_MESSAGE_TO_EXCEPTION[code]
            raise exc_class()

        # All-purpose fallback
        raise CannotLoan(code)

    def checkin(
        self, patron: Patron, pin: str | None, licensepool: LicensePool
    ) -> None:
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
        self.patron_request(patron, pin, url, method="DELETE")

    def perform_early_return(
        self,
        patron: Patron,
        pin: str | None,
        loan: Loan,
        http_get: Callable[..., Response] | None = None,
    ) -> bool:
        """Ask Overdrive for a loanEarlyReturnURL for the given loan
        and try to hit that URL.

        :param patron: A Patron
        :param pin: Authorization PIN for the patron
        :param loan: A Loan object corresponding to the title on loan.
        :param http_get: You may pass in a mock of HTTP.get_with_timeout
            for use in tests.
        """
        if loan.fulfillment is None:
            return False
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
        result = self.get_fulfillment_link(
            patron, pin, loan.license_pool.identifier.identifier, internal_format
        )
        if isinstance(result, Fulfillment):
            raise RuntimeError("Unexpected Fulfillment object: %r" % result)
        url, media_type = result
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
    def _extract_early_return_url(cls, location: str | None) -> str | None:
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
        return None

    def fill_out_form(self, **values: str) -> tuple[dict[str, str], str]:
        fields = []
        for k, v in list(values.items()):
            fields.append(dict(name=k, value=v))
        headers = {"Content-Type": "application/json; charset=utf-8"}
        return headers, json.dumps(dict(fields=fields))

    error_to_exception = {
        "TitleNotCheckedOut": NoActiveLoan,
    }

    def raise_exception_on_error(
        self,
        data: Mapping[str, str],
        custom_error_to_exception: (
            Mapping[str, type[BasePalaceException]] | None
        ) = None,
    ) -> None:
        if not "errorCode" in data:
            return
        error = data["errorCode"]
        message = data.get("message") or ""

        if custom_error_to_exception is None:
            custom_error_to_exception = {}

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
        data = self.patron_request(patron, pin, url, palace_context=True).json()
        self.raise_exception_on_error(data)
        return data  # type: ignore[no-any-return]

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
    ) -> Fulfillment:
        """Get the actual resource file to the patron."""
        internal_format = self.internal_format(delivery_mechanism)
        try:
            result = self.get_fulfillment_link(
                patron, pin, licensepool.identifier.identifier, internal_format
            )
            if isinstance(result, Fulfillment):
                # The fulfillment process was short-circuited, probably
                # by the creation of an OverdriveManifestFulfillment.
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

        if fulfillment_force_redirect:
            return RedirectFulfillment(
                content_link=url,
                content_type=media_type,
            )
        else:
            return FetchFulfillment(
                content_link=url,
                content_type=media_type,
            )

    def get_fulfillment_link(
        self, patron: Patron, pin: str | None, overdrive_id: str, format_type: str
    ) -> OverdriveManifestFulfillment | tuple[str, str]:
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
            response_json = response.json()
            try:
                download_link = self.extract_download_link(
                    response_json, self.DEFAULT_ERROR_URL
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
                fulfillment_access_token = self._get_patron_oauth_credential(
                    patron,
                    pin,
                    palace_context=True,
                ).credential
                # The credential should never be None, but mypy doesn't know that, so
                # we assert to be safe.
                assert fulfillment_access_token is not None
                return OverdriveManifestFulfillment(
                    download_link,
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
        self,
        patron: Patron,
        pin: str | None,
        download_link: str,
        fulfill_url: str | None = None,
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

    def extract_content_link(
        self, content_link_gateway_json: dict[str, Any]
    ) -> tuple[str, str]:
        link = content_link_gateway_json["links"]["contentlink"]
        return link["href"], link["type"]

    def lock_in_format(
        self, patron: Patron, pin: str | None, overdrive_id: str, format_type: str
    ) -> Response:
        overdrive_id = overdrive_id.upper()
        headers, document = self.fill_out_form(
            reserveId=overdrive_id, formatType=format_type
        )
        url = self.endpoint(self.FORMATS_ENDPOINT, overdrive_id=overdrive_id)
        return self.patron_request(patron, pin, url, headers, document)

    @classmethod
    def extract_data_from_checkout_response(
        cls, checkout_response_json: dict[str, Any], format_type: str, error_url: str
    ) -> tuple[datetime.datetime | None, str | None]:
        expires = cls.extract_expiration_date(checkout_response_json)
        return expires, cls.get_download_link(
            checkout_response_json, format_type, error_url
        )

    @classmethod
    def extract_data_from_hold_response(
        cls, hold_response_json: dict[str, Any]
    ) -> tuple[int, datetime.datetime | None]:
        position = hold_response_json["holdListPosition"]
        placed = cls._extract_date(hold_response_json, "holdPlacedDate")
        return position, placed

    @classmethod
    def extract_expiration_date(cls, data: dict[str, Any]) -> datetime.datetime | None:
        return cls._extract_date(data, "expires")

    @classmethod
    def _extract_date(
        cls, data: dict[str, Any] | Any, field_name: str
    ) -> datetime.datetime | None:
        if not isinstance(data, dict):
            return None
        if not field_name in data:
            return None
        try:
            return strptime_utc(data[field_name], cls.TIME_FORMAT)
        except ValueError as e:
            # Wrong format
            return None

    def get_patron_information(self, patron: Patron, pin: str | None) -> dict[str, Any]:
        data = self.patron_request(patron, pin, self.ME_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data  # type: ignore[no-any-return]

    def get_patron_checkouts(self, patron: Patron, pin: str | None) -> dict[str, Any]:
        """Get information for the given patron's loans.

        :param patron: A patron.
        :param pin: An optional PIN/password for the patron.
        :return: Information about the patron's loans.
        """
        data = self.patron_request(
            patron, pin, self.CHECKOUTS_ENDPOINT, palace_context=True
        ).json()
        self.raise_exception_on_error(data)
        return data  # type: ignore[no-any-return]

    def get_patron_holds(self, patron: Patron, pin: str | None) -> dict[str, Any]:
        data = self.patron_request(patron, pin, self.HOLDS_ENDPOINT).json()
        self.raise_exception_on_error(data)
        return data  # type: ignore[no-any-return]

    @classmethod
    def _pd(cls, d: str | None) -> datetime.datetime | None:
        """Stupid method to parse a date.

        TIME_FORMAT mentions "Z" for Zulu time, which is the same as
        UTC.
        """
        if not d:
            return None
        return strptime_utc(d, cls.TIME_FORMAT)

    def patron_activity(
        self, patron: Patron, pin: str | None
    ) -> Iterable[LoanInfo | HoldInfo]:
        collection = self.collection
        if collection is None or collection.id is None:
            raise BasePalaceException(
                "No collection available for Overdrive patron activity."
            )

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
            loan_info = self.process_checkout_data(checkout, collection.id)
            if loan_info is not None:
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
                collection_id=collection.id,
                identifier_type=Identifier.OVERDRIVE_ID,
                identifier=overdrive_identifier,
                start_date=start,
                end_date=end,
                hold_position=position,
            )

    @classmethod
    def process_checkout_data(
        cls, checkout: dict[str, Any], collection_id: int
    ) -> LoanInfo | None:
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
            collection_id=collection_id,
            identifier_type=Identifier.OVERDRIVE_ID,
            identifier=overdrive_identifier,
            start_date=start,
            end_date=end,
            locked_to=locked_to,
        )

    def default_notification_email_address(
        self, patron: Patron, pin: str | None
    ) -> str | None:
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

    def place_hold(
        self,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
        notification_email_address: str | None,
    ) -> HoldInfo:
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
        form_fields: dict[str, Any] = dict(reserveId=overdrive_id)
        if notification_email_address:
            form_fields["emailAddress"] = notification_email_address
        else:
            form_fields["ignoreHoldEmail"] = True

        headers, document = self.fill_out_form(**form_fields)
        response = self.patron_request(
            patron, pin, self.HOLDS_ENDPOINT, headers, document
        )
        return self.process_place_hold_response(response, patron, pin, licensepool)

    def process_place_hold_response(
        self,
        response: Response,
        patron: Patron,
        pin: str | None,
        licensepool: LicensePool,
    ) -> HoldInfo:
        """Process the response to a HOLDS_ENDPOINT request.

        :return: A HoldData object, if a hold was successfully placed,
            or the book was already on hold.
        :raise: A CirculationException explaining why no hold
            could be placed.
        """

        family = response.status_code // 100

        if family == 4:
            error = response.json()
            if not error or not "errorCode" in error:
                raise CannotHold()
            code = error["errorCode"]
            if code == "AlreadyOnWaitList":
                # The book is already on hold.
                raise AlreadyOnHold()
            elif code == "NotWithinRenewalWindow":
                # The patron has this book checked out and cannot yet
                # renew their loan.
                raise CannotRenew()
            elif code == "PatronExceededHoldLimit":
                raise PatronHoldLimitReached()
            else:
                raise CannotHold(code)
        elif family == 2:
            # The book was successfully placed on hold. Return an
            # appropriate HoldInfo.
            data = response.json()
            position, date = self.extract_data_from_hold_response(data)
            return HoldInfo.from_license_pool(
                licensepool,
                start_date=date,
                hold_position=position,
            )
        else:
            # Some other problem happened -- we don't know what.  It's
            # not a 5xx error because the HTTP client would have been
            # turned that into a RemoteIntegrationException.
            raise CannotHold()

    def release_hold(
        self, patron: Patron, pin: str | None, licensepool: LicensePool
    ) -> None:
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
            return
        if not response.content:
            raise CannotReleaseHold()
        data = response.json()
        if not "errorCode" in data:
            raise CannotReleaseHold()
        if data["errorCode"] == "PatronDoesntHaveTitleOnHold":
            # There was never a hold to begin with, so we're fine.
            return
        raise CannotReleaseHold(debug_info=response.text)

    def circulation_lookup(
        self, book: str | dict[str, str]
    ) -> tuple[dict[str, Any], tuple[int, CaseInsensitiveDict[str], bytes]]:
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
            circulation_link = _make_link_safe(circulation_link)
        return book, self.get(circulation_link, {})

    def update_formats(self, licensepool: LicensePool) -> None:
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
        metadata.apply(edition, self.collection, replace=replace, db=self._db)

    def update_licensepool(
        self, book_id: str | dict[str, Any]
    ) -> tuple[LicensePool | None, bool | None, bool]:
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
        book.update(json.loads(content))

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
    def update_availability(self, licensepool: LicensePool) -> None:
        self.update_licensepool(licensepool.identifier.identifier)

    def _edition(self, licensepool: LicensePool) -> tuple[Edition, bool]:
        """Find or create the Edition that would be used to contain
        Overdrive metadata for the given LicensePool.
        """
        return Edition.for_foreign_id(
            self._db,
            self.source,
            licensepool.identifier.type,
            licensepool.identifier.identifier,
        )

    def update_licensepool_with_book_info(
        self, book: dict[str, Any], license_pool: LicensePool, is_new_pool: bool
    ) -> tuple[LicensePool, bool, bool]:
        """Update a book's LicensePool with information from a JSON
        representation of its circulation info.

        Then, create an Edition and make sure it has bibliographic
        coverage. If the new Edition is the only candidate for the
        pool's presentation_edition, promote it to presentation
        status.
        """
        extractor = OverdriveRepresentationExtractor(self)
        circulation = extractor.book_info_to_circulation(book)
        lp, circulation_changed = circulation.apply(self._db, license_pool.collection)
        if lp is not None:
            license_pool = lp

        edition, is_new_edition = self._edition(license_pool)

        if is_new_pool:
            license_pool.open_access = False
            self.log.info("New Overdrive book discovered: %r", edition)
        return license_pool, is_new_pool, circulation_changed

    @classmethod
    def get_download_link(
        self, checkout_response: dict[str, Any], format_type: str, error_url: str
    ) -> str | None:
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
                msg = "Could not find specified format %s. Available formats: %s"
                raise NoAcceptableFormat(
                    msg % (use_format_type, ", ".join(available_formats))
                )

        return self.extract_download_link(format, error_url, fetch_manifest)

    @classmethod
    def extract_download_link(
        cls, format: dict[str, Any], error_url: str, fetch_manifest: bool = False
    ) -> str | None:
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
            return download_link  # type: ignore[no-any-return]
        else:
            return None

    @classmethod
    def make_direct_download_link(cls, link: str) -> str:
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
