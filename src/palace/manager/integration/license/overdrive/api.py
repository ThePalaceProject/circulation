from __future__ import annotations

import asyncio
import datetime
import json
from collections import deque
from collections.abc import Generator, Iterable
from dataclasses import dataclass
from functools import partial
from threading import RLock
from typing import Any, NamedTuple, Unpack, cast, overload
from urllib.parse import urlsplit

import flask
from httpx import Limits, Timeout
from pydantic import ValidationError
from requests import Response
from requests.structures import CaseInsensitiveDict
from sqlalchemy.orm import Session

from palace.manager.api.circulation.base import (
    BaseCirculationAPI,
    CirculationInternalFormatsMixin,
    PatronActivityCirculationAPI,
)
from palace.manager.api.circulation.data import HoldInfo, LoanInfo
from palace.manager.api.circulation.exceptions import (
    AlreadyCheckedOut,
    CannotFulfill,
    CannotHold,
    CannotLoan,
    CannotReleaseHold,
    CannotReturn,
    DeliveryMechanismError,
    FormatNotAvailable,
    FulfilledOnIncompatiblePlatform,
    NoAcceptableFormat,
    NoActiveLoan,
    NoAvailableCopies,
    NotCheckedOut,
    PatronAuthorizationFailedException,
)
from palace.manager.api.circulation.fulfillment import (
    FetchFulfillment,
    Fulfillment,
    RedirectFulfillment,
)
from palace.manager.api.selftest import HasCollectionSelfTests, SelfTestResult
from palace.manager.core.config import CannotLoadConfiguration, Configuration
from palace.manager.core.exceptions import IntegrationException
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.base import HasChildIntegrationConfiguration
from palace.manager.integration.license.overdrive.advantage import (
    OverdriveAdvantageAccount,
)
from palace.manager.integration.license.overdrive.constants import (
    OVERDRIVE_FORMATS,
    OVERDRIVE_INCOMPATIBLE_FORMATS,
    OVERDRIVE_LABEL,
    OVERDRIVE_LOCK_IN_FORMATS,
    OVERDRIVE_MAIN_ACCOUNT_ID,
    OVERDRIVE_OPEN_FORMATS,
    OVERDRIVE_PALACE_MANIFEST_FORMATS,
    OVERDRIVE_STREAMING_FORMATS,
    OverdriveConstants,
)
from palace.manager.integration.license.overdrive.coverage import (
    OverdriveBibliographicCoverageProvider,
)
from palace.manager.integration.license.overdrive.exception import (
    InvalidFieldOptionError,
    OverdriveModelError,
    OverdriveResponseException,
    OverdriveValidationError,
)
from palace.manager.integration.license.overdrive.fulfillment import (
    OverdriveManifestFulfillment,
)
from palace.manager.integration.license.overdrive.model import (
    BaseOverdriveModel,
    Checkout,
    Checkouts,
    ErrorResponse,
    Format,
    Hold as HoldResponse,
    Holds as HoldsResponse,
    PatronInformation,
    PatronRequestCallable,
    _overdrive_field_request,
)
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.integration.license.overdrive.settings import (
    OverdriveChildSettings,
    OverdriveLibrarySettings,
    OverdriveSettings,
)
from palace.manager.integration.license.overdrive.util import _make_link_safe
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
    RightsStatus,
)
from palace.manager.sqlalchemy.model.patron import Hold, Patron
from palace.manager.sqlalchemy.model.resource import Representation
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util import base64
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.async_http import WORKER_DEFAULT_BACKOFF, AsyncClient
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.http.http import HTTP, RequestKwargs


class OverdriveToken(NamedTuple):
    token: str
    expires: datetime.datetime


@dataclass
class BookInfoEndpoint:
    url: str


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
    delivery_mechanism_to_internal_format: dict[tuple[str | None, str | None], str] = {
        (epub, no_drm): "ebook-epub-open",
        (epub, adobe_drm): "ebook-epub-adobe",
        (pdf, no_drm): "ebook-pdf-open",
        (streaming_text, streaming_drm): "ebook-overdrive",
        (streaming_audio, streaming_drm): "audiobook-overdrive",
        (overdrive_audiobook_manifest, libby_drm): "audiobook-overdrive-manifest",
    }

    internal_format_to_delivery_mechanism = {
        v: k for k, v in delivery_mechanism_to_internal_format.items()
    }

    # TODO: This is a terrible choice but this URL should never be
    # displayed to a patron, so it doesn't matter much.
    DEFAULT_ERROR_URL = "http://thepalaceproject.org/"

    # A lock for threaded usage.
    lock = RLock()

    # Production and testing have different host names for some of the
    # API endpoints. This is configurable on the collection level.
    # Production and testing setups use the same URLs for Client
    # Authentication and Patron Authentication, but we use the same
    # system as for other hostnames to give a consistent look to the
    # templates.
    HOSTS = {
        OverdriveConstants.PRODUCTION_SERVERS: dict(
            host="https://api.overdrive.com",
            patron_host="https://patron.api.overdrive.com",
            oauth_patron_host="https://oauth-patron.overdrive.com",
            oauth_host="https://oauth.overdrive.com",
        ),
        OverdriveConstants.TESTING_SERVERS: dict(
            host="https://integration.api.overdrive.com",
            patron_host="https://integration-patron.api.overdrive.com",
            oauth_patron_host="https://oauth-patron.overdrive.com",
            oauth_host="https://oauth.overdrive.com",
        ),
    }

    # Each of these endpoint URLs has a slot to plug in one of the
    # appropriate servers. This will be filled in either by a call to
    # the endpoint() method (if there are other variables in the
    # template), or by the _do_get or _do_post methods (if there are
    # no other variables).
    TOKEN_ENDPOINT = "%(oauth_host)s/token"
    PATRON_TOKEN_ENDPOINT = "%(oauth_patron_host)s/patrontoken"

    HOST_ENDPOINT_BASE = "%(host)s"
    LIBRARY_ENDPOINT = "%(host)s/v1/libraries/%(library_id)s"
    ADVANTAGE_LIBRARY_ENDPOINT = (
        "%(host)s/v1/libraries/%(parent_library_id)s/advantageAccounts/%(library_id)s"
    )
    ALL_PRODUCTS_ENDPOINT = f"{HOST_ENDPOINT_BASE}/v1/collections/%(collection_token)s/products?sort=%(sort)s"

    METADATA_ENDPOINT_BASE = "/v1/collections/%(collection_token)s/products"

    METADATA_ENDPOINT = (
        f"{HOST_ENDPOINT_BASE}{METADATA_ENDPOINT_BASE}/%(item_id)s/metadata"
    )

    EVENTS_ENDPOINT_BASE = "/v1/collections/%(collection_token)s/products"
    EVENTS_ENDPOINT = (
        "%(host)s"
        + EVENTS_ENDPOINT_BASE
        + "?lastUpdateTime=%(lastupdatetime)s&limit=%(limit)s"
    )

    AVAILABILITY_ENDPOINT_BASE = "/v2/collections/%(collection_token)s/products"
    AVAILABILITY_ENDPOINT = (
        f"{HOST_ENDPOINT_BASE}{AVAILABILITY_ENDPOINT_BASE}/%(product_id)s/availability"
    )

    PATRON_INFORMATION_ENDPOINT = "%(patron_host)s/v1/patrons/me"
    CHECKOUTS_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s"
    HOLDS_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds"
    HOLD_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds/%(product_id)s"

    MAX_CREDENTIAL_AGE = 50 * 60

    PAGE_SIZE_LIMIT = 300
    EVENT_SOURCE = "Overdrive"

    EVENT_DELAY = datetime.timedelta(minutes=120)

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

        # This is set by access to .collection_token
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
    def data_source(self) -> DataSource:
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
        request_headers = self._get_headers(self._client_oauth_token)
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

        s = "{}:{}".format(
            client_credentials["key"],
            client_credentials["secret"],
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

    def _get_headers(self, auth_token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {auth_token}"}

    def book_info_initial_endpoint(
        self,
        start: datetime.datetime | None = None,
        page_size: int = PAGE_SIZE_LIMIT,
    ) -> BookInfoEndpoint:
        """Create an initial book info url."""

        # if no start date specified, assume effect beginning of time.
        if not start:
            start = datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=datetime.UTC)
        last_update_time = start - self.EVENT_DELAY
        self.log.info("Creating url for circulation changes since %s", last_update_time)
        last_update = last_update_time.strftime(self.TIME_FORMAT)

        book_info_initial_endpoint = self.endpoint(
            self.EVENTS_ENDPOINT,
            # From https://developer.overdrive.com/apis/search:
            # "**Note: When you search using the lastTitleUpdateTime or
            # lastUpdateTime parameters, your results will be automatically
            # sorted in ascending order (and all other sort options will be ignored)."
            lastupdatetime=last_update,
            limit=str(min(page_size, self.PAGE_SIZE_LIMIT)),
            collection_token=self.collection_token,
        )
        endpoint: str = _make_link_safe(book_info_initial_endpoint)

        return BookInfoEndpoint(endpoint)

    async def fetch_book_info_list(
        self,
        endpoint: BookInfoEndpoint,
        rel_to_follow: str = "next",
        fetch_metadata: bool = False,
        fetch_availability: bool = False,
        connections: int = 5,
        extractor_class: type[OverdriveRepresentationExtractor] | None = None,
    ) -> tuple[list[dict[str, Any]], BookInfoEndpoint | None]:
        """
        This method is used to fetch a "page" of book data. Users can optionally fetch metadata and availability info
        by using the fetch_metadata and fetch_availability parameters.  Internally an async http client is used to
        parallelize the retrieval of the metadata and availability.  A list of book data is returned which can be
        parsed or converted according to the needs of the client.  Additionally, we return the link to the next page
        of book data. In this way, "page" retrievals are accelerated while allowing the client to retrieve chunks
        in a deterministic and therefore retriable manner.
        """
        base_url = self.endpoint(self.HOST_ENDPOINT_BASE)
        async with self.create_async_client(
            connections=connections, base_url=base_url
        ) as client:
            urls: deque[str] = deque()
            books: dict[str, Any] = {}
            extractor_class = extractor_class or OverdriveRepresentationExtractor
            urls.append(endpoint.url)
            req = client.get(endpoint.url)
            response = await req
            data = response.json()
            next_url = extractor_class.link(data, rel_to_follow)
            next_endpoint: BookInfoEndpoint | None = (
                BookInfoEndpoint(next_url) if next_url else None
            )
            async_task_list = list()
            response_products = data["products"]
            for product in response_products:
                identifier = product["id"].lower()
                books[identifier] = product
                if fetch_metadata:
                    async_task_list.append(
                        self._get_metadata_async(base_url, product, client)
                    )

                if fetch_availability:
                    async_task_list.append(
                        self._get_availability_async(
                            base_url,
                            product,
                            client,
                        )
                    )

            await asyncio.gather(*async_task_list)

            return list(books.values()), next_endpoint

    async def _get_availability_async(
        self, base_url: str, book_info: dict[str, Any], client: AsyncClient
    ) -> None:
        url = book_info["links"]["availabilityV2"]["href"].removeprefix(base_url)
        data = await self._get_product_relation(client, url)
        if data:
            book_info["availabilityV2"] = data

    async def _get_metadata_async(
        self, base_url: str, book_info: dict[str, Any], client: AsyncClient
    ) -> None:
        url = book_info["links"]["metadata"]["href"].removeprefix(base_url)
        data = await self._get_product_relation(client, url)
        if data:
            book_info["metadata"] = data

    async def _get_product_relation(
        self, client: AsyncClient, url: str
    ) -> dict[str, Any] | None:
        req = client.get(url)
        response = await req
        # We allow a 404 response code for availability or metadata since those links may not exist for a given
        # identifier.
        if response.status_code == 404:
            return None
        else:
            data: dict[str, Any] = response.json()
            return data

    def create_async_client(
        self,
        base_url: str,
        connections: int = 5,
    ) -> AsyncClient:
        return AsyncClient.for_worker(
            base_url=base_url,
            headers=self._get_headers(self._client_oauth_token),
            timeout=Timeout(20.0, pool=None),
            allowed_response_codes=[200, 404],
            limits=Limits(
                max_connections=connections,
                max_keepalive_connections=connections,
                keepalive_expiry=5,
            ),
            backoff=WORKER_DEFAULT_BACKOFF,
        )

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
        for default_patrons_result in self.default_patrons(collection):
            if isinstance(default_patrons_result, SelfTestResult):
                yield default_patrons_result
                continue
            library, patron, pin = default_patrons_result
            task = (
                "Checking Patron Authentication privileges, using test patron for library %s"
                % library.name
            )
            yield self.run_test(task, self._get_patron_oauth_credential, patron, pin)

    @overload
    def patron_request(
        self,
        patron: Patron,
        pin: str | None,
        url: str,
        extra_headers: dict[str, str] | None = ...,
        data: str | None = ...,
        method: str | None = ...,
        response_type: None = ...,
        exception_on_401: bool = ...,
    ) -> Response: ...

    @overload
    def patron_request[TOverdriveModel: BaseOverdriveModel](
        self,
        patron: Patron,
        pin: str | None,
        url: str,
        extra_headers: dict[str, str] | None = ...,
        data: str | None = ...,
        method: str | None = ...,
        response_type: type[TOverdriveModel] = ...,
        exception_on_401: bool = ...,
    ) -> TOverdriveModel: ...

    def patron_request[TOverdriveModel: BaseOverdriveModel](
        self,
        patron: Patron,
        pin: str | None,
        url: str,
        extra_headers: dict[str, str] | None = None,
        data: str | None = None,
        method: str | None = None,
        response_type: type[TOverdriveModel] | None = None,
        exception_on_401: bool = False,
    ) -> Response | TOverdriveModel:
        """
        Make an HTTP request on behalf of a patron to Overdrive's API.

        This request will be made using an OAuth bearer token for the
        patron that was acquired using the privileged Palace credentials
        so that the patron can take actions that require extra api
        permissions.
        """
        patron_credential = self._get_patron_oauth_credential(patron, pin)
        assert patron_credential.credential
        headers = self._get_headers(patron_credential.credential)
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
        try:
            response = self._do_patron_request(
                method,
                url,
                headers=headers,
                data=data,
                allowed_response_codes=["2xx", 401],
            )
        except BadResponseException as e:
            ErrorResponse.raise_from_response(e.response, e.message)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise IntegrationException(
                    "Something's wrong with the patron OAuth Bearer Token!"
                )
            else:
                # Refresh the token and try again.
                self._refresh_patron_oauth_token(patron_credential, patron, pin)
                return self.patron_request(
                    patron,
                    pin,
                    url,
                    extra_headers=extra_headers,
                    data=data,
                    method=method,
                    exception_on_401=True,
                )

        if response_type is None:
            return response
        else:
            try:
                return response_type.model_validate_json(response.content)
            except ValidationError as e:
                # We were unable to validate the response as the expected type. Log some relevant details and
                # raise a BadResponseException.
                self.log.exception(
                    "Unable to validate Overdrive response. %s",
                    str(e),
                )
                raise OverdriveValidationError(
                    response.url,
                    "Error validating Overdrive response",
                    response,
                    debug_message=str(e),
                ) from e

    def _do_patron_request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        """This method is overridden in MockOverdriveAPI."""
        url = self.endpoint(url)
        return HTTP.request_with_timeout(
            http_method,
            url,
            **kwargs,
        )

    def _get_patron_oauth_credential(
        self, patron: Patron, pin: str | None
    ) -> Credential:
        """Get an Overdrive OAuth token for the given patron.

        See: https://developer.overdrive.com/apis/patron-auth

        :param patron: The patron for whom to fetch the credential.
        :param pin: The patron's PIN or password.
        """

        refresh = partial(
            self._refresh_patron_oauth_token,
            patron=patron,
            pin=pin,
        )

        return Credential.lookup(
            self._db,
            DataSource.OVERDRIVE,
            "Palace Context Patron OAuth Token",
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
                {"Authorization": self._palace_context_basic_auth_header},
                allowed_response_codes=["2xx"],
            )
        except BadResponseException as e:
            error = ErrorResponse.from_response(e.response)
            error_code = error.error_code if error and error.error_code else "Unknown"
            description = (
                error.message
                if error and error.message
                else "Failed to authenticate with Overdrive"
            )
            debug_message = (
                f"_refresh_patron_oauth_token failed. Status code: '{e.response.status_code}'. "
                f"Error: '{error_code}'. Description: '{description}'."
            )
            self.log.info(debug_message + f" Response: '{e.response.text}'")
            raise PatronAuthorizationFailedException(description, debug_message) from e

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

        already_checked_out = False
        try:
            make_request: PatronRequestCallable[Checkout] = partial(
                self.patron_request, patron, pin, response_type=Checkout
            )
            checkout = _overdrive_field_request(
                make_request, self.CHECKOUTS_ENDPOINT, {"reserveId": overdrive_id}
            )
        except OverdriveResponseException as e:
            code = e.error_message
            raise CannotLoan(code) from e
        except (AlreadyCheckedOut, NoAvailableCopies) as e:
            # The client should have used a fulfill link instead, but we'll handle this case.
            # When a book has holds, the Overdrive API returns NoAvailableCopies error even if
            # the patron already has an active loan. To address this, we check if the patron
            # already has a loan for this title - if so, we return that loan instead of raising
            # an exception.
            #
            # NOTE: This scenario is rare but possible, typically occurring when a patron borrows
            # a book through Libby and then immediately attempts to borrow the same title through
            # Palace.
            try:
                checkout = self.get_loan(patron, pin, identifier.identifier)
                already_checked_out = True
            except NoActiveLoan:
                # Reraise the original exception.
                self.log.info(f"No active loan found. Raising {e.__class__.__name__}.")
                raise e from None

        # At this point we know all available formats for this book.
        # For Overdrive ebooks, this may be our first complete view of format availability.
        if "ebook-overdrive" in checkout.available_formats:
            self._set_licensepool_delivery_mechanism_availability(licensepool, checkout)

            # Handle books that are not available in any formats that the mobile
            # apps can read.
            if not checkout.available_formats & OVERDRIVE_LOCK_IN_FORMATS:
                title = (
                    licensepool.presentation_edition.title
                    if licensepool.presentation_edition
                    else "<unknown>"
                )
                author = (
                    licensepool.presentation_edition.author
                    if licensepool.presentation_edition
                    else "<unknown>"
                )
                self.log.error(
                    f"Patron checked out a book that is not available in a supported format. "
                    f"Overdrive ID: '{checkout.reserve_id}' Title: '{title}' Author: '{author}'"
                )

                existing_hold = get_one(
                    self._db,
                    Hold,
                    patron=patron,
                    license_pool=licensepool,
                    on_multiple="interchangeable",
                )

                # Only do an early return if this is a fresh checkout and the patron isn't
                # converting a hold to a checkout.
                do_early_return = not already_checked_out and existing_hold is None

                if do_early_return:
                    make_request = partial(self.patron_request, patron, pin)
                    checkout.action("early_return", make_request)

                # If this was a hold, we remove the hold record from the database before
                # we raise the exception, since the hold has been converted to a checkout.
                if existing_hold:
                    existing_hold.collect_event_and_delete()

                msg = "The format of this book is not supported by the Palace app."

                if not do_early_return:
                    msg += " The book can only be accessed in your OverDrive/Libby app account."

                raise CannotLoan(msg)

        # Create the loan info.
        return LoanInfo.from_license_pool(
            licensepool,
            end_date=checkout.expires,
        )

    def _set_licensepool_delivery_mechanism_availability(
        self, licensepool: LicensePool, checkout: Checkout
    ) -> None:
        for delivery_mechanism in licensepool.delivery_mechanisms:
            try:
                internal_format = self.internal_format(delivery_mechanism)
            except DeliveryMechanismError:
                # This shouldn't happen, but if it does we just log the error
                # and move on.
                self.log.error(
                    f"Could not find internal format for delivery mechanism {delivery_mechanism!r}"
                )
                continue

            if internal_format in checkout.available_formats:
                delivery_mechanism.available = True
            else:
                delivery_mechanism.available = False

    def checkin(
        self, patron: Patron, pin: str | None, licensepool: LicensePool
    ) -> None:
        # First we get the loan for this patron.
        try:
            loan = self.get_loan(patron, pin, licensepool.identifier.identifier)
            make_request = partial(self.patron_request, patron, pin)
            loan.action("early_return", make_request)
        except NoActiveLoan:
            # The loan is already gone, no need to return it. This exception gets
            # handled higher up the stack.
            raise NotCheckedOut()
        except OverdriveModelError as e:
            # Something went wrong following the link in the response from Overdrive,
            # or we could not find the link.
            # We log the error, and treat this loan like it was returned. If it wasn't
            # it may come back via patron sync.
            self.log.exception(
                f"Something went wrong calling the earlyReturn action. {e}"
            )
        except OverdriveResponseException as e:
            raise CannotReturn(e.error_message) from e

    def get_loan(self, patron: Patron, pin: str | None, overdrive_id: str) -> Checkout:
        """Get patron's loan information for the identified item.

        :param patron: A patron.
        :param pin: An optional PIN/password for the patron.
        :param overdrive_id: The OverDrive identifier for an item.
        :return: Information about the loan.
        """
        url = f"{self.CHECKOUTS_ENDPOINT}/{overdrive_id.upper()}"
        return self.patron_request(patron, pin, url, response_type=Checkout)

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> Fulfillment:
        """Get the actual resource file to the patron."""
        internal_format = self.internal_format(delivery_mechanism)
        format_info = self._get_fulfill_format_information(
            patron, pin, licensepool.identifier.identifier, internal_format
        )

        if internal_format in OVERDRIVE_PALACE_MANIFEST_FORMATS:
            return self._manifest_fulfillment(patron, pin, format_info)
        else:
            return self._contentlink_fulfillment(
                patron, pin, internal_format, format_info
            )

    def _contentlink_fulfillment(
        self, patron: Patron, pin: str | None, internal_format: str, format_info: Format
    ) -> Fulfillment:
        # If this for Overdrive's streaming reader, and the link expires,
        # the patron can go back to the circulation manager fulfill url
        # again to get a new one.
        if flask.request:
            fulfill_url = flask.request.url
        else:
            fulfill_url = ""
        download_link = format_info.template_link(
            "downloadLink",
            errorpageurl=self.DEFAULT_ERROR_URL,
            odreadauthurl=fulfill_url,
        )
        download_response = self.patron_request(
            patron, pin, download_link, response_type=Format
        )
        result = download_response.links["contentlink"]
        url = result.href
        media_type = result.type
        # TODO: we should return a different type of fulfillment for streaming formats
        #   so we don't have to override this in the circulation API later.
        if internal_format in OVERDRIVE_STREAMING_FORMATS:
            media_type += DeliveryMechanism.STREAMING_PROFILE
        # In case we are a non-drm asset, we should just redirect the client to the asset directly
        if internal_format in OVERDRIVE_OPEN_FORMATS:
            return RedirectFulfillment(
                content_link=url,
                content_type=media_type,
            )
        else:
            return FetchFulfillment(
                content_link=url,
                content_type=media_type,
            )

    def _manifest_fulfillment(
        self, patron: Patron, pin: str | None, format_info: Format
    ) -> OverdriveManifestFulfillment:
        download_link = format_info.link_templates["downloadLink"].href
        download_link_split = urlsplit(download_link)
        download_link_split = download_link_split._replace(query="contentfile=true")
        download_link = download_link_split.geturl()
        # The client must authenticate using its own
        # credentials to fulfill this URL; we can't do it.
        scope_string = self.scope_string(patron.library)
        fulfillment_access_token = self._get_patron_oauth_credential(
            patron,
            pin,
        ).credential
        # The credential should never be None, but mypy doesn't know that, so
        # we assert to be safe.
        assert fulfillment_access_token is not None
        return OverdriveManifestFulfillment(
            download_link,
            scope_string,
            fulfillment_access_token,
        )

    def _get_fulfill_format_information(
        self, patron: Patron, pin: str | None, overdrive_id: str, format_type: str
    ) -> Format:
        try:
            loan = self.get_loan(patron, pin, overdrive_id)
        except PatronAuthorizationFailedException as e:
            message = f"Error authenticating patron for fulfillment: {e.args[0]}"
            raise CannotFulfill(message, *e.args[1:]) from e

        if not loan.locked_in and format_type in OVERDRIVE_LOCK_IN_FORMATS:
            # The format is not locked in. Lock it in.
            # This will happen the first time someone tries to fulfill
            # a loan with a lock-in format.
            return self._lock_in_format(patron, pin, format_type, loan)

        format_info = loan.get_format(format_type)
        if format_info is None:
            available_formats = loan.available_formats
            if OVERDRIVE_INCOMPATIBLE_FORMATS & available_formats:
                # The most likely explanation is that the patron
                # already had this book delivered to their Kindle.
                raise FulfilledOnIncompatiblePlatform(
                    "It looks like this loan was already fulfilled on another platform, most likely "
                    "Amazon Kindle. We're not allowed to also send it to you as an EPUB."
                )
            else:
                raise NoAcceptableFormat(
                    f"Could not find specified format {format_type}. "
                    f"Available formats: {', '.join(available_formats)}"
                )

        return format_info

    def _lock_in_format(
        self, patron: Patron, pin: str | None, format_type: str, loan: Checkout
    ) -> Format:
        make_request: PatronRequestCallable[Format] = partial(
            self.patron_request, patron, pin, response_type=Format
        )
        try:
            format_data = loan.action("format", make_request, format_type=format_type)
        except InvalidFieldOptionError:
            raise FormatNotAvailable(
                "This book is not available in the format you requested."
            )
        except (OverdriveModelError, OverdriveResponseException) as e:
            self.log.exception(
                f"Error locking in loan. Overdrive ID: {loan.reserve_id}, format: {format_type}",
            )
            raise CannotFulfill(f"Could not lock in format {format_type}") from e
        return format_data

    def get_patron_checkouts(self, patron: Patron, pin: str | None) -> Checkouts:
        """Get information for the given patron's loans.

        :param patron: A patron.
        :param pin: An optional PIN/password for the patron.
        :return: Information about the patron's loans.
        """
        return self.patron_request(
            patron,
            pin,
            self.CHECKOUTS_ENDPOINT,
            response_type=Checkouts,
        )

    def get_patron_holds(self, patron: Patron, pin: str | None) -> HoldsResponse:
        return self.patron_request(
            patron, pin, self.HOLDS_ENDPOINT, response_type=HoldsResponse
        )

    def patron_activity(
        self, patron: Patron, pin: str | None
    ) -> Iterable[LoanInfo | HoldInfo]:
        collection = self.collection

        try:
            checkouts = self.get_patron_checkouts(patron, pin).checkouts
            holds = self.get_patron_holds(patron, pin).holds
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
            checkouts = []
            holds = []

        for checkout in checkouts:
            loan_info = self.process_checkout_data(checkout, collection.id)
            if loan_info is not None:
                yield loan_info

        for hold in holds:
            overdrive_identifier = hold.reserve_id.lower()
            start = hold.hold_placed_date
            end = hold.hold_expires
            position = hold.hold_list_position
            if "checkout" in hold.actions:
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
        cls, checkout: Checkout, collection_id: int
    ) -> LoanInfo | None:
        """Convert one checkout from Overdrive's list of checkouts
        into a LoanInfo object.

        :return: A LoanInfo object if the book can be fulfilled
            by the default Library Simplified client, and None otherwise.
        """
        overdrive_identifier = checkout.reserve_id.lower()
        start = checkout.checkout_date
        end = checkout.expires

        usable_formats = checkout.available_formats & OVERDRIVE_FORMATS

        if (
            not usable_formats
            or checkout.locked_in
            and not usable_formats & OVERDRIVE_LOCK_IN_FORMATS
        ):
            # Either this book is not available in any format readable
            # by the default client, or the patron previously chose to
            # fulfill it in a format not readable by the default
            # client. Either way, we cannot fulfill this loan, and we
            # shouldn't show it in the list.
            return None

        locked_to = None
        if checkout.locked_in:
            locked_formats = list(usable_formats & OVERDRIVE_LOCK_IN_FORMATS)
            if len(locked_formats) == 1:
                [locked_format] = locked_formats
                if locked_format in cls.internal_format_to_delivery_mechanism:
                    content_type, drm_scheme = (
                        cls.internal_format_to_delivery_mechanism[locked_format]
                    )
                    locked_to = FormatData(
                        content_type=content_type,
                        drm_scheme=drm_scheme,
                        rights_uri=RightsStatus.IN_COPYRIGHT,
                        available=True,
                        update_available=True,
                    )

        available_formats = None
        if "ebook-overdrive" in usable_formats:
            # Overdrive ebooks don't give us useful format information until we have a loan, so
            # we take this opportunity to get information about what formats are available. And
            # include this with the LoanInfo, so that the licensepool can be updated with more
            # accurate format information.

            # Get all the formats that we create by default for an overdrive ebook. We start them
            # all with their availability set to False, then update the availability of the formats that
            # we now know are available.
            internal_formats = {}
            for format_data in OverdriveRepresentationExtractor.internal_formats(
                "ebook-overdrive"
            ):
                internal_formats[(format_data.content_type, format_data.drm_scheme)] = (
                    False
                )

            for overdrive_format in usable_formats:
                if overdrive_format in cls.internal_format_to_delivery_mechanism:
                    content_type, drm_scheme = (
                        cls.internal_format_to_delivery_mechanism[overdrive_format]
                    )
                    internal_formats[(content_type, drm_scheme)] = True

            available_formats = {
                FormatData(
                    content_type=content_type,
                    drm_scheme=drm_scheme,
                    rights_uri=RightsStatus.IN_COPYRIGHT,
                    available=available,
                    update_available=True,
                )
                for (content_type, drm_scheme), available in internal_formats.items()
            }

        return LoanInfo(
            collection_id=collection_id,
            identifier_type=Identifier.OVERDRIVE_ID,
            identifier=overdrive_identifier,
            start_date=start,
            end_date=end,
            locked_to=locked_to,
            available_formats=available_formats,
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
        try:
            patron_information = self.patron_request(
                patron,
                pin,
                self.PATRON_INFORMATION_ENDPOINT,
                response_type=PatronInformation,
            )
            address = patron_information.last_hold_email

            # Great! Except, it's possible that this address is the
            # 'trash everything' address, because we _used_ to send
            # that address to Overdrive. If so, ignore it.
            if address == trash_everything_address:
                address = None
        except OverdriveResponseException as e:
            self.log.exception(
                "Unable to get patron information for %s: %s",
                patron.authorization_identifier,
                e.response.text,
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

        try:
            make_request: PatronRequestCallable[HoldResponse] = partial(
                self.patron_request, patron, pin, response_type=HoldResponse
            )
            hold = _overdrive_field_request(
                make_request,
                self.HOLDS_ENDPOINT,
                form_fields,
            )
        except OverdriveResponseException as e:
            raise CannotHold(e.error_code) from e
        return HoldInfo.from_license_pool(
            licensepool,
            start_date=hold.hold_placed_date,
            hold_position=hold.hold_list_position,
        )

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
        try:
            self.patron_request(patron, pin, url, method="DELETE")
        except OverdriveResponseException as e:
            response = e.response
            if (
                response.status_code == 404
                or e.error_code == "PatronDoesntHaveTitleOnHold"
            ):
                # Hold not found, this is fine
                return
            raise CannotReleaseHold(e.error_code, debug_info=response.text) from e

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

        bibliographic = OverdriveRepresentationExtractor.book_info_to_bibliographic(
            info, include_bibliographic=True, include_formats=True
        )
        if not bibliographic:
            # No work to be done.
            return

        edition, ignore = self._edition(licensepool)

        replace = ReplacementPolicy.from_license_source(self._db)
        bibliographic.apply(self._db, edition, self.collection, replace=replace)

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
            cast(str, book_id),
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
            self.data_source,
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
