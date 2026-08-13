"""HTTP transport layer for the Overdrive integration.

This module separates the mechanics of talking to the Overdrive API --
host selection, URL templating, OAuth token management, request execution,
and 401 retry behavior -- from the business logic in
:mod:`palace.manager.integration.license.overdrive.api`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from threading import Lock
from typing import Any, Protocol, Unpack, overload

from frozendict import frozendict
from pydantic import ValidationError
from requests import Response
from requests.structures import CaseInsensitiveDict

from palace.util.log import LoggerMixin

from palace.manager.api.circulation.exceptions import (
    CannotFulfill,
    PatronAuthorizationFailedException,
)
from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.core.config import CannotLoadConfiguration, Configuration
from palace.manager.core.exceptions import IntegrationException
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.exception import (
    OverdriveValidationError,
)
from palace.manager.integration.license.overdrive.model import (
    BaseOverdriveModel,
    Checkout,
    Checkouts,
    ErrorResponse,
    Format,
    Hold,
    Holds,
    PatronInformation,
    RequestSpec,
    build_field_request,
)
from palace.manager.integration.license.overdrive.settings import OverdriveSettings
from palace.manager.integration.license.overdrive.util import _make_link_safe
from palace.manager.util import base64
from palace.manager.util.http.async_http import WORKER_DEFAULT_BACKOFF, AsyncClient
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.http.http import HTTP, RequestKwargs


class PatronTokenProvider(Protocol):
    """Supplies the bearer token for a patron-context request.

    The token itself is persisted in the database by the API layer, so the
    request layer reaches it through this callable rather than owning it.

    Calling with ``force_refresh`` must return a newly issued token rather
    than the one already in hand, since that is the only way a request that
    has been refused gets a usable one.
    """

    def __call__(self, *, force_refresh: bool = False) -> str: ...


# The host portion of every client-context URL template. Module level because
# OverdriveClientRequests builds its templates in its own class body, where a
# class attribute inherited from the base would not be visible.
HOST_ENDPOINT_BASE = "%(host)s"


@dataclass
class BookInfoEndpoint:
    url: str


class BaseOverdriveRequests(LoggerMixin):
    """Shared host and URL-template machinery for the Overdrive request classes."""

    # Production and testing have different host names for some of the
    # API endpoints. This is configurable on the collection level.
    # Production and testing setups use the same URLs for Client
    # Authentication and Patron Authentication, but we use the same
    # system as for other hostnames to give a consistent look to the
    # templates.
    HOSTS = frozendict(
        {
            OverdriveConstants.PRODUCTION_SERVERS: frozendict(
                {
                    "host": "https://api.overdrive.com",
                    "patron_host": "https://patron.api.overdrive.com",
                    "oauth_patron_host": "https://oauth-patron.overdrive.com",
                    "oauth_host": "https://oauth.overdrive.com",
                }
            ),
            OverdriveConstants.TESTING_SERVERS: frozendict(
                {
                    "host": "https://integration.api.overdrive.com",
                    "patron_host": "https://integration-patron.api.overdrive.com",
                    "oauth_patron_host": "https://oauth-patron.overdrive.com",
                    "oauth_host": "https://oauth.overdrive.com",
                }
            ),
        }
    )

    # Overdrive can be slow to answer, particularly on the token endpoints, so
    # we allow far more than the global default before giving up.
    REQUEST_TIMEOUT = 120

    def __init__(self, settings: OverdriveSettings) -> None:
        self._server_nickname = settings.overdrive_server_nickname
        self._hosts = self._determine_hosts(server_nickname=self._server_nickname)
        self._max_retry_count = settings.max_retry_count

    @classmethod
    def _determine_hosts(cls, *, server_nickname: str) -> dict[str, str]:
        # Figure out which hostnames we'll be using when constructing
        # endpoint URLs. An unrecognized nickname falls back to the
        # production hosts.
        if server_nickname not in cls.HOSTS:
            server_nickname = OverdriveConstants.PRODUCTION_SERVERS

        return dict(cls.HOSTS[server_nickname])

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

    def _do_get(self, url: str, headers: dict[str, str], **kwargs: Any) -> Response:
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self._max_retry_count
        kwargs["timeout"] = self.REQUEST_TIMEOUT
        return HTTP.get_with_timeout(url, headers=headers, **kwargs)

    def _do_post(
        self, url: str, payload: dict[str, str], headers: dict[str, str], **kwargs: Any
    ) -> Response:
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self._max_retry_count
        kwargs["timeout"] = self.REQUEST_TIMEOUT
        return HTTP.post_with_timeout(url, data=payload, headers=headers, **kwargs)


class OverdriveClientRequests(BaseOverdriveRequests):
    """The Overdrive "Client Authentication" request context.

    Uses the collection-configured client key/secret to acquire a bearer
    token and issue collection-scoped requests (library document, book
    lists, metadata, availability).

    See: https://developer.overdrive.com/docs/api-security
         https://developer.overdrive.com/apis/client-auth
    """

    # Each of these endpoint URLs has a slot to plug in one of the
    # appropriate servers. This will be filled in either by a call to
    # the endpoint() method (if there are other variables in the
    # template), or by the request methods (if there are no other
    # variables).
    TOKEN_ENDPOINT = "%(oauth_host)s/token"

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

    def __init__(
        self,
        settings: OverdriveSettings,
        *,
        parent_library_id: str | None = None,
    ) -> None:
        super().__init__(settings)

        if not settings.external_account_id:
            raise CannotLoadConfiguration("Overdrive library ID is not configured")
        if not settings.overdrive_client_key:
            raise CannotLoadConfiguration("Overdrive client key is not configured")
        if not settings.overdrive_client_secret:
            raise CannotLoadConfiguration(
                "Overdrive client password/secret is not configured"
            )

        self._library_id = settings.external_account_id
        self._parent_library_id = parent_library_id
        self._client_key = settings.overdrive_client_key
        self._client_secret = settings.overdrive_client_secret

        # This is set by access to ._client_oauth_token
        self._cached_token: OAuthTokenResponse | None = None
        self._lock = Lock()

    @property
    def library_endpoint_url(self) -> str:
        """Which URL should we go to to get information about this collection?

        If this is an ordinary Overdrive account, we get information
        from LIBRARY_ENDPOINT.

        If this is an Overdrive Advantage account, we get information
        from ADVANTAGE_LIBRARY_ENDPOINT.
        """
        args = dict(library_id=self._library_id)
        if self._parent_library_id:
            # This is an Overdrive advantage account.
            args["parent_library_id"] = self._parent_library_id
            endpoint = self.ADVANTAGE_LIBRARY_ENDPOINT
        else:
            endpoint = self.LIBRARY_ENDPOINT
        return self.endpoint(endpoint, **args)

    @property
    def _collection_context_basic_auth_header(self) -> str:
        """
        Returns the Basic Auth header used to acquire an OAuth bearer token.

        This header contains the collection's credentials that were configured
        through the admin interface for this specific collection.
        """
        credentials = f"{self._client_key}:{self._client_secret}"
        return "Basic " + base64.standard_b64encode(credentials).strip()

    @staticmethod
    def _auth_headers(auth_token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {auth_token}"}

    def auth_headers(self) -> dict[str, str]:
        """The Authorization header for a client-context request.

        Refreshes the cached bearer token if needed.
        """
        return self._auth_headers(self._client_oauth_token)

    @property
    def _client_oauth_token(self) -> str:
        """
        The client oauth bearer token used for authentication with
        Overdrive for this collection.

        This token is refreshed as needed and cached for reuse
        by this property.
        """
        if (token := self._cached_token) is not None and not token.expired:
            return token.access_token

        return self.refresh_client_oauth_token().access_token

    def refresh_client_oauth_token(self) -> OAuthTokenResponse:
        """Fetch a fresh client credentials bearer token and cache it."""
        with self._lock:
            response = self._do_post(
                self.TOKEN_ENDPOINT,
                dict(grant_type="client_credentials"),
                {"Authorization": self._collection_context_basic_auth_header},
                allowed_response_codes=[200],
            )
            try:
                token = OAuthTokenResponse.model_validate_json(response.content)
            except ValidationError as e:
                # Overdrive accepted the credentials but sent back something
                # we can't use as a token. Raise it as an Overdrive error
                # rather than letting a bare ValidationError escape.
                self.log.exception(
                    "Unable to validate Overdrive token response. %s", str(e)
                )
                raise OverdriveValidationError(
                    response.url,
                    "Error validating Overdrive token response",
                    response,
                    debug_message=str(e),
                ) from e
            self._cached_token = token
            return token

    def raw_get(
        self,
        url: str,
        extra_headers: dict[str, str] | None = None,
        exception_on_401: bool = False,
    ) -> tuple[int, CaseInsensitiveDict[str], bytes]:
        """Make an HTTP GET request using the active Bearer Token.

        Returns the raw ``(status code, headers, content)`` tuple. This shape
        is do_get-compatible, so this method can be passed to
        ``Representation.get`` as its fetch callable.

        A 401 response triggers a single token refresh and retry; a second
        401 raises :class:`BadResponseException`. A 404 is returned to the
        caller rather than raised.
        """
        request_headers = self._auth_headers(self._client_oauth_token)
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
                self.refresh_client_oauth_token()
                return self.raw_get(url, extra_headers, True)
        else:
            return status_code, headers, content


class OverdriveAsyncRequests(BaseOverdriveRequests):
    """The client-context requests made concurrently by import workers.

    Separate from :class:`OverdriveClientRequests` because it shares none of
    that class's request machinery: it runs on httpx rather than requests,
    with its own retry and backoff policy, and holds one client open across a
    whole page of products. It does share that class's bearer token, which it
    reaches through the object it is given, so a collection still only ever
    holds one.
    """

    NEXT_REL = "next"

    def __init__(
        self,
        settings: OverdriveSettings,
        client_requests: OverdriveClientRequests,
    ) -> None:
        super().__init__(settings)
        self._client_requests = client_requests

    @staticmethod
    def _page_link(page: dict[str, Any], rel: str) -> str | None:
        """The href of the given link relation on a book list page, if present."""
        if "links" in page and rel in page["links"]:
            return _make_link_safe(page["links"][rel]["href"])
        return None

    async def fetch_book_info_list(
        self,
        endpoint: BookInfoEndpoint,
        fetch_metadata: bool = False,
        fetch_availability: bool = False,
    ) -> tuple[list[dict[str, Any]], BookInfoEndpoint | None]:
        """
        This method is used to fetch a "page" of book data. Users can optionally fetch metadata and availability info
        by using the fetch_metadata and fetch_availability parameters. Internally, an async http client is used to
        parallelize the retrieval of the metadata and availability.  A list of book data is returned which can be
        parsed or converted according to the needs of the client.  Additionally, we return the link to the next page
        of book data. In this way, "page" retrievals are accelerated while allowing the client to retrieve chunks
        in a deterministic and therefore retriable manner.
        """
        base_url = self.endpoint(HOST_ENDPOINT_BASE)
        async with self._create_configured_async_client(base_url=base_url) as client:
            books: dict[str, Any] = {}
            req = client.get(endpoint.url)
            response = await req
            data = response.json()
            next_url = self._page_link(data, self.NEXT_REL)
            next_endpoint: BookInfoEndpoint | None = (
                BookInfoEndpoint(next_url) if next_url else None
            )
            async_task_list = list()
            response_products = data.get("products")
            if response_products is None:
                # Overdrive omits the 'products' key entirely when a collection
                # (or page) contains no titles. In that case 'totalItems' is 0
                # and there is simply nothing to import, so we treat it as an
                # empty page rather than an error.
                if data.get("totalItems") == 0:
                    return [], next_endpoint

                self.log.warning(
                    f"Overdrive response missing 'products' key for endpoint {endpoint.url}.",
                    extra={
                        "palace_response_data": data,
                        "palace_response_status_code": response.status_code,
                    },
                )
                raise BadResponseException(
                    endpoint.url,
                    f"Overdrive response missing 'products' key. Response data: {data}",
                    response,
                )
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
            self.log.warning(
                f"The following URL unexpectedly returned a 404: {url}. "
                f'Response text: "{response.text}" -> Skipping...'
            )
            return None
        else:
            data: dict[str, Any] = response.json()
            return data

    def _create_configured_async_client(
        self,
        base_url: str,
    ) -> AsyncClient:
        return AsyncClient.for_worker(
            base_url=base_url,
            headers=self._client_requests.auth_headers(),
            allowed_response_codes=[200, 404],
            backoff=WORKER_DEFAULT_BACKOFF,
        )


class OverdrivePatronRequests(BaseOverdriveRequests):
    """The Overdrive "Patron Authentication" request context.

    Acts on behalf of a specific patron, using a bearer token acquired with
    the privileged Palace Project credentials so the patron can take actions
    that require extra API permissions.

    See: https://developer.overdrive.com/apis/patron-auth
    """

    PATRON_TOKEN_ENDPOINT = "%(oauth_patron_host)s/patrontoken"

    PATRON_INFORMATION_ENDPOINT = "%(patron_host)s/v1/patrons/me"
    CHECKOUTS_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts"
    CHECKOUT_ENDPOINT = "%(patron_host)s/v1/patrons/me/checkouts/%(overdrive_id)s"
    HOLDS_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds"
    HOLD_ENDPOINT = "%(patron_host)s/v1/patrons/me/holds/%(product_id)s"

    @property
    def _palace_context_basic_auth_header(self) -> str:
        """
        Returns the Basic Auth header used to acquire an OAuth bearer token.

        This header contains the Palace Project credentials passed into the
        Circulation Manager via environment variables. This is used to acquire
        a privileged token that has extra permissions for the Overdrive API.
        """
        is_test_mode = self._server_nickname == OverdriveConstants.TESTING_SERVERS
        try:
            client_credentials = Configuration.overdrive_fulfillment_keys(
                testing=is_test_mode
            )
        except CannotLoadConfiguration as e:
            raise CannotFulfill() from e

        credentials = f"{client_credentials['key']}:{client_credentials['secret']}"
        return "Basic " + base64.standard_b64encode(credentials).strip()

    def _do_request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        # Unlike the token endpoints, patron API calls stay on the global HTTP
        # timeout and retry defaults. A patron is waiting on these, so failing
        # fast beats holding the request open for the token endpoint's two
        # minutes.
        url = self.endpoint(url)
        return HTTP.request_with_timeout(
            http_method,
            url,
            **kwargs,
        )

    def refresh_patron_oauth_token(
        self,
        *,
        username: str | None,
        password: str | None,
        scope: str,
    ) -> OAuthTokenResponse:
        """Request an OAuth bearer token that allows us to act on
        behalf of a specific patron.

        :param username: The patron's authorization identifier.
        :param password: The patron's PIN or password, if one was provided.
        :param scope: The Overdrive scope string for the patron's library.

        :raises PatronAuthorizationFailedException: If Overdrive refuses to
            issue a token.
        """
        payload = dict(
            grant_type="password",
            scope=scope,
        )
        if username:
            payload["username"] = username
        if password:
            # A PIN was provided.
            payload["password"] = password
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
            error = ErrorResponse.from_response_data(e.response)
            error_code = error.error_code if error and error.error_code else "Unknown"
            description = (
                error.message
                if error and error.message
                else "Failed to authenticate with Overdrive"
            )
            debug_message = (
                f"refresh_patron_oauth_token failed. Status code: '{e.response.status_code}'. "
                f"Error: '{error_code}'. Description: '{description}'."
            )
            self.log.info(debug_message + f" Response: '{e.response.text}'")
            raise PatronAuthorizationFailedException(description, debug_message) from e

        try:
            return OAuthTokenResponse.model_validate_json(response.content)
        except ValidationError as e:
            # Overdrive accepted the credentials but sent back something we
            # can't use as a token. Surface it as an authorization failure so
            # it stays inside the circulation error path.
            #
            # The body is a 2xx token document, so it carries a live bearer
            # token: report the validation errors without it, and without
            # pydantic's echo of the input that produced them.
            errors = e.errors(include_input=False, include_url=False)
            debug_message = (
                f"refresh_patron_oauth_token got an unusable token response. "
                f"Errors: '{errors}'."
            )
            self.log.error(debug_message)
            raise PatronAuthorizationFailedException(
                "Failed to authenticate with Overdrive", debug_message
            ) from e

    @overload
    def patron_request(
        self,
        token: PatronTokenProvider,
        request: RequestSpec,
        response_type: None = ...,
    ) -> Response: ...

    @overload
    def patron_request[TOverdriveModel: BaseOverdriveModel](
        self,
        token: PatronTokenProvider,
        request: RequestSpec,
        response_type: type[TOverdriveModel] = ...,
    ) -> TOverdriveModel: ...

    def patron_request[TOverdriveModel: BaseOverdriveModel](
        self,
        token: PatronTokenProvider,
        request: RequestSpec,
        response_type: type[TOverdriveModel] | None = None,
    ) -> Response | TOverdriveModel:
        """
        Make an HTTP request on behalf of a patron to Overdrive's API.

        A 401 response triggers a single token refresh and retry; a second
        401 raises an IntegrationException.

        :param token: Supplies (and refreshes) the patron's bearer token.
        :param request: The request to make, as described by a model or caller.
        :param response_type: If given, the model to validate the response into.
        """
        url = self.endpoint(request.url)

        bearer = token()
        for last_attempt in (False, True):
            headers = {"Authorization": f"Bearer {bearer}", **request.headers}
            try:
                response = self._do_request(
                    request.method,
                    url,
                    headers=headers,
                    data=request.data,
                    allowed_response_codes=["2xx", 401],
                )
            except BadResponseException as e:
                ErrorResponse.raise_from_response_data(e.response, e.message)
            if response.status_code != 401:
                break
            if last_attempt:
                raise IntegrationException(
                    "Something's wrong with the patron OAuth Bearer Token!"
                )
            bearer = token(force_refresh=True)

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

    def get_checkouts(self, token: PatronTokenProvider) -> Checkouts:
        """All of the patron's current loans."""
        return self.patron_request(
            token,
            RequestSpec.get(self.CHECKOUTS_ENDPOINT),
            response_type=Checkouts,
        )

    def get_checkout(self, token: PatronTokenProvider, overdrive_id: str) -> Checkout:
        """The patron's loan for a single title."""
        return self.patron_request(
            token,
            RequestSpec.get(
                self.endpoint(self.CHECKOUT_ENDPOINT, overdrive_id=overdrive_id.upper())
            ),
            response_type=Checkout,
        )

    def create_checkout(
        self, token: PatronTokenProvider, overdrive_id: str
    ) -> Checkout:
        """Check a title out to the patron."""
        return self.patron_request(
            token,
            build_field_request(self.CHECKOUTS_ENDPOINT, {"reserveId": overdrive_id}),
            response_type=Checkout,
        )

    def get_holds(self, token: PatronTokenProvider) -> Holds:
        """All of the patron's current holds."""
        return self.patron_request(
            token,
            RequestSpec.get(self.HOLDS_ENDPOINT),
            response_type=Holds,
        )

    def create_hold(
        self,
        token: PatronTokenProvider,
        overdrive_id: str,
        notification_email_address: str | None,
    ) -> Hold:
        """Place a hold on a title for the patron.

        :param notification_email_address: Where Overdrive should send the
            "hold is ready" notice. If None, Overdrive is told to send none.
        """
        fields: dict[str, str | bool | int] = {"reserveId": overdrive_id}
        if notification_email_address:
            fields["emailAddress"] = notification_email_address
        else:
            fields["ignoreHoldEmail"] = True
        return self.patron_request(
            token,
            build_field_request(self.HOLDS_ENDPOINT, fields),
            response_type=Hold,
        )

    def delete_hold(self, token: PatronTokenProvider, product_id: str) -> None:
        """Release the patron's hold on a title."""
        url = self.endpoint(self.HOLD_ENDPOINT, product_id=product_id)
        self.patron_request(token, RequestSpec("DELETE", url))

    def get_patron_information(self, token: PatronTokenProvider) -> PatronInformation:
        """Overdrive's record of the patron, including their notification email."""
        return self.patron_request(
            token,
            RequestSpec.get(self.PATRON_INFORMATION_ENDPOINT),
            response_type=PatronInformation,
        )

    def follow_download_link(self, token: PatronTokenProvider, url: str) -> Format:
        """Follow a format or download link from a loan."""
        return self.patron_request(token, RequestSpec.get(url), response_type=Format)
