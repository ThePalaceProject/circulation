"""HTTP transport layer for the Overdrive integration.

This module separates the mechanics of talking to the Overdrive API --
host selection, URL templating, OAuth token management, request execution,
and 401 retry behavior -- from the business logic in
:mod:`palace.manager.integration.license.overdrive.api`.
"""

from __future__ import annotations

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
    ErrorResponse,
)
from palace.manager.integration.license.overdrive.settings import OverdriveSettings
from palace.manager.util import base64
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.http.http import HTTP, RequestKwargs


class PatronTokenProvider(Protocol):
    """Supplies the bearer token for a patron-context request.

    The token itself is persisted in the database by the API layer, so the
    request layer reaches it through this callable rather than owning it.

    A forced refresh must be visible to later calls: the retry after a 401
    calls the provider again rather than using what the refresh returned, so
    a provider that handed back a fresh token without storing it would retry
    with the rejected one.
    """

    def __call__(self, *, force_refresh: bool = False) -> str: ...


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
        token: PatronTokenProvider,
        url: str,
        extra_headers: dict[str, str] | None = ...,
        data: str | None = ...,
        method: str | None = ...,
        response_type: type[TOverdriveModel] = ...,
        exception_on_401: bool = ...,
    ) -> TOverdriveModel: ...

    def patron_request[TOverdriveModel: BaseOverdriveModel](
        self,
        token: PatronTokenProvider,
        url: str,
        extra_headers: dict[str, str] | None = None,
        data: str | None = None,
        method: str | None = None,
        response_type: type[TOverdriveModel] | None = None,
        exception_on_401: bool = False,
    ) -> Response | TOverdriveModel:
        """
        Make an HTTP request on behalf of a patron to Overdrive's API.

        A 401 response triggers a single token refresh and retry; a second
        401 raises an IntegrationException.
        """
        headers = {"Authorization": f"Bearer {token()}"}
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
            response = self._do_request(
                method,
                url,
                headers=headers,
                data=data,
                allowed_response_codes=["2xx", 401],
            )
        except BadResponseException as e:
            ErrorResponse.raise_from_response_data(e.response, e.message)
        if response.status_code == 401:
            if exception_on_401:
                # This is our second try. Give up.
                raise IntegrationException(
                    "Something's wrong with the patron OAuth Bearer Token!"
                )
            else:
                # Refresh the token and try again.
                token(force_refresh=True)
                return self.patron_request(
                    token,
                    url,
                    extra_headers=extra_headers,
                    data=data,
                    method=method,
                    response_type=response_type,
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
