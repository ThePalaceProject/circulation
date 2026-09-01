"""HTTP transport layer for the Overdrive integration.

This module separates the mechanics of talking to the Overdrive API --
host selection, URL templating, OAuth token management, request execution,
and 401 retry behavior -- from the business logic in
:mod:`palace.manager.integration.license.overdrive.api`.
"""

from __future__ import annotations

from threading import Lock
from typing import Any

from frozendict import frozendict
from pydantic import ValidationError
from requests import Response
from requests.structures import CaseInsensitiveDict

from palace.util.log import LoggerMixin

from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.license.overdrive.constants import OverdriveConstants
from palace.manager.integration.license.overdrive.exception import (
    OverdriveValidationError,
)
from palace.manager.integration.license.overdrive.settings import OverdriveSettings
from palace.manager.util import base64
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.http.http import HTTP


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

    def __init__(self, settings: OverdriveSettings) -> None:
        self._server_nickname = settings.overdrive_server_nickname
        self._hosts = self._determine_hosts(server_nickname=self._server_nickname)

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
        self._max_retry_count = settings.max_retry_count

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

    def _do_get(self, url: str, headers: dict[str, str], **kwargs: Any) -> Response:
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self._max_retry_count
        kwargs["timeout"] = 120
        return HTTP.get_with_timeout(url, headers=headers, **kwargs)

    def _do_post(
        self, url: str, payload: dict[str, str], headers: dict[str, str], **kwargs: Any
    ) -> Response:
        url = self.endpoint(url)
        kwargs["max_retry_count"] = self._max_retry_count
        kwargs["timeout"] = 120
        return HTTP.post_with_timeout(url, data=payload, headers=headers, **kwargs)

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
