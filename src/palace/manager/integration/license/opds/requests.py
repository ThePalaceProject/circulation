from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import Enum
from functools import partial
from typing import TypeVar, overload

from pydantic import ValidationError
from requests import Response
from typing_extensions import Unpack

from palace.manager.api.model.token import OAuthTokenResponse
from palace.manager.core.exceptions import IntegrationException, PalaceValueError
from palace.manager.integration.license.opds.exception import OpdsResponseException
from palace.manager.opds.authentication import AuthenticationDocument
from palace.manager.util.http import (
    HTTP,
    BadResponseException,
    BearerAuth,
    RequestKwargs,
    ResponseCodesT,
)

T = TypeVar("T")


class BaseOpdsHttpRequest(ABC):
    """
    Base class for OPDS HTTP requests.

    Different subclasses can implement different authentication methods.
    """

    @classmethod
    def _make_request(
        cls, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        """Actually make an HTTP request."""
        return HTTP.request_with_timeout(http_method, url, **kwargs)

    @abstractmethod
    def _request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        """Subclasses should implement this method to make the actual HTTP request.
        with appropriate authentication."""
        ...

    @overload
    def __call__(
        self,
        http_method: str,
        url: str,
        *,
        parser: Callable[[bytes], T],
        **kwargs: Unpack[RequestKwargs],
    ) -> T: ...

    @overload
    def __call__(
        self,
        http_method: str,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response: ...

    def __call__(
        self,
        http_method: str,
        url: str,
        *,
        parser: Callable[[bytes], T] | None = None,
        **kwargs: Unpack[RequestKwargs],
    ) -> T | Response:
        try:
            response = self._request(http_method, url, **kwargs)
            if parser is None:
                return response
            return parser(response.content)
        except BadResponseException as e:
            response = e.response
            if opds_exception := OpdsResponseException.from_response(response):
                raise opds_exception from e
            raise


class NoAuthOpdsRequest(BaseOpdsHttpRequest):
    """An OPDS request that does not require authentication."""

    def _request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        return self._make_request(http_method, url, **kwargs)


class BasicAuthOpdsRequest(BaseOpdsHttpRequest):
    """An OPDS request that requires basic authentication."""

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password

    def _request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        kwargs["auth"] = (self._username, self._password)
        return self._make_request(http_method, url, **kwargs)


class OAuthOpdsRequest(BaseOpdsHttpRequest):
    """An OPDS request that authenticates via OAuth."""

    def __init__(self, feed_url: str, username: str, password: str) -> None:
        self._feed_url = feed_url
        self._username = username
        self._password = password

        self.session_token: OAuthTokenResponse | None = None
        self._token_url: str | None = None

    def _request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        # If the request restricts allowed response codes, we need to add 401 to the allowed response codes
        # so that we can handle the 401 response and refresh the token, if necessary.
        original_allowed_response_codes: ResponseCodesT = kwargs.get(
            "allowed_response_codes"
        )
        if (
            original_allowed_response_codes
            and 401 not in original_allowed_response_codes
        ):
            allowed_response_codes = list(original_allowed_response_codes)
            allowed_response_codes.append(401)
            kwargs["allowed_response_codes"] = allowed_response_codes

        # Make a request, refreshing the token if we get a 401 response
        make_request = partial(self._make_request, http_method, url, **kwargs)
        resp = make_request(auth=self._auth)
        if resp.status_code == 401:
            self.refresh_token()
            resp = make_request(
                auth=self._auth, allowed_response_codes=original_allowed_response_codes
            )

        return resp

    @property
    def _auth(self) -> BearerAuth:
        if self.session_token is None or self.session_token.expired:
            token = self.refresh_token()
        else:
            token = self.session_token

        return BearerAuth(token.access_token)

    @staticmethod
    def _get_oauth_url_from_auth_document(auth_document_str: str) -> str:
        try:
            auth_document = AuthenticationDocument.model_validate_json(
                auth_document_str
            )
            return (
                auth_document.by_type(
                    "http://opds-spec.org/auth/oauth/client_credentials"
                )
                .links.get(rel="authenticate", raising=True)
                .href
            )
        except ValidationError as e:
            raise IntegrationException(
                "Invalid OPDS authentication document",
                debug_message=f"Auth document: {auth_document_str}",
            ) from e
        except PalaceValueError:
            raise IntegrationException(
                "Unable to find valid authentication link for "
                "'http://opds-spec.org/auth/oauth/client_credentials' with rel 'authenticate'",
                debug_message=f"Auth document: {auth_document_str}",
            )

    @classmethod
    def _oauth_session_token_refresh(
        cls, auth_url: str, username: str, password: str
    ) -> OAuthTokenResponse:
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = dict(grant_type="client_credentials")
        resp = cls._make_request(
            "POST",
            auth_url,
            headers=headers,
            data=body,
            auth=(username, password),
            allowed_response_codes=["2xx"],
        )
        try:
            token = OAuthTokenResponse.model_validate_json(resp.content)
        except ValidationError as e:
            raise IntegrationException(
                "Invalid oauth authentication response",
                debug_message=f"Authentication response: {resp.text}",
            ) from e

        return token

    def _fetch_auth_document(self) -> str:
        resp = self._make_request("GET", self._feed_url)
        content_type = resp.headers.get("Content-Type")
        if (
            resp.status_code != 401
            or content_type not in AuthenticationDocument.content_types()
        ):
            raise IntegrationException(
                "Unable to fetch OPDS authentication document. Incorrect status code or content type.",
                debug_message=f"Status code: '{resp.status_code}' Content-type: '{content_type}' Response: {resp.text}",
            )
        return resp.text

    def refresh_token(self) -> OAuthTokenResponse:
        if self._token_url is None:
            auth_document = self._fetch_auth_document()
            token_url = self._token_url = self._get_oauth_url_from_auth_document(
                auth_document
            )
        else:
            token_url = self._token_url

        self.session_token = self._oauth_session_token_refresh(
            token_url, self._username, self._password
        )
        return self.session_token


class OPDS2AuthType(Enum):
    BASIC = "Basic Auth"
    OAUTH = "OAuth (via OPDS authentication document)"
    NONE = "None"


def get_opds_requests(
    authentication: OPDS2AuthType,
    username: str | None,
    password: str | None,
    feed_url: str | None,
) -> BaseOpdsHttpRequest:
    """Get the appropriate OPDS request class based on the authentication type."""
    if authentication == OPDS2AuthType.BASIC:
        if not username or not password:
            raise PalaceValueError("Username and password are required for basic auth.")
        return BasicAuthOpdsRequest(username, password)
    elif authentication == OPDS2AuthType.OAUTH:
        if not username or not password or not feed_url:
            raise PalaceValueError(
                "Username, password and feed_url are required for OAuth."
            )
        return OAuthOpdsRequest(feed_url, username, password)
    elif authentication == OPDS2AuthType.NONE:
        return NoAuthOpdsRequest()
    else:
        raise PalaceValueError(
            f"Unsupported authentication type: {authentication}. "
            "Supported types are: Basic, OAuth, None."
        )
