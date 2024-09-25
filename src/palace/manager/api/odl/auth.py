from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, Literal, NamedTuple

from pydantic import AnyUrl, BaseModel, PositiveInt, ValidationError
from requests import Response

from palace.manager.api.odl.settings import OPDS2AuthType
from palace.manager.core.exceptions import IntegrationException, PalaceValueError
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import HTTP, BearerAuth


class TokenTuple(NamedTuple):
    token: str
    expires: datetime


class TokenGrant(BaseModel):
    access_token: str
    token_type: Literal["Bearer"]
    expires_in: PositiveInt


class AuthenticationLink(BaseModel):
    rel: str
    href: str


class Authentication(BaseModel):
    type: str
    links: list[AuthenticationLink]

    def by_rel(self, rel: str) -> list[AuthenticationLink]:
        return [link for link in self.links if link.rel == rel]


class AuthenticationDocument(BaseModel):
    id: AnyUrl
    title: str
    authentication: list[Authentication]

    def by_type(self, auth_type: str) -> list[Authentication]:
        return [auth for auth in self.authentication if auth.type == auth_type]

    def link_href_by_type_and_rel(self, auth_type: str, rel: str) -> list[str]:
        return [
            link.href for auth in self.by_type(auth_type) for link in auth.by_rel(rel)
        ]


class ODLAuthenticatedGet(ABC):
    AUTH_DOC_CONTENT_TYPES = [
        "application/opds-authentication+json",
        "application/vnd.opds.authentication.v1.0+json",
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._session_token: TokenTuple | None = None
        self._token_url: str | None = None

    @property
    @abstractmethod
    def _username(self) -> str:
        ...

    @property
    @abstractmethod
    def _password(self) -> str:
        ...

    @property
    @abstractmethod
    def _auth_type(self) -> OPDS2AuthType:
        ...

    @property
    @abstractmethod
    def _feed_url(self) -> str:
        ...

    @staticmethod
    def _no_auth_get(
        url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        return HTTP.get_with_timeout(url, headers=headers, **kwargs)

    def _basic_auth_get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        return HTTP.get_with_timeout(
            url, headers=headers, auth=(self._username, self._password), **kwargs
        )

    def _oauth_get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        # If the request restricts allowed response codes, we need to add 401 to the allowed response codes
        # so that we can handle the 401 response and refresh the token, if necessary.
        original_allowed_response_codes: Sequence[int | str] = kwargs.get(
            "allowed_response_codes", []
        )
        if (
            original_allowed_response_codes
            and 401 not in original_allowed_response_codes
        ):
            allowed_response_codes = list(original_allowed_response_codes)
            allowed_response_codes.append(401)
            kwargs["allowed_response_codes"] = allowed_response_codes

        token_refreshed = False
        if self._session_token is None or self._session_token.expires < utc_now():
            self._refresh_token()
            token_refreshed = True

        # Make a request, refreshing the token if we get a 401 response, and we haven't already refreshed the token.
        resp = self._session_token_get(url, headers=headers, **kwargs)
        if resp.status_code == 401 and not token_refreshed:
            self._refresh_token()
            resp = self._session_token_get(url, headers=headers, **kwargs)

        # If we got a 401 response and we modified the allowed response codes, we process the response
        # with the original allowed response codes, so the calling function can handle the 401 response.
        if original_allowed_response_codes and resp.status_code == 401:
            # Process the original allowed response codes
            return HTTP._process_response(url, resp, original_allowed_response_codes)

        return resp

    def _get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        match self._auth_type:
            case OPDS2AuthType.BASIC:
                return self._basic_auth_get(url, headers, **kwargs)
            case OPDS2AuthType.OAUTH:
                return self._oauth_get(url, headers, **kwargs)
            case OPDS2AuthType.NONE:
                return self._no_auth_get(url, headers=headers, **kwargs)
            case _:
                raise PalaceValueError(f"Invalid OPDS2AuthType: '{self._auth_type}'")

    @staticmethod
    def _get_oauth_url_from_auth_document(auth_document_str: str) -> str:
        try:
            auth_document = AuthenticationDocument.model_validate_json(
                auth_document_str
            )
        except ValidationError as e:
            raise IntegrationException(
                "Invalid OPDS authentication document",
                debug_message=f"Auth document: {auth_document_str}",
            ) from e

        auth_links = auth_document.link_href_by_type_and_rel(
            "http://opds-spec.org/auth/oauth/client_credentials", "authenticate"
        )

        if len(auth_links) != 1:
            raise IntegrationException(
                "Unable to find exactly one valid authentication link",
                debug_message=f"Found {len(auth_links)} authentication links. Auth document: {auth_document_str}",
            )

        return auth_links[0]

    @staticmethod
    def _oauth_session_token_refresh(
        auth_url: str, username: str, password: str
    ) -> TokenTuple:
        start_time = utc_now()
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        body = dict(grant_type="client_credentials")
        resp = HTTP.request_with_timeout(
            "POST",
            auth_url,
            headers=headers,
            data=body,
            auth=(username, password),
            allowed_response_codes=["2xx"],
        )
        try:
            data = TokenGrant.model_validate_json(resp.content)
        except ValidationError as e:
            raise IntegrationException(
                "Invalid oauth authentication response",
                debug_message=f"Authentication response: {resp.text}",
            ) from e

        return TokenTuple(
            data.access_token, start_time + timedelta(seconds=data.expires_in)
        )

    def _fetch_auth_document(self) -> str:
        resp = HTTP.get_with_timeout(self._feed_url)
        content_type = resp.headers.get("Content-Type")
        if resp.status_code != 401 or content_type not in self.AUTH_DOC_CONTENT_TYPES:
            raise IntegrationException(
                "Unable to fetch OPDS authentication document. Incorrect status code or content type.",
                debug_message=f"Status code: '{resp.status_code}' Content-type: '{content_type}' Response: {resp.text}",
            )
        return resp.text

    def _session_token_get(
        self, url: str, headers: Mapping[str, str] | None = None, **kwargs: Any
    ) -> Response:
        auth = BearerAuth(self._session_token.token) if self._session_token else None
        return HTTP.get_with_timeout(url, headers=headers, auth=auth, **kwargs)

    def _refresh_token(self) -> None:
        if self._token_url is None:
            auth_document = self._fetch_auth_document()
            self._token_url = self._get_oauth_url_from_auth_document(auth_document)

        self._session_token = self._oauth_session_token_refresh(
            self._token_url, self._username, self._password
        )
