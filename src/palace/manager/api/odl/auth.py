from abc import ABC, abstractmethod
from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, PositiveInt, ValidationError
from requests import Response
from typing_extensions import Self

from palace.manager.api.odl.settings import OPDS2AuthType
from palace.manager.core.exceptions import IntegrationException, PalaceValueError
from palace.manager.opds.authentication import AuthenticationDocument
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http import (
    HTTP,
    BadResponseException,
    BearerAuth,
    ResponseCodesT,
)
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail


class TokenTuple(NamedTuple):
    token: str
    expires: datetime


class TokenGrant(BaseModel):
    access_token: str
    token_type: Literal["Bearer"]
    expires_in: PositiveInt


class OpdsWithOdlException(BadResponseException):
    """
    ODL and Readium LCP specify that all errors should be returned as Problem
    Detail documents. This isn't always the case, but we try to use this information
    when we can.
    """

    def __init__(
        self,
        type: str,
        title: str,
        status: int,
        detail: str | None,
        response: Response,
    ) -> None:
        super().__init__(url_or_service=response.url, message=title, response=response)
        self.type = type
        self.title = title
        self.status = status
        self.detail = detail

    @property
    def problem_detail(self) -> ProblemDetail:
        return ProblemDetail(
            uri=self.type,
            status_code=self.status,
            title=self.title,
            detail=self.detail,
        )

    @classmethod
    def from_response(cls, response: Response) -> Self | None:
        # Wrap the response in a OpdsWithOdlException if it is a problem detail document.
        #
        # DeMarque sends "application/api-problem+json", but the ODL spec says we should
        # expect "application/problem+json", so we need to check for both.
        if response.headers.get("Content-Type") not in [
            "application/api-problem+json",
            "application/problem+json",
        ]:
            return None

        try:
            json_response = response.json()
        except ValueError:
            json_response = {}

        type = json_response.get("type")
        title = json_response.get("title")
        status = json_response.get("status") or response.status_code
        detail = json_response.get("detail")

        if type is None or title is None:
            return None

        return cls(type, title, status, detail, response)


class OdlAuthenticatedRequest(LoggerMixin, ABC):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._session_token: TokenTuple | None = None
        self._token_url: str | None = None

    @property
    @abstractmethod
    def _username(self) -> str: ...

    @property
    @abstractmethod
    def _password(self) -> str: ...

    @property
    @abstractmethod
    def _auth_type(self) -> OPDS2AuthType: ...

    @property
    @abstractmethod
    def _feed_url(self) -> str: ...

    def _no_auth_request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> Response:
        return HTTP.request_with_timeout(method, url, headers=headers, **kwargs)

    def _basic_auth_request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> Response:
        return HTTP.request_with_timeout(
            method,
            url,
            headers=headers,
            auth=(self._username, self._password),
            **kwargs,
        )

    def _oauth_request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> Response:
        # If the request restricts allowed response codes, we need to add 401 to the allowed response codes
        # so that we can handle the 401 response and refresh the token, if necessary.
        original_allowed_response_codes: ResponseCodesT = (
            kwargs.get("allowed_response_codes") or []
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
        resp = self._session_token_request(method, url, headers=headers, **kwargs)
        if resp.status_code == 401 and not token_refreshed:
            self._refresh_token()
            resp = self._session_token_request(method, url, headers=headers, **kwargs)

        # If we got a 401 response and we modified the allowed response codes, we process the response
        # with the original allowed response codes, so the calling function can handle the 401 response.
        if original_allowed_response_codes and resp.status_code == 401:
            # Process the original allowed response codes
            return HTTP._process_response(url, resp, original_allowed_response_codes)

        return resp

    def _request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> Response:
        try:
            match self._auth_type:
                case OPDS2AuthType.BASIC:
                    return self._basic_auth_request(
                        method, url, headers=headers, **kwargs
                    )
                case OPDS2AuthType.OAUTH:
                    return self._oauth_request(method, url, headers=headers, **kwargs)
                case OPDS2AuthType.NONE:
                    return self._no_auth_request(method, url, headers=headers, **kwargs)
                case _:
                    raise PalaceValueError(
                        f"Invalid OPDS2AuthType: '{self._auth_type}'"
                    )
        except BadResponseException as e:
            response = e.response
            # Create a OpdsWithOdlException if the response is a problem detail document.
            if opds_exception := OpdsWithOdlException.from_response(response):
                raise opds_exception from e
            raise

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

        try:
            return (
                auth_document.by_type(
                    "http://opds-spec.org/auth/oauth/client_credentials"
                )
                .links.get(rel="authenticate", raising=True)
                .href
            )
        except PalaceValueError:
            raise IntegrationException(
                "Unable to find valid authentication link for "
                "'http://opds-spec.org/auth/oauth/client_credentials' with rel 'authenticate'",
                debug_message=f"Auth document: {auth_document_str}",
            )

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
        if (
            resp.status_code != 401
            or content_type not in AuthenticationDocument.content_types()
        ):
            raise IntegrationException(
                "Unable to fetch OPDS authentication document. Incorrect status code or content type.",
                debug_message=f"Status code: '{resp.status_code}' Content-type: '{content_type}' Response: {resp.text}",
            )
        return resp.text

    def _session_token_request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str] | None = None,
        **kwargs: Any,
    ) -> Response:
        auth = BearerAuth(self._session_token.token) if self._session_token else None
        return HTTP.request_with_timeout(
            method, url, headers=headers, auth=auth, **kwargs
        )

    def _refresh_token(self) -> None:
        if self._token_url is None:
            auth_document = self._fetch_auth_document()
            self._token_url = self._get_oauth_url_from_auth_document(auth_document)

        self._session_token = self._oauth_session_token_refresh(
            self._token_url, self._username, self._password
        )
