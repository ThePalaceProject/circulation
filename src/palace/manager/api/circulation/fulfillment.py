from __future__ import annotations

from abc import ABC, abstractmethod

import requests
from flask import Response

from palace.manager.util.http.base import ResponseCodesTypes
from palace.manager.util.http.exception import BadResponseException, ResponseData
from palace.manager.util.http.http import HTTP
from palace.manager.util.log import LoggerMixin


class Fulfillment(ABC):
    """
    Represents a method of fulfilling a loan.
    """

    @abstractmethod
    def response(self) -> Response:
        """
        Return a Flask Response object that can be used to fulfill a loan.
        """
        ...


class UrlFulfillment(Fulfillment, ABC):
    """
    Represents a method of fulfilling a loan that has a URL to an external resource.
    """

    def __init__(self, content_link: str, content_type: str | None = None) -> None:
        self.content_link = content_link
        self.content_type = content_type

    def __repr__(self) -> str:
        repr_data = [f"content_link: {self.content_link}"]
        if self.content_type:
            repr_data.append(f"content_type: {self.content_type}")
        return f"<{self.__class__.__name__}: {', '.join(repr_data)}>"


class DirectFulfillment(Fulfillment):
    """
    Represents a method of fulfilling a loan by directly serving some content
    that we know about locally.
    """

    def __init__(self, content: str | bytes, content_type: str | None) -> None:
        self.content = content
        self.content_type = content_type

    def response(self) -> Response:
        return Response(self.content, content_type=self.content_type)

    def __repr__(self) -> str:
        length = len(self.content)
        return f"<{self.__class__.__name__}: content_type: {self.content_type}, content: {length} bytes>"


class RedirectFulfillment(UrlFulfillment):
    """
    Fulfill a loan by redirecting the client to a URL.
    """

    def response(self) -> Response:
        return Response(
            f"Redirecting to {self.content_link} ...",
            status=302,
            headers={"Location": self.content_link},
            content_type="text/plain",
        )


class FetchResponse(Response):
    """
    Response object that defaults to no mimetype if none is provided.
    """

    default_mimetype = None


class FetchFulfillment(UrlFulfillment, LoggerMixin):
    """
    Fulfill a loan by fetching a URL and returning the content. This should be
    avoided for large files, since it will be slow and unreliable as well as
    blocking the server.

    In some cases for small files like ACSM or LCPL files, the server may be
    the only entity that can fetch the file, so we fetch it and then return it
    to the client.
    """

    def __init__(
        self,
        content_link: str,
        content_type: str | None = None,
        *,
        include_headers: dict[str, str] | None = None,
        allowed_response_codes: ResponseCodesTypes | None = None,
    ) -> None:
        super().__init__(content_link, content_type)
        self.include_headers = include_headers or {}
        self.allowed_response_codes = allowed_response_codes or []

    def get(self, url: str) -> requests.Response:
        return HTTP.get_with_timeout(
            url,
            headers=self.include_headers,
            allowed_response_codes=self.allowed_response_codes,
            allow_redirects=True,
        )

    def response(self) -> Response:
        response: requests.Response | ResponseData
        try:
            response = self.get(self.content_link)
        except BadResponseException as ex:
            response = ex.response
            self.log.exception(
                f"Error fulfilling loan. Bad response from: {self.content_link}. "
                f"Status code: {response.status_code}. "
                f"Response: {response.text}."
            )
            raise

        headers = {"Cache-Control": "private"}

        if self.content_type:
            headers["Content-Type"] = self.content_type
        elif "Content-Type" in response.headers:
            headers["Content-Type"] = response.headers["Content-Type"]

        return FetchResponse(
            response.content, status=response.status_code, headers=headers
        )
