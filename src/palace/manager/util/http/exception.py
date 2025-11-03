from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Self
from urllib.parse import urlparse

import httpx
import requests
from flask_babel import lazy_gettext as _
from httpx import Headers

from palace.manager.core.exceptions import IntegrationException, PalaceValueError
from palace.manager.core.problem_details import INTEGRATION_ERROR
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail


class RemoteIntegrationException(IntegrationException, BaseProblemDetailException):
    """An exception that happens when we try and fail to communicate
    with a third-party service over HTTP.
    """

    title = _("Failure contacting external service")
    detail = _(
        "The server tried to access %(service)s but the third-party service experienced an error."
    )
    internal_message = "Error accessing %s: %s"

    def __init__(
        self, url_or_service: str, message: str, debug_message: str | None = None
    ) -> None:
        """Indicate that a remote integration has failed.

        `param url_or_service` The name of the service that failed
           (e.g. "Overdrive"), or the specific URL that had the problem.
        """
        if url_or_service and any(
            url_or_service.startswith(x) for x in ("http:", "https:")
        ):
            self.url = url_or_service
            self.service = urlparse(url_or_service).netloc
        else:
            self.url = self.service = url_or_service

        super().__init__(message, debug_message)

    def __str__(self) -> str:
        message = super().__str__()
        if self.debug_message:
            message += "\n\n" + self.debug_message
        return self.internal_message % (self.url, message)

    @property
    def problem_detail(self) -> ProblemDetail:
        return INTEGRATION_ERROR.detailed(
            detail=self.document_detail(),
            title=self.title,
            debug_message=self.document_debug_message(),
        )

    def document_detail(self) -> str:
        return _(str(self.detail), service=self.service)  # type: ignore[no-any-return]

    def document_debug_message(self) -> str:
        return str(self)


@dataclass(frozen=True)
class HttpResponse:
    """
    A simple dataclass representing an HTTP response.

    We use this so that we can have a common representation of HTTP responses
    from different libraries (like requests and httpx) without depending on either
    library throughout our codebase.
    """

    status_code: int
    url: str
    headers: Headers
    text: str
    content: bytes
    extensions: Mapping[str, Any]

    def json(self) -> Any:
        return json.loads(self.text)

    @classmethod
    def from_response(cls, response: requests.Response | httpx.Response | Self) -> Self:
        if isinstance(response, cls):
            return response

        extensions = (
            {} if isinstance(response, requests.Response) else response.extensions
        )

        return cls(
            status_code=response.status_code,
            url=str(response.url),
            headers=Headers(response.headers),
            text=response.text,
            content=response.content,
            extensions=extensions,
        )


class BadResponseException(RemoteIntegrationException):
    """The request seemingly went okay, but we got a bad response."""

    title = _("Bad response")
    detail = _(
        "The server made a request to %(service)s, and got an unexpected or invalid response."
    )
    internal_message = "Bad response from %s: %s"

    BAD_STATUS_CODE_MESSAGE = (
        "Got status code %s from external server, cannot continue."
    )

    def __init__(
        self,
        url_or_service: str,
        message: str,
        response: httpx.Response | requests.Response | HttpResponse,
        debug_message: str | None = None,
        retry_count: int | None = None,
    ):
        """Indicate that a remote integration has failed.

        :param url_or_service: The name of the service that failed
           (e.g. "Overdrive"), or the specific URL that had the problem.
        :param message: The error message
        :param response: The HTTP response object
        :param debug_message: Optional debug message
        :param retry_count: Number of times the request was retried before failing,
            or None if retry tracking is not available
        """
        if debug_message is None:
            debug_message = (
                f"Status code: {response.status_code}\nContent: {response.text}"
            )

        super().__init__(url_or_service, message, debug_message)
        self.response = HttpResponse.from_response(response)
        self.retry_count: int | None = retry_count

    @classmethod
    def bad_status_code(
        cls, url: str, response: httpx.Response | requests.Response
    ) -> Self:
        """The response is bad because the status code is wrong."""
        message = cls.BAD_STATUS_CODE_MESSAGE % response.status_code
        return cls(
            url,
            message,
            response,
        )

    def __getstate__(self) -> dict[str, Any]:
        return {"dict": self.__dict__, "args": self.args}

    def __setstate__(self, state: dict[str, Any] | None) -> None:
        if state is None:
            raise PalaceValueError(
                "Cannot deserialize BadResponseException with no state"
            )

        self.__dict__.update(state["dict"])
        self.args = state["args"]

    def __reduce__(self) -> tuple[Any, ...]:
        state = self.__getstate__()
        return self.__class__.__new__, (self.__class__,), state


class RequestNetworkException(RemoteIntegrationException):
    """An exception from the requests module that can be represented as
    a problem detail document.
    """

    title = _("Network failure contacting third-party service")
    detail = _("The server experienced a network error while contacting %(service)s.")
    internal_message = "Network error contacting %s: %s"


class RequestTimedOut(RequestNetworkException):
    """A timeout exception that can be represented as a problem
    detail document.
    """

    title = _("Timeout")
    detail = _("The server made a request to %(service)s, and that request timed out.")
    internal_message = "Timeout accessing %s: %s"

    def __init__(
        self,
        url_or_service: str,
        message: str,
        debug_message: str | None = None,
        retry_count: int | None = None,
    ) -> None:
        """Indicate that a request timed out.

        :param url_or_service: The name of the service that failed
           (e.g. "Overdrive"), or the specific URL that had the problem.
        :param message: The error message
        :param debug_message: Optional debug message
        :param retry_count: Number of times the request was retried before failing,
            or None if retry tracking is not available
        """
        super().__init__(url_or_service, message, debug_message)
        self.retry_count: int | None = retry_count
