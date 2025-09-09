from __future__ import annotations

from typing import Generic, TypeVar
from urllib.parse import urlparse

import httpx
import requests
from flask_babel import lazy_gettext as _
from typing_extensions import Self

from palace.manager.core.exceptions import IntegrationException
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


T = TypeVar("T", requests.Response, httpx.Response)


class BadResponseException(RemoteIntegrationException, Generic[T]):
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
        response: T,
        debug_message: str | None = None,
    ):
        """Indicate that a remote integration has failed.

        `param url_or_service` The name of the service that failed
           (e.g. "Overdrive"), or the specific URL that had the problem.
        """
        if debug_message is None:
            debug_message = (
                f"Status code: {response.status_code}\nContent: {response.text}"
            )

        super().__init__(url_or_service, message, debug_message)
        self.response: T = response

    @classmethod
    def bad_status_code(cls, url: str, response: T) -> Self:
        """The response is bad because the status code is wrong."""
        message = cls.BAD_STATUS_CODE_MESSAGE % response.status_code
        return cls(
            url,
            message,
            response,
        )


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
