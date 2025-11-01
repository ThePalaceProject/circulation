from __future__ import annotations

from typing import Any, Self
from urllib.parse import urlparse

import httpx
import requests
from flask_babel import lazy_gettext as _

from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.problem_details import INTEGRATION_ERROR
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail


def _unpickle_bad_response_exception(
    exception_module: str,
    exception_class: str,
    exception_dict: dict[str, Any],
    response_data: dict[str, Any],
    response_module: str,
    response_class: str,
) -> Any:
    # Returns BadResponseException or subclass
    # Using Any here because the generic type parameter doesn't support unions
    """Helper function to unpickle BadResponseException and its subclasses.

    This function recreates a BadResponseException (or subclass) from pickled data by
    constructing a minimal response object and restoring the exception's state.

    :param exception_module: Module name of the exception class
    :param exception_class: Class name of the exception
    :param exception_dict: Dictionary of the exception's __dict__
    :param response_data: Dictionary with response status_code, content, headers, url
    :param response_module: Module name of the original response class
    :param response_class: Class name of the original response class
    :return: Reconstructed exception
    """
    import importlib

    # Create a minimal response object
    response: requests.Response | httpx.Response
    if response_module == "requests.models":
        # Create a requests.Response object
        req_response = requests.Response()
        req_response.status_code = response_data["status_code"]
        req_response._content = response_data["content"]
        req_response.headers.update(response_data["headers"])
        if response_data["url"]:
            req_response.url = response_data["url"]
        response = req_response
    elif response_module == "httpx._models":
        # Create an httpx.Response object
        # httpx.Response requires a request to have a URL, so create a minimal request
        request = httpx.Request(
            method="GET",
            url=response_data["url"] or "http://unknown",
        )
        response = httpx.Response(
            status_code=response_data["status_code"],
            content=response_data["content"],
            headers=response_data["headers"],
            request=request,
        )
    else:
        # Fallback: create a requests.Response object
        req_response = requests.Response()
        req_response.status_code = response_data["status_code"]
        req_response._content = response_data["content"]
        req_response.headers.update(response_data["headers"])
        if response_data["url"]:
            req_response.url = response_data["url"]
        response = req_response

    # Dynamically import and get the exception class
    module = importlib.import_module(exception_module)
    exc_class = getattr(module, exception_class)

    # Create instance without calling __init__
    exc = exc_class.__new__(exc_class)

    # Restore the exception's state
    exc.__dict__.update(exception_dict)

    # Update the response object (since it was reconstructed)
    exc.response = response

    return exc


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


class BadResponseException[T: (requests.Response, httpx.Response)](
    RemoteIntegrationException,
):
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
        self.response: T = response
        self.retry_count: int | None = retry_count

    @classmethod
    def bad_status_code(cls, url: str, response: T) -> Self:
        """The response is bad because the status code is wrong."""
        message = cls.BAD_STATUS_CODE_MESSAGE % response.status_code
        return cls(
            url,
            message,
            response,
        )

    def __reduce__(self) -> tuple[object, tuple[Any, ...]]:
        """Custom pickle support to handle response serialization.

        The requests.Response and httpx.Response objects are not fully pickleable,
        so we need to extract and store only the essential information.

        This method also preserves the exact exception subclass type, so that
        OpdsResponseException, OverdriveResponseException, etc. are properly
        reconstructed during unpickling.
        """
        # Extract essential response information for serialization
        response_data = {
            "status_code": self.response.status_code,
            "content": self.response.content,
            "headers": dict(self.response.headers),
            "url": str(self.response.url) if hasattr(self.response, "url") else None,
        }

        # Create a copy of __dict__ without the response object
        exception_dict = self.__dict__.copy()
        exception_dict.pop("response", None)

        # Return a tuple of (callable, args) for unpickling
        return (
            _unpickle_bad_response_exception,
            (
                type(self).__module__,
                type(self).__qualname__,
                exception_dict,
                response_data,
                type(self.response).__module__,
                type(self.response).__name__,
            ),
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
