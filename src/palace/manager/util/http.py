from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from json import JSONDecodeError
from typing import Any
from urllib.parse import urlparse

import requests
from flask_babel import lazy_gettext as _
from requests import PreparedRequest, sessions
from requests.adapters import HTTPAdapter, Response
from requests.auth import AuthBase
from typing_extensions import Self
from urllib3 import Retry

from palace import manager
from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.problem_details import INTEGRATION_ERROR
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import (
    JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE,
)
from palace.manager.util.problem_detail import (
    BaseProblemDetailException,
    ProblemDetail,
    ProblemDetailException,
)


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
        response: Response,
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
        self.response = response

    @classmethod
    def bad_status_code(cls, url: str, response: Response) -> Self:
        """The response is bad because the status code is wrong."""
        message = cls.BAD_STATUS_CODE_MESSAGE % response.status_code
        return cls(
            url,
            message,
            response,
        )


class RequestNetworkException(
    RemoteIntegrationException, requests.exceptions.RequestException
):
    """An exception from the requests module that can be represented as
    a problem detail document.
    """

    title = _("Network failure contacting third-party service")
    detail = _("The server experienced a network error while contacting %(service)s.")
    internal_message = "Network error contacting %s: %s"


class RequestTimedOut(RequestNetworkException, requests.exceptions.Timeout):
    """A timeout exception that can be represented as a problem
    detail document.
    """

    title = _("Timeout")
    detail = _("The server made a request to %(service)s, and that request timed out.")
    internal_message = "Timeout accessing %s: %s"


class HTTP(LoggerMixin):
    """A helper for the `requests` module."""

    # In case an app version is not present, we can use this version as a fallback
    # for all outgoing http requests without a custom user-agent
    DEFAULT_USER_AGENT_VERSION = "1.x.x"

    DEFAULT_REQUEST_RETRIES = 5
    DEFAULT_REQUEST_TIMEOUT = 20

    @classmethod
    def set_quick_failure_settings(cls) -> None:
        """Ensure any outgoing requests aren't long-running"""
        cls.DEFAULT_REQUEST_RETRIES = 0
        cls.DEFAULT_REQUEST_TIMEOUT = 5

    @classmethod
    def get_with_timeout(cls, url: str, *args: Any, **kwargs: Any) -> Response:
        """Make a GET request with timeout handling."""
        return cls.request_with_timeout("GET", url, *args, **kwargs)

    @classmethod
    def post_with_timeout(
        cls, url: str, payload: str | Mapping[str, Any], *args: Any, **kwargs: Any
    ) -> Response:
        """Make a POST request with timeout handling."""
        kwargs["data"] = payload
        return cls.request_with_timeout("POST", url, *args, **kwargs)

    @classmethod
    def put_with_timeout(
        cls, url: str, payload: str | Mapping[str, Any], *args: Any, **kwargs: Any
    ) -> Response:
        """Make a PUT request with timeout handling."""
        kwargs["data"] = payload
        return cls.request_with_timeout("PUT", url, *args, **kwargs)

    @classmethod
    def request_with_timeout(
        cls, http_method: str, url: str, *args: Any, **kwargs: Any
    ) -> Response:
        """Call requests.request and turn a timeout into a RequestTimedOut
        exception.
        """
        return cls._request_with_timeout(
            url, sessions.Session.request, http_method, *args, **kwargs
        )

    # The set of status codes on which a retry will be attempted (if the number of retries requested is non-zero).
    RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

    @classmethod
    def _request_with_timeout(
        cls,
        url: str,
        make_request_with: Callable[..., Response],
        *args: Any,
        **kwargs: Any,
    ) -> Response:
        """Call some kind of method and turn a timeout into a RequestTimedOut
        exception.

        The core of `request_with_timeout` made easy to test.

        :param url: Make the request to this URL.
        :param make_request_with: A function that actually makes the
            HTTP request.
        :param args: Positional arguments for the request function.
        :param kwargs: Keyword arguments for the request function.
        """
        process_response_with = kwargs.pop(
            "process_response_with", cls._process_response
        )
        allowed_response_codes = kwargs.pop("allowed_response_codes", [])
        disallowed_response_codes = kwargs.pop("disallowed_response_codes", [])
        verbose = kwargs.pop("verbose", False)

        if not "timeout" in kwargs:
            kwargs["timeout"] = cls.DEFAULT_REQUEST_TIMEOUT

        max_retry_count: int = int(
            kwargs.pop("max_retry_count", cls.DEFAULT_REQUEST_RETRIES)
        )
        backoff_factor: float = float(kwargs.pop("backoff_factor", 1.0))

        # Unicode data can't be sent over the wire. Convert it to UTF-8.
        if "data" in kwargs and isinstance(kwargs["data"], str):
            kwdata: str = kwargs["data"]
            kwargs["data"] = kwdata.encode("utf8")

        # Set a user-agent if not already present
        version = (
            manager.__version__
            if manager.__version__
            else cls.DEFAULT_USER_AGENT_VERSION
        )
        ua_header = {"User-Agent": f"Palace Manager/{version}"}
        headers = ua_header | (kwargs.get("headers") or {})

        # Make sure headers are encoded as utf-8
        kwargs["headers"] = {
            k.encode() if isinstance(k, str) else k: (
                v.encode() if isinstance(v, str) else v
            )
            for k, v in headers.items()
        }

        try:
            if verbose:
                logging.info(
                    "Sending request to %s: args %r kwargs %r", url, args, kwargs
                )
            if len(args) == 1:
                # requests.request takes two positional arguments,
                # an HTTP method and a URL. In most cases, the URL
                # gets added on here. But if you do pass in both
                # arguments, it will still work.
                args = args + (url,)

            request_start_time = time.time()
            if make_request_with == sessions.Session.request:
                with sessions.Session() as session:
                    retry_strategy = Retry(
                        total=max_retry_count,
                        status_forcelist=cls.RETRY_STATUS_CODES,
                        backoff_factor=backoff_factor,
                    )
                    adapter = HTTPAdapter(max_retries=retry_strategy)

                    session.mount("http://", adapter)
                    session.mount("https://", adapter)

                    response = session.request(*args, **kwargs)
            else:
                response = make_request_with(*args, **kwargs)
            cls.logger().info(
                f"Request time for {url} took {time.time() - request_start_time:.2f} seconds"
            )

            if verbose:
                logging.info(
                    "Response from %s: %s %r %r",
                    url,
                    response.status_code,
                    response.headers,
                    response.content,
                )
        except requests.exceptions.Timeout as e:
            # Wrap the requests-specific Timeout exception
            # in a generic RequestTimedOut exception.
            raise RequestTimedOut(url, str(e)) from e
        except requests.exceptions.RequestException as e:
            # Wrap all other requests-specific exceptions in
            # a generic RequestNetworkException.
            raise RequestNetworkException(url, str(e)) from e

        return process_response_with(  # type: ignore[no-any-return]
            url,
            response,
            allowed_response_codes,
            disallowed_response_codes,
        )

    @classmethod
    def _process_response(
        cls,
        url: str,
        response: Response,
        allowed_response_codes: Sequence[int | str] | None = None,
        disallowed_response_codes: Sequence[int | str] | None = None,
    ) -> Response:
        """Raise a RequestNetworkException if the response code indicates a
        server-side failure, or behavior so unpredictable that we can't
        continue.

        :param allowed_response_codes If passed, then only the responses with
            http status codes in this list are processed.  The rest generate
            BadResponseExceptions. If both allowed_response_codes and
            disallowed_response_codes are passed, then the allowed_response_codes
            list is used.
        :param disallowed_response_codes The values passed are added to 5xx, as
            http status codes that would generate BadResponseExceptions.
        """
        allowed_response_codes_str = (
            list(map(str, allowed_response_codes)) if allowed_response_codes else []
        )
        disallowed_response_codes_str = (
            list(map(str, disallowed_response_codes))
            if disallowed_response_codes
            else []
        )

        series = cls.series(response.status_code)
        code = str(response.status_code)

        if code in allowed_response_codes_str or series in allowed_response_codes_str:
            # The code or series has been explicitly allowed. Allow
            # the request to be processed.
            return response

        error_message = None
        if (
            series == "5xx"
            or code in disallowed_response_codes_str
            or series in disallowed_response_codes_str
        ):
            # Unless explicitly allowed, the 5xx series always results in
            # an exception.
            error_message = BadResponseException.BAD_STATUS_CODE_MESSAGE
        elif allowed_response_codes and not (
            code in allowed_response_codes_str or series in allowed_response_codes_str
        ):
            error_message = (
                "Got status code %%s from external server, but can only continue on: %s."
                % (", ".join(sorted(allowed_response_codes_str)),)
            )

        if error_message:
            raise BadResponseException(
                url,
                error_message % code,
                debug_message="Response content: %s"
                % cls._decode_response_content(response, url),
                response=response,
            )
        return response

    @classmethod
    def _decode_response_content(cls, response: Response, url: str) -> str:
        try:
            return response.text
        except Exception as e:
            raise RequestNetworkException(url, str(e)) from e

    @classmethod
    def series(cls, status_code: int) -> str:
        """Return the HTTP series for the given status code."""
        return "%sxx" % (int(status_code) // 100)

    @classmethod
    def debuggable_get(cls, url: str, **kwargs: Any) -> Response:
        """Make a GET request that returns a detailed problem
        detail document on error.
        """
        return cls.debuggable_request("GET", url, **kwargs)

    @classmethod
    def debuggable_post(
        cls, url: str, payload: str | dict[str, Any], **kwargs: Any
    ) -> Response:
        """Make a POST request that returns a detailed problem
        detail document on error.
        """
        kwargs["data"] = payload
        return cls.debuggable_request("POST", url, **kwargs)

    @classmethod
    def debuggable_request(
        cls,
        http_method: str,
        url: str,
        make_request_with: Callable[..., Response] | None = None,
        **kwargs: Any,
    ) -> Response:
        """Make a request that raises a ProblemError with a detailed problem detail
        document on error, rather than a generic "an integration error occurred"
        message.

        :param http_method: HTTP method to use when making the request.
        :param url: Make the request to this URL.
        :param make_request_with: A function that actually makes the
            HTTP request.
        :param kwargs: Keyword arguments for the make_request_with
            function.
        """
        logging.info(
            "Making debuggable %s request to %s: kwargs %r", http_method, url, kwargs
        )
        make_request_with = make_request_with or requests.request
        return cls._request_with_timeout(
            url,
            make_request_with,
            http_method,
            process_response_with=cls.process_debuggable_response,
            **kwargs,
        )

    @classmethod
    def process_debuggable_response(
        cls,
        url: str,
        response: Response,
        allowed_response_codes: list[str | int] | None = None,
        disallowed_response_codes: list[str | int] | None = None,
    ) -> Response:
        """If there was a problem with an integration request,
        raise ProblemError with an appropriate ProblemDetail. Otherwise, return the
        response to the original request.

        :param response: A Response object from the requests library.
        """

        allowed_response_codes = allowed_response_codes or ["2xx", "3xx"]
        allowed_response_codes_str = list(map(str, allowed_response_codes))
        disallowed_response_codes = disallowed_response_codes or []
        disallowed_response_codes_str = list(map(str, disallowed_response_codes))

        code = response.status_code
        series = cls.series(code)
        if (
            str(code) in allowed_response_codes_str
            or series in allowed_response_codes_str
        ):
            # Whether it looks like there's been a problem,
            # we've been told to let this response code through.
            return response

        content_type = response.headers.get("Content-Type")
        if content_type == PROBLEM_DETAIL_JSON_MEDIA_TYPE:
            # The server returned a problem detail document. Wrap it
            # in a new document that represents the integration
            # failure.
            try:
                problem_detail = INTEGRATION_ERROR.detailed(
                    f"Remote service returned a problem detail document: '{response.text}'"
                )
                problem_detail.debug_message = response.text
                raise ProblemDetailException(problem_detail=problem_detail)
            except JSONDecodeError:
                # Failed to decode the problem detail document, we just fall through
                # and raise the generic integration error.
                pass

        # There's been a problem. Return the message we got from the
        # server, verbatim.
        raise ProblemDetailException(
            problem_detail=INTEGRATION_ERROR.detailed(
                f'{response.status_code} response from integration server: "{response.text}"'
            )
        )


class BearerAuth(AuthBase):
    """
    Requests Auth class that supports authentication using a Bearer token.

    See: https://docs.python-requests.org/en/latest/user/authentication/#new-forms-of-authentication
    """

    def __init__(self, token: str) -> None:
        self.token = token

    def __call__(self, r: PreparedRequest) -> PreparedRequest:
        r.headers["authorization"] = f"Bearer {self.token}"
        return r

    def __repr__(self) -> str:
        return f"BearerAuth({self.token})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, BearerAuth):
            return NotImplemented
        return self.token == other.token
