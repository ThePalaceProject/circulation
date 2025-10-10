from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping
from io import BytesIO, StringIO
from json import JSONDecodeError
from typing import Any, Literal, Protocol, TypedDict, Unpack

import requests
from requests import PreparedRequest, Session as RequestsSession
from requests.adapters import HTTPAdapter, Response
from requests.auth import AuthBase
from urllib3 import Retry

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.core.problem_details import INTEGRATION_ERROR
from palace.manager.util.http.base import (
    ResponseCodesTypes,
    get_default_headers,
    get_series,
    raise_for_bad_response,
)
from palace.manager.util.http.exception import (
    RequestNetworkException,
    RequestTimedOut,
)
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import (
    JSON_MEDIA_TYPE as PROBLEM_DETAIL_JSON_MEDIA_TYPE,
    ProblemDetailException,
)
from palace.manager.util.sentinel import SentinelType

MakeRequestT = (
    RequestsSession | Callable[..., Response] | Literal[SentinelType.NotGiven]
)


class GetRequestKwargs(TypedDict, total=False):
    params: Mapping[str, str | int | float | None] | None
    headers: Mapping[str, str] | None
    auth: tuple[str, str] | AuthBase | None
    timeout: float | int | None
    allow_redirects: bool
    stream: bool | None
    verify: bool | None

    allowed_response_codes: ResponseCodesTypes
    disallowed_response_codes: ResponseCodesTypes
    verbose: bool
    max_retry_count: int
    backoff_factor: float

    make_request_with: MakeRequestT


class RequestKwargs(GetRequestKwargs, total=False):
    data: Iterable[bytes] | str | bytes | Mapping[str, Any] | None
    files: Mapping[str, BytesIO | StringIO | str | bytes] | None
    json: Mapping[str, Any] | None


class GetRequestCallable(Protocol):
    def __call__(
        self,
        url: str,
        **kwargs: Unpack[GetRequestKwargs],
    ) -> Response: ...


class MakeRequestCallable(Protocol):
    def __call__(
        self,
        method: str,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response: ...


class _ProcessResponseCallable(Protocol):
    def __call__(
        self,
        url: str,
        response: Response,
        allowed_response_codes: ResponseCodesTypes,
        disallowed_response_codes: ResponseCodesTypes,
    ) -> Response: ...


class HTTP(LoggerMixin):
    """A helper for the `requests` module."""

    DEFAULT_REQUEST_RETRIES = 5
    DEFAULT_REQUEST_TIMEOUT = 20
    DEFAULT_BACKOFF_FACTOR = 1.0

    @classmethod
    def set_quick_failure_settings(cls) -> None:
        """Ensure any outgoing requests aren't long-running"""
        cls.DEFAULT_REQUEST_RETRIES = 0
        cls.DEFAULT_REQUEST_TIMEOUT = 5

    @classmethod
    def session(
        cls,
        max_retry_count: int | None = None,
        backoff_factor: float | None = None,
    ) -> RequestsSession:
        """
        Create a requests session with the given retry settings.

        Using the session allows future requests to reuse the same connection
        and settings, which can improve performance and reduce overhead when
        making multiple requests to the same host.

        Note: RequestsSession is not thread-safe, so this should be used
        in a context where the session is not shared across threads.
        """
        max_retry_count = (
            max_retry_count
            if max_retry_count is not None
            else cls.DEFAULT_REQUEST_RETRIES
        )
        backoff_factor = (
            backoff_factor if backoff_factor is not None else cls.DEFAULT_BACKOFF_FACTOR
        )

        session = RequestsSession()
        retry_strategy = Retry(
            total=max_retry_count,
            status_forcelist=cls.RETRY_STATUS_CODES,
            backoff_factor=backoff_factor,
            # We set raise_on_status to False, so if our automatic retries are exhausted,
            # we can handle the final response ourselves in _process_response.
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)

        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    @classmethod
    def get_with_timeout(cls, url: str, **kwargs: Unpack[GetRequestKwargs]) -> Response:
        """Make a GET request with timeout handling."""
        return cls.request_with_timeout("GET", url, **kwargs)

    @classmethod
    def post_with_timeout(
        cls,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response:
        """Make a POST request with timeout handling."""
        return cls.request_with_timeout("POST", url, **kwargs)

    @classmethod
    def put_with_timeout(
        cls,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response:
        """Make a PUT request with timeout handling."""
        return cls.request_with_timeout("PUT", url, **kwargs)

    @classmethod
    def request_with_timeout(
        cls, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        """Call requests.request and turn a timeout into a RequestTimedOut
        exception.
        """
        return cls._request_with_timeout(http_method, url, **kwargs)

    # The set of status codes on which a retry will be attempted (if the number of retries requested is non-zero).
    RETRY_STATUS_CODES = [429, 500, 502, 503, 504]

    @classmethod
    def _validate_kwargs(cls, kwargs: RequestKwargs) -> None:
        """Validate the kwargs passed to the request methods."""
        make_request_with = kwargs.get("make_request_with", SentinelType.NotGiven)
        if isinstance(make_request_with, RequestsSession):
            # If make_request_with is a Session, we raise an error if retry settings were provided,
            # as the Session already has its own retry settings.
            settings_present = []
            if "max_retry_count" in kwargs:
                settings_present.append("'max_retry_count'")
            if "backoff_factor" in kwargs:
                settings_present.append("'backoff_factor'")

            if settings_present:
                raise PalaceValueError(
                    f"Cannot set {', '.join(settings_present)} when 'make_request_with' is a Session."
                )

    @classmethod
    def _request_with_timeout(
        cls,
        http_method: str,
        url: str,
        *,
        process_response_with: _ProcessResponseCallable | None = None,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response:
        """Call some kind of method and turn a timeout into a RequestTimedOut
        exception.

        The core of `request_with_timeout` made easy to test.

        :param url: Make the request to this URL.
        :param kwargs: Keyword arguments for the request function.
        """
        cls._validate_kwargs(kwargs)

        process_response_with = process_response_with or raise_for_bad_response
        make_request_with: MakeRequestT = kwargs.pop(
            "make_request_with", SentinelType.NotGiven
        )

        allowed_response_codes = kwargs.pop("allowed_response_codes", [])
        disallowed_response_codes = kwargs.pop("disallowed_response_codes", [])
        verbose = kwargs.pop("verbose", False)

        if not "timeout" in kwargs:
            kwargs["timeout"] = cls.DEFAULT_REQUEST_TIMEOUT

        max_retry_count: int | None = kwargs.pop("max_retry_count", None)
        backoff_factor: float | None = kwargs.pop("backoff_factor", None)

        # Set a user-agent if not already present
        headers = get_default_headers()
        if (additional_headers := kwargs.get("headers")) is not None:
            headers.update(additional_headers)
        kwargs["headers"] = headers

        try:
            if verbose:
                logging.info(
                    f"Sending {http_method} request to {url}: kwargs {kwargs!r}"
                )

            request_start_time = time.time()
            if make_request_with is SentinelType.NotGiven:
                with cls.session(
                    max_retry_count=max_retry_count, backoff_factor=backoff_factor
                ) as session:
                    response = session.request(http_method, url, **kwargs)  # type: ignore[misc]
            elif isinstance(make_request_with, RequestsSession):
                response = make_request_with.request(http_method, url, **kwargs)  # type: ignore[misc]
            else:
                response = make_request_with(http_method, url, **kwargs)
            cls.logger().info(
                f"Request time for {url} took {time.time() - request_start_time:.2f} seconds"
            )

            if verbose:
                logging.info(
                    f"Response from {url}: {response.status_code} {response.headers!r} {response.content!r}"
                )
        except requests.exceptions.Timeout as e:
            # Wrap the requests-specific Timeout exception
            # in a generic RequestTimedOut exception.
            raise RequestTimedOut(url, str(e)) from e
        except requests.exceptions.RequestException as e:
            # Wrap all other requests-specific exceptions in
            # a generic RequestNetworkException.
            raise RequestNetworkException(url, str(e)) from e

        return process_response_with(
            url,
            response,
            allowed_response_codes,
            disallowed_response_codes,
        )

    @classmethod
    def debuggable_get(cls, url: str, **kwargs: Unpack[GetRequestKwargs]) -> Response:
        """Make a GET request that returns a detailed problem
        detail document on error.
        """
        return cls.debuggable_request("GET", url, **kwargs)

    @classmethod
    def debuggable_post(
        cls,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response:
        """Make a POST request that returns a detailed problem
        detail document on error.
        """
        return cls.debuggable_request("POST", url, **kwargs)

    @classmethod
    def debuggable_request(
        cls,
        http_method: str,
        url: str,
        **kwargs: Unpack[RequestKwargs],
    ) -> Response:
        """Make a request that raises a ProblemError with a detailed problem detail
        document on error, rather than a generic "an integration error occurred"
        message.

        :param http_method: HTTP method to use when making the request.
        :param url: Make the request to this URL.
        :param kwargs: Keyword arguments for the make_request_with
            function.
        """
        logging.info(
            "Making debuggable %s request to %s: kwargs %r", http_method, url, kwargs
        )
        return cls._request_with_timeout(
            http_method,
            url,
            process_response_with=cls.process_debuggable_response,
            **kwargs,
        )

    @classmethod
    def process_debuggable_response(
        cls,
        url: str,
        response: Response,
        allowed_response_codes: ResponseCodesTypes,
        disallowed_response_codes: ResponseCodesTypes,
    ) -> Response:
        """If there was a problem with an integration request,
        raise ProblemError with an appropriate ProblemDetail. Otherwise, return the
        response to the original request.
        """

        allowed_response_codes = allowed_response_codes or ["2xx", "3xx"]
        allowed_response_codes_str = list(map(str, allowed_response_codes))
        disallowed_response_codes = disallowed_response_codes or []
        disallowed_response_codes_str = list(map(str, disallowed_response_codes))

        code = response.status_code
        series = get_series(code)
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
