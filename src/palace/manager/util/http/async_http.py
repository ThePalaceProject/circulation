from __future__ import annotations

import asyncio
from collections.abc import AsyncIterable, Iterable, Mapping, Sequence
from types import TracebackType
from typing import IO, Any, TypedDict, Union, cast

import httpx
from typing_extensions import Self, Unpack

from palace.manager.util.backoff import exponential_backoff
from palace.manager.util.http.base import (
    ResponseCodesTypes,
    get_default_headers,
    raise_for_bad_response,
    status_code_matches,
)
from palace.manager.util.http.exception import (
    BadResponseException,
    RequestNetworkException,
    RequestTimedOut,
)
from palace.manager.util.log import LoggerMixin

# Most of these type aliases are adapted from httpx._types. I would
# prefer to import them directly, but they are not part of the public API.

PrimitiveData = str | int | float | bool | None

URLTypes = httpx.URL | str

AuthTypes = tuple[str, str] | httpx.Auth

QueryParamTypes = Union[
    httpx.QueryParams,
    Mapping[str, PrimitiveData | Sequence[PrimitiveData]],
    list[tuple[str, PrimitiveData]],
    tuple[tuple[str, PrimitiveData], ...],
    str,
]

HeaderTypes = Union[
    httpx.Headers,
    Mapping[str, str],
    Sequence[tuple[str, str]],
]

TimeoutTypes = Union[
    float,
    None,
    tuple[float | None, float | None, float | None, float | None],
    httpx.Timeout,
]

RequestContent = Union[str, bytes, Iterable[bytes], AsyncIterable[bytes]]

RequestData = Mapping[str, Any]

FileContent = Union[IO[bytes], bytes, str]
FileTypes = Union[
    # file (or bytes)
    FileContent,
    # (filename, file (or bytes))
    tuple[str | None, FileContent],
    # (filename, file (or bytes), content_type)
    tuple[str | None, FileContent, str | None],
    # (filename, file (or bytes), content_type, headers)
    tuple[str | None, FileContent, str | None, Mapping[str, str]],
]
RequestFiles = Union[Mapping[str, FileTypes], Sequence[tuple[str, FileTypes]]]


class RequestNoBodyKwargs(TypedDict, total=False):
    """
    Keyword arguments for HTTP requests that do not include a body.

    These are mostly the same as httpx, but we add a few additional options for
    retrying and response code handling.

    See https://www.python-httpx.org/api/#helper-functions for details on the standard httpx options.
    """

    params: QueryParamTypes
    """
    Query parameters to include in the URL, as a string, dictionary, or sequence of two-tuples.
    """

    headers: HeaderTypes
    """
    Dictionary of HTTP headers to include in the request.
    """

    auth: AuthTypes
    """
    An authentication class to use when sending the request.
    """

    follow_redirects: bool
    """
    Enables or disables HTTP redirects.
    """

    timeout: TimeoutTypes
    """
    The timeout configuration to use when sending the request.
    """

    # Palace specific options for request handling
    allowed_response_codes: ResponseCodesTypes
    """
    The HTTP response codes that should be allowed.  If the response code
    is not in this list, a BadResponseException will be raised. If no
    allowed_response_codes are specified, all response codes are allowed
    except those in disallowed_response_codes.
    """

    disallowed_response_codes: ResponseCodesTypes
    """
    The HTTP response codes that should be disallowed.  If the response code
    is in this list, a BadResponseException will be raised. By default, this
    includes all 5xx response codes.
    """

    no_retry_status_codes: ResponseCodesTypes
    """
    The HTTP response codes that should not trigger retries. If the response code
    is in this list, no retry will be attempted even if the response would normally
    cause a BadResponseException.
    """

    max_retries: int
    """
    The maximum number of times to retry a request if it fails due to a
    bad response code or a timeout.
    """

    backoff_factor: float
    """
    The factor to use when calculating the backoff time between retries.
    """

    max_backoff: float
    """
    The maximum backoff time, in seconds.
    """


class RequestKwargs(RequestNoBodyKwargs, total=False):
    """
    Keyword arguments for HTTP requests that include a body.

    These are mostly the same as httpx, but we add a few additional options for
    retrying and response code handling.

    See https://www.python-httpx.org/api/#helper-functions for details on the standard httpx options.
    """

    content: RequestContent
    """
    Binary content to include in the body of the request, as bytes or a byte iterator.
    """

    data: RequestData
    """
    Form data to include in the body of the request, as a dictionary.
    """

    files: RequestFiles
    """
    A dictionary of upload files to include in the body of the request.
    """

    json: Any
    """
    A JSON serializable object to include in the body of the request.
    """


class ClientKwargs(TypedDict, total=False):
    """
    The keyword arguments accepted by the httpx.AsyncClient constructor.

    See: https://www.python-httpx.org/api/#asyncclient
    """

    auth: AuthTypes
    params: QueryParamTypes
    headers: HeaderTypes
    verify: bool
    timeout: TimeoutTypes
    follow_redirects: bool
    limits: httpx.Limits
    max_redirects: int
    base_url: URLTypes


WEB_DEFAULT_TIMEOUT = httpx.Timeout(5.0, pool=None)
WEB_DEFAULT_MAX_REDIRECTS = 2
WEB_DEFAULT_MAX_RETRIES = 0
WEB_DEFAULT_BACKOFF_FACTOR = 0
WEB_DEFAULT_MAX_BACKOFF = 0

WORKER_DEFAULT_TIMEOUT = httpx.Timeout(20.0, pool=None)
WORKER_DEFAULT_MAX_REDIRECTS = 20
WORKER_DEFAULT_MAX_RETRIES = 3
WORKER_DEFAULT_BACKOFF_FACTOR = 1
WORKER_DEFAULT_MAX_BACKOFF = 45

DEFAULT_LIMITS = httpx.Limits(max_connections=10, max_keepalive_connections=None)


class AsyncClient(LoggerMixin):
    """
    An asynchronous HTTP client, with connection pooling, HTTP/2, redirects,
    cookie persistence, etc.

    This is just a thin wrapper around `httpx.AsyncClient`, with some
    additional functionality for logging, default headers, and error handling.
    """

    def __init__(
        self,
        *,
        client: httpx.AsyncClient,
        allowed_response_codes: ResponseCodesTypes | None = None,
        disallowed_response_codes: ResponseCodesTypes | None = None,
        no_retry_status_codes: ResponseCodesTypes | None = None,
        max_retries: int = 0,
        backoff_factor: float = 0,
        max_backoff: float = 0,
    ) -> None:
        """
        Initialize the AsyncClient.

        :param client:
            The underlying httpx.AsyncClient instance to use for making requests.
        :param allowed_response_codes:
            The default value for allowed_response_codes for requests made with this client. This will be
            used if the allowed_response_codes parameter is not provided to the request method.
        :param disallowed_response_codes:
            The default value for disallowed_response_codes for requests made with this client. This will be
            used if the disallowed_response_codes parameter is not provided to the request method.
        :param no_retry_status_codes:
            The default value for no_retry_status_codes for requests made with this client. This will be
            used if the no_retry_status_codes parameter is not provided to the request method.
            These status codes will not trigger retries even if they would normally cause a BadResponseException.
        :param max_retries:
            The default value for max_retries for requests made with this client. This will be
            used if the max_retries parameter is not provided to the request method.
        :param backoff_factor:
            The default value for backoff_factor for requests made with this client. This will be
            used if the backoff_factor parameter is not provided to the request method.
        :param max_backoff:
            The default value for max_backoff for requests made with this client. This will be
            used if the max_backoff parameter is not provided to the request method.
        """

        self._httpx_client = client

        self._allowed_response_codes = allowed_response_codes or []
        self._disallowed_response_codes = disallowed_response_codes or []
        self._no_retry_status_codes = no_retry_status_codes or []
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._max_backoff = max_backoff

    @staticmethod
    def _defaults(kwargs: ClientKwargs) -> None:
        """
        Sets our global defaults for httpx.AsyncClient parameters.

        Modifies the passed in kwargs in place.
        """

        # We can't use setdefault here, because we need to merge headers
        # Set a user-agent if not already present
        headers = get_default_headers()
        if "headers" in kwargs:
            headers.update(kwargs["headers"])
        kwargs["headers"] = headers

        kwargs.setdefault("verify", True)
        kwargs.setdefault("limits", DEFAULT_LIMITS)
        kwargs.setdefault("follow_redirects", True)

    @classmethod
    def for_web(
        cls,
        *,
        allowed_response_codes: ResponseCodesTypes | None = None,
        disallowed_response_codes: ResponseCodesTypes | None = None,
        no_retry_status_codes: ResponseCodesTypes | None = None,
        **kwargs: Unpack[ClientKwargs],
    ) -> Self:
        """
        Create an `AsyncClient` with settings suitable for general web requests.

        This means that timeouts are relatively short, redirects are limited,
        and retries are disabled by default.
        """
        cls._defaults(kwargs)
        kwargs.setdefault("timeout", WEB_DEFAULT_TIMEOUT)
        kwargs.setdefault("max_redirects", WEB_DEFAULT_MAX_REDIRECTS)
        return cls(
            client=httpx.AsyncClient(**kwargs),
            allowed_response_codes=allowed_response_codes,
            disallowed_response_codes=disallowed_response_codes,
            no_retry_status_codes=no_retry_status_codes,
            max_retries=WEB_DEFAULT_MAX_RETRIES,
            backoff_factor=WEB_DEFAULT_BACKOFF_FACTOR,
            max_backoff=WEB_DEFAULT_MAX_BACKOFF,
        )

    @classmethod
    def for_worker(
        cls,
        *,
        allowed_response_codes: ResponseCodesTypes | None = None,
        disallowed_response_codes: ResponseCodesTypes | None = None,
        no_retry_status_codes: ResponseCodesTypes | None = None,
        **kwargs: Unpack[ClientKwargs],
    ) -> Self:
        """
        Create an `AsyncClient` with settings suitable for background worker tasks.

        This means that timeouts are longer, redirects are more permissive,
        and retries are enabled by default.
        """
        cls._defaults(kwargs)
        kwargs.setdefault("timeout", WORKER_DEFAULT_TIMEOUT)
        kwargs.setdefault("max_redirects", WORKER_DEFAULT_MAX_REDIRECTS)
        return cls(
            client=httpx.AsyncClient(**kwargs),
            allowed_response_codes=allowed_response_codes,
            disallowed_response_codes=disallowed_response_codes,
            no_retry_status_codes=no_retry_status_codes,
            max_retries=WORKER_DEFAULT_MAX_RETRIES,
            backoff_factor=WORKER_DEFAULT_BACKOFF_FACTOR,
            max_backoff=WORKER_DEFAULT_MAX_BACKOFF,
        )

    async def _perform_request(
        self,
        method: str,
        url: URLTypes,
        *,
        allowed_response_codes: ResponseCodesTypes,
        disallowed_response_codes: ResponseCodesTypes,
        **kwargs: Any,
    ) -> httpx.Response:
        """
        Perform a single HTTP request, handling exceptions and logging, without retries.
        """
        try:
            response = await self._httpx_client.request(method, url, **kwargs)
            self.log.info(
                f"Request time for {url} took {response.elapsed.total_seconds():.2f} seconds"
            )

        except httpx.TimeoutException as e:
            # Wrap the httpx-specific Timeout exception in a generic RequestTimedOut exception.
            raise RequestTimedOut(str(url), str(e)) from e
        except httpx.RequestError as e:
            # Wrap all other httpx-specific exceptions in a generic RequestNetworkException.
            raise RequestNetworkException(str(url), str(e)) from e

        return raise_for_bad_response(
            url, response, allowed_response_codes, disallowed_response_codes
        )

    async def request(
        self,
        method: str,
        url: URLTypes,
        **kwargs: Unpack[RequestKwargs],
    ) -> httpx.Response:
        """
        Make an HTTP request, with retries on failure.
        """

        allowed_response_codes = kwargs.pop(
            "allowed_response_codes", self._allowed_response_codes
        )
        disallowed_response_codes = kwargs.pop(
            "disallowed_response_codes", self._disallowed_response_codes
        )
        no_retry_status_codes = kwargs.pop(
            "no_retry_status_codes", self._no_retry_status_codes
        )
        max_retries = kwargs.pop("max_retries", self._max_retries)
        backoff_factor = kwargs.pop("backoff_factor", self._backoff_factor)
        max_backoff = kwargs.pop("max_backoff", self._max_backoff)

        attempt = 0
        while True:
            try:
                return await self._perform_request(
                    method,
                    url,
                    allowed_response_codes=allowed_response_codes,
                    disallowed_response_codes=disallowed_response_codes,
                    # Mypy doesn't understand that we're popping known keys from kwargs
                    # and that the rest are valid httpx request parameters, so we need
                    # to do a cast here.
                    **cast(Any, kwargs),
                )
            except (BadResponseException, RequestTimedOut) as e:
                # Check if this is a BadResponseException with a status code we shouldn't retry
                should_retry = True
                if isinstance(e, BadResponseException) and e.response:
                    if status_code_matches(
                        e.response.status_code, no_retry_status_codes
                    ):
                        should_retry = False

                if not should_retry or attempt >= max_retries:
                    # Update the retry count before re-raising
                    e.retry_count = attempt
                    raise e

                # Calculate backoff time
                delay = exponential_backoff(
                    attempt, factor=backoff_factor, max_time=max_backoff
                )
                attempt += 1
                self.log.warning(
                    f"Request to {url} failed ({e}). "
                    f"Retrying in {delay:.2f}s... (attempt {attempt}/{max_retries})"
                )

                # Wait before retrying
                await asyncio.sleep(delay)

    async def get(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestNoBodyKwargs],
    ) -> httpx.Response:
        return await self.request("GET", url, **kwargs)

    async def options(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestNoBodyKwargs],
    ) -> httpx.Response:
        return await self.request("OPTIONS", url, **kwargs)

    async def head(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestNoBodyKwargs],
    ) -> httpx.Response:
        return await self.request("HEAD", url, **kwargs)

    async def post(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestKwargs],
    ) -> httpx.Response:
        return await self.request("POST", url, **kwargs)

    async def put(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestKwargs],
    ) -> httpx.Response:
        return await self.request("PUT", url, **kwargs)

    async def patch(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestKwargs],
    ) -> httpx.Response:
        return await self.request("PATCH", url, **kwargs)

    async def delete(
        self,
        url: URLTypes,
        **kwargs: Unpack[RequestNoBodyKwargs],
    ) -> httpx.Response:
        return await self.request("DELETE", url, **kwargs)

    async def aclose(self) -> None:
        await self._httpx_client.aclose()

    async def __aenter__(self) -> Self:
        await self._httpx_client.__aenter__()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> None:
        return await self._httpx_client.__aexit__(exc_type, exc_value, traceback)
