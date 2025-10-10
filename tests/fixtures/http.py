from __future__ import annotations

from collections import deque
from collections.abc import AsyncGenerator, Callable, Generator, Mapping
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Self, Unpack, overload
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from requests import Response

from palace.manager.util.http.http import HTTP, GetRequestKwargs, RequestKwargs
from tests.mocks.mock import MockRequestsResponse


class MockHttpClientFixture:
    def __init__(self) -> None:
        self.responses: deque[Response] = deque()
        self.requests: list[str] = []
        self.requests_args: list[RequestKwargs] = []
        self.requests_methods: list[str] = []
        self._unpatch: Callable[[], None] | None = None

    def reset_mock(self) -> None:
        self.responses = deque()
        self.requests = []
        self.requests_args = []
        self.requests_methods = []

    def stop_patch(self) -> None:
        if self._unpatch is None:
            raise RuntimeError("MockHTTPClientFixture is not currently patched.")
        self._unpatch()

    @overload
    def queue_response(
        self,
        response: MockRequestsResponse,
        /,
        *,
        index: int | None = ...,
    ) -> None: ...

    @overload
    def queue_response(
        self,
        code: int,
        /,
        *,
        media_type: str | None = ...,
        headers: dict[str, str] | None = ...,
        content: str | bytes | dict[str, Any] = ...,
        index: int | None = ...,
    ) -> None: ...

    def queue_response(
        self,
        response_or_code: int | MockRequestsResponse,
        /,
        *,
        media_type: str | None = None,
        headers: Mapping[str, str] | None = None,
        content: str | bytes | dict[str, Any] = "",
        index: int | None = None,
    ) -> None:
        """Queue a response of the type produced by HTTP.get_with_timeout."""
        if not isinstance(response_or_code, MockRequestsResponse):
            headers_dict = dict(headers) if headers else {}
            if media_type:
                headers_dict["Content-Type"] = media_type
            response = MockRequestsResponse(response_or_code, headers_dict, content)
        else:
            response = response_or_code

        if index is None:
            self.responses.append(response)
        else:
            self.responses.insert(index, response)

    def _request(self, *args: Any, **kwargs: Any) -> Response:
        return self.responses.popleft()

    def do_request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        self.requests.append(url)
        self.requests_methods.append(http_method)
        self.requests_args.append(kwargs.copy())

        # Before we switch the "make_request_with" to our mock request, we call the original validation
        # function to make sure that the kwargs pass validation as originally given.
        HTTP._validate_kwargs(kwargs)

        kwargs["make_request_with"] = self._request
        return HTTP._request_with_timeout(http_method, url, **kwargs)

    def do_get(self, url: str, **kwargs: Unpack[GetRequestKwargs]) -> Response:
        return self.do_request("GET", url, **kwargs)

    @classmethod
    @contextmanager
    def fixture(cls) -> Generator[Self]:
        fixture = cls()
        patcher = patch.object(HTTP, "request_with_timeout", fixture.do_request)
        fixture._unpatch = patcher.stop
        patcher.start()
        try:
            yield fixture
        finally:
            fixture.stop_patch()


@pytest.fixture
def http_client() -> Generator[MockHttpClientFixture]:
    """Fixture to provide a mock HTTP client for testing."""
    with MockHttpClientFixture.fixture() as mock_client:
        yield mock_client


class MockHttpxResponse(httpx.Response):
    """A mock object that simulates an HTTP response from the httpx library."""

    def __init__(
        self,
        status_code: int,
        headers: dict[str, str] | None = None,
        content: Any = None,
        url: str | None = None,
        request: httpx.Request | None = None,
    ) -> None:
        import datetime

        # Handle content encoding similar to MockRequestsResponse
        response_content: bytes
        headers_dict = dict(headers) if headers else {}

        if content is not None:
            if isinstance(content, str):
                response_content = content.encode("utf-8")
            elif isinstance(content, bytes):
                response_content = content
            elif isinstance(content, dict):
                import json

                response_content = json.dumps(content).encode("utf-8")
                if "Content-Type" not in headers_dict:
                    headers_dict["Content-Type"] = "application/json"
            else:
                import json

                response_content = json.dumps(content).encode("utf-8")
        else:
            response_content = b""

        # Create a mock request if none provided
        if request is None:
            request = httpx.Request("GET", url or "http://example.com/")

        # Initialize the parent httpx.Response
        super().__init__(
            status_code=status_code,
            headers=headers_dict,
            content=response_content,
            request=request,
        )

        # Set the _elapsed attribute to simulate a completed request
        # This allows the elapsed property to work properly
        self._elapsed = datetime.timedelta(milliseconds=100)


class MockAsyncClientFixture:
    def __init__(self) -> None:
        self.queue: deque[MockHttpxResponse | Exception] = deque()
        self.requests: list[httpx.Request] = []
        self._unpatch: Callable[[], None] | None = None

    def reset_mock(self) -> None:
        self.queue = deque()
        self.requests = []

    def stop_patch(self) -> None:
        if self._unpatch is None:
            raise RuntimeError("MockAsyncClientFixture is not currently patched.")
        self._unpatch()

    @overload
    def queue_response(
        self,
        response: MockHttpxResponse,
        /,
        *,
        index: int | None = ...,
    ) -> None: ...

    @overload
    def queue_response(
        self,
        code: int,
        /,
        *,
        media_type: str | None = ...,
        headers: dict[str, str] | None = ...,
        content: str | bytes | dict[str, Any] = ...,
        index: int | None = ...,
    ) -> None: ...

    def queue_response(
        self,
        response_or_code: int | MockHttpxResponse,
        /,
        *,
        media_type: str | None = None,
        headers: Mapping[str, str] | None = None,
        content: str | bytes | dict[str, Any] = "",
        index: int | None = None,
    ) -> None:
        """Queue a response of the type produced by AsyncClient requests."""
        if not isinstance(response_or_code, MockHttpxResponse):
            headers_dict = dict(headers) if headers else {}
            if media_type:
                headers_dict["Content-Type"] = media_type
            response = MockHttpxResponse(response_or_code, headers_dict, content)
        else:
            response = response_or_code

        if index is None:
            self.queue.append(response)
        else:
            self.queue.insert(index, response)

    def queue_exception(
        self, exception: Exception, *, index: int | None = None
    ) -> None:
        """Queue an exception to be raised by the next request."""
        if index is None:
            self.queue.append(exception)
        else:
            self.queue.insert(index, exception)

    async def _mock_transport_handle_async_request(
        self, request: httpx.Request
    ) -> httpx.Response:
        # Capture the fully processed request
        self.requests.append(request)

        # Get the next item from the queue
        item = self.queue.popleft()
        if isinstance(item, Exception):
            raise item
        else:
            return item

    @property
    def request_urls(self) -> list[str]:
        """Get list of URLs that were requested (for compatibility with sync version)."""
        return [str(req.url) for req in self.requests]

    @property
    def request_methods(self) -> list[str]:
        """Get list of HTTP methods that were used (for compatibility with sync version)."""
        return [req.method for req in self.requests]

    @classmethod
    @asynccontextmanager
    async def fixture(cls) -> AsyncGenerator[Self]:
        fixture = cls()

        # Mock the transport's handle_async_request method
        # We need to patch the actual transport implementation used by httpx
        mock_handle = AsyncMock(
            side_effect=fixture._mock_transport_handle_async_request
        )

        # Patch the HTTPTransport class method that's actually used
        patcher = patch.object(
            httpx.AsyncHTTPTransport, "handle_async_request", mock_handle
        )
        fixture._unpatch = patcher.stop
        patcher.start()

        try:
            yield fixture
        finally:
            fixture.stop_patch()


@pytest.fixture
async def async_http_client() -> AsyncGenerator[MockAsyncClientFixture]:
    """Fixture to provide a mock AsyncClient for testing."""
    async with MockAsyncClientFixture.fixture() as mock_client:
        yield mock_client
