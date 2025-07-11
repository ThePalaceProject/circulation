from __future__ import annotations

from collections.abc import Callable, Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import patch

import pytest
from requests import Response
from typing_extensions import Self, Unpack, overload

from palace.manager.util.http import HTTP, GetRequestKwargs, RequestKwargs
from tests.mocks.mock import MockRequestsResponse


class MockHttpClientFixture:
    def __init__(self) -> None:
        self.responses: list[Response] = []
        self.requests: list[str] = []
        self.requests_args: list[RequestKwargs] = []
        self.requests_methods: list[str] = []
        self._unpatch: Callable[[], None] | None = None

    def reset_mock(self) -> None:
        self.responses = []
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
    ) -> None: ...

    @overload
    def queue_response(
        self,
        code: int,
        /,
        media_type: str | None = ...,
        other_headers: dict[str, str] | None = ...,
        content: str | bytes | dict[str, Any] = ...,
    ) -> None: ...

    def queue_response(
        self,
        response_or_code: int | MockRequestsResponse,
        /,
        media_type: str | None = None,
        other_headers: dict[str, str] | None = None,
        content: str | bytes | dict[str, Any] = "",
    ) -> None:
        """Queue a response of the type produced by HTTP.get_with_timeout."""
        if not isinstance(response_or_code, MockRequestsResponse):
            headers = dict(other_headers) if other_headers else {}
            if media_type:
                headers["Content-Type"] = media_type
            response = MockRequestsResponse(response_or_code, headers, content)
        else:
            response = response_or_code

        self.responses.append(response)

    def _request(self, *args: Any, **kwargs: Any) -> Response:
        return self.responses.pop(0)

    def do_request(
        self, http_method: str, url: str, **kwargs: Unpack[RequestKwargs]
    ) -> Response:
        self.requests.append(url)
        self.requests_methods.append(http_method)
        self.requests_args.append(kwargs)
        return HTTP._request_with_timeout(http_method, url, self._request, **kwargs)

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
