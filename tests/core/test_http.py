import functools
from collections.abc import Callable
from dataclasses import dataclass

import pytest
import requests

from core.util.http import HTTP, RequestNetworkException
from tests.core.util.test_mock_web_server import MockAPIServer, MockAPIServerResponse


@dataclass
class TestHttpFixture:
    server: MockAPIServer
    request_with_timeout: Callable[..., requests.Response]


@pytest.fixture
def test_http_fixture(mock_web_server: MockAPIServer):
    # Make sure we don't wait for retries, as that will slow down the tests.
    request_with_timeout = functools.partial(
        HTTP.request_with_timeout, timeout=1, backoff_factor=0
    )
    return TestHttpFixture(
        server=mock_web_server, request_with_timeout=request_with_timeout
    )


class TestHTTP:
    def test_retries_unspecified(self, test_http_fixture: TestHttpFixture):
        for i in range(1, 7):
            response = MockAPIServerResponse()
            response.content = b"Ouch."
            response.status_code = 502
            test_http_fixture.server.enqueue_response("GET", "/test", response)

        with pytest.raises(RequestNetworkException):
            test_http_fixture.request_with_timeout(
                "GET", test_http_fixture.server.url("/test")
            )

        assert len(test_http_fixture.server.requests()) == 6
        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

    def test_retries_none(self, test_http_fixture: TestHttpFixture):
        response = MockAPIServerResponse()
        response.content = b"Ouch."
        response.status_code = 502

        test_http_fixture.server.enqueue_response("GET", "/test", response)
        with pytest.raises(RequestNetworkException):
            test_http_fixture.request_with_timeout(
                "GET", test_http_fixture.server.url("/test"), max_retry_count=0
            )

        assert len(test_http_fixture.server.requests()) == 1
        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

    def test_retries_3(self, test_http_fixture: TestHttpFixture):
        response0 = MockAPIServerResponse()
        response0.content = b"Ouch."
        response0.status_code = 502

        response1 = MockAPIServerResponse()
        response1.content = b"Ouch."
        response1.status_code = 502

        response2 = MockAPIServerResponse()
        response2.content = b"OK!"
        response2.status_code = 200

        test_http_fixture.server.enqueue_response("GET", "/test", response0)
        test_http_fixture.server.enqueue_response("GET", "/test", response1)
        test_http_fixture.server.enqueue_response("GET", "/test", response2)

        response = test_http_fixture.request_with_timeout(
            "GET", test_http_fixture.server.url("/test"), max_retry_count=3
        )
        assert response.status_code == 200

        assert len(test_http_fixture.server.requests()) == 3
        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

        request = test_http_fixture.server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"
