import logging

import pytest

from core.util.http import HTTP, RequestNetworkException
from tests.core.util.test_mock_web_server import MockAPIServer, MockAPIServerResponse


@pytest.fixture
def mock_web_server():
    """A test fixture that yields a usable mock web server for the lifetime of the test."""
    _server = MockAPIServer("127.0.0.1", 10256)
    _server.start()
    logging.info(f"starting mock web server on {_server.address()}:{_server.port()}")
    yield _server
    logging.info(
        f"shutting down mock web server on {_server.address()}:{_server.port()}"
    )
    _server.stop()


class TestHTTP:
    def test_retries_unspecified(self, mock_web_server: MockAPIServer):
        for i in range(1, 7):
            response = MockAPIServerResponse()
            response.content = b"Ouch."
            response.status_code = 502
            mock_web_server.enqueue_response("GET", "/test", response)

        with pytest.raises(RequestNetworkException):
            HTTP.request_with_timeout("GET", mock_web_server.url("/test"))

        assert len(mock_web_server.requests()) == 6
        request = mock_web_server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

    def test_retries_none(self, mock_web_server: MockAPIServer):
        response = MockAPIServerResponse()
        response.content = b"Ouch."
        response.status_code = 502

        mock_web_server.enqueue_response("GET", "/test", response)
        with pytest.raises(RequestNetworkException):
            HTTP.request_with_timeout(
                "GET", mock_web_server.url("/test"), max_retry_count=0
            )

        assert len(mock_web_server.requests()) == 1
        request = mock_web_server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

    def test_retries_3(self, mock_web_server: MockAPIServer):
        response0 = MockAPIServerResponse()
        response0.content = b"Ouch."
        response0.status_code = 502

        response1 = MockAPIServerResponse()
        response1.content = b"Ouch."
        response1.status_code = 502

        response2 = MockAPIServerResponse()
        response2.content = b"OK!"
        response2.status_code = 200

        mock_web_server.enqueue_response("GET", "/test", response0)
        mock_web_server.enqueue_response("GET", "/test", response1)
        mock_web_server.enqueue_response("GET", "/test", response2)

        response = HTTP.request_with_timeout(
            "GET", mock_web_server.url("/test"), max_retry_count=3
        )
        assert response.status_code == 200

        assert len(mock_web_server.requests()) == 3
        request = mock_web_server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

        request = mock_web_server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"

        request = mock_web_server.requests().pop()
        assert request.path == "/test"
        assert request.method == "GET"
