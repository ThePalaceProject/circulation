import pytest

from core.util.http import HTTP, RequestNetworkException
from tests.fixtures.webserver import MockAPIServer, MockAPIServerResponse


class TestMockAPIServer:
    def test_server_get(self, mock_web_server: MockAPIServer):
        mock_web_server.enqueue_response("GET", "/x/y/z", MockAPIServerResponse())

        url = mock_web_server.url("/x/y/z")
        response = HTTP.request_with_timeout("GET", url)
        assert response.status_code == 200
        assert response.content == b""

        requests = mock_web_server.requests()
        assert len(requests) == 1
        assert requests[0].path == "/x/y/z"
        assert requests[0].method == "GET"

    def test_server_post(self, mock_web_server: MockAPIServer):
        _r = MockAPIServerResponse()
        _r.status_code = 201
        _r.headers["Extra"] = "Thing"
        _r.set_content(b"DATA!")
        mock_web_server.enqueue_response("POST", "/x/y/z", _r)

        url = mock_web_server.url("/x/y/z")
        response = HTTP.request_with_timeout(
            "POST", url, data=b"DATA!", headers={"Extra": "Thing"}
        )
        assert response.status_code == 201
        assert response.content == b"DATA!"
        assert response.headers["Extra"] == "Thing"

        requests = mock_web_server.requests()
        assert len(requests) == 1
        assert requests[0].path == "/x/y/z"
        assert requests[0].method == "POST"
        assert requests[0].payload == b"DATA!"
        assert requests[0].headers["Extra"] == "Thing"

    def test_server_get_no_response(self, mock_web_server: MockAPIServer):
        url = mock_web_server.url("/x/y/z")
        with pytest.raises(RequestNetworkException):
            HTTP.request_with_timeout("GET", url, timeout=1, backoff_factor=0)

    def test_server_get_dies(self, mock_web_server: MockAPIServer):
        _r = MockAPIServerResponse()
        _r.close_obnoxiously = True
        mock_web_server.enqueue_response("GET", "/x/y/z", _r)

        url = mock_web_server.url("/x/y/z")
        with pytest.raises(RequestNetworkException):
            HTTP.request_with_timeout("GET", url, timeout=1, backoff_factor=0)
