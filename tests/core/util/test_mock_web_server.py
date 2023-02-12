import logging
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Dict, List, Optional, Tuple

import pytest

from palace.core.util.http import HTTP, RequestNetworkException


class MockAPIServerRequest:
    """A request made to a server."""

    headers: Dict[str, str]
    payload: bytes
    method: str
    path: str

    def __init__(self):
        self.headers = {}
        self.payload = b""
        self.method = "GET"
        self.path = "/"


class MockAPIServerResponse:
    """A response returned from a server."""

    status_code: int
    content: bytes
    headers: Dict[str, str]
    close_obnoxiously: bool

    def __init__(self):
        self.status_code = 200
        self.content = b""
        self.headers = {}
        self.close_obnoxiously = False

    def set_content(self, data: bytes):
        """A convenience method that automatically sets the correct content length for data."""
        self.content = data
        self.headers["content-length"] = str(len(data))


class MockAPIServerRequestHandler(BaseHTTPRequestHandler):
    """Basic request handler."""

    def _send_everything(self, _response: MockAPIServerResponse):
        if _response.close_obnoxiously:
            return

        self.send_response(_response.status_code)
        for key in _response.headers.keys():
            _value = _response.headers.get(key)
            if _value:
                self.send_header(key, _value)

        self.end_headers()
        self.wfile.write(_response.content)
        self.wfile.flush()

    def _read_everything(self) -> MockAPIServerRequest:
        _request = MockAPIServerRequest()
        _request.method = self.command
        for k in self.headers.keys():
            _request.headers[k] = self.headers.get(k)
        _request.path = self.path
        _readable = int(self.headers.get("Content-Length") or 0)
        if _readable > 0:
            _request.payload = self.rfile.read(_readable)
        return _request

    def _handle_everything(self):
        _request = self._read_everything()
        _response = self.server.mock_api_server.dequeue_response(_request)
        if _response is None:
            logging.error(
                f"failed to find a response for {_request.method} {_request.path}"
            )
            raise AssertionError(
                f"No available response for {_request.method} {_request.path}!"
            )
        self._send_everything(_response)

    def do_GET(self):
        logging.info("GET")
        self._handle_everything()

    def do_POST(self):
        logging.info("POST")
        self._handle_everything()

    def do_PUT(self):
        logging.info("PUT")
        self._handle_everything()

    def version_string(self) -> str:
        return ""

    def date_time_string(self, timestamp: Optional[int] = 0) -> str:
        return "Sat, 1 January 2000 00:00:00 UTC"


class MockAPIInternalServer(HTTPServer):
    mock_api_server: "MockAPIServer"

    def __init__(self, server_address: Tuple[str, int], bind_and_activate: bool):
        super().__init__(server_address, MockAPIServerRequestHandler, bind_and_activate)
        self.allow_reuse_address = True


class MockAPIServer:
    """Embedded web server."""

    _address: str
    _port: int
    _server: HTTPServer
    _server_thread: threading.Thread
    _responses: Dict[str, Dict[str, List[MockAPIServerResponse]]]
    _requests: List[MockAPIServerRequest]

    def __init__(self, address: str, port: int):
        self._address = address
        self._port = port
        self._server = MockAPIInternalServer(
            (self._address, self._port), bind_and_activate=True
        )
        self._server.mock_api_server = self
        self._server_thread = threading.Thread(target=self._server.serve_forever)
        self._responses = {}
        self._requests = []

    def start(self) -> None:
        self._server_thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._server_thread.join(timeout=10)

    def enqueue_response(
        self, request_method: str, request_path: str, response: MockAPIServerResponse
    ):
        _by_method = self._responses.get(request_method) or {}
        _by_path = _by_method.get(request_path) or []
        _by_path.append(response)
        _by_method[request_path] = _by_path
        self._responses[request_method] = _by_method

    def dequeue_response(
        self, request: MockAPIServerRequest
    ) -> Optional[MockAPIServerResponse]:
        self._requests.append(request)
        _by_method = self._responses.get(request.method) or {}
        _by_path = _by_method.get(request.path) or []
        if len(_by_path) > 0:
            return _by_path.pop(0)
        return None

    def address(self) -> str:
        return self._address

    def port(self) -> int:
        return self._port

    def url(self, path: str) -> str:
        return f"http://{self.address()}:{self.port()}{path}"

    def requests(self) -> List[MockAPIServerRequest]:
        return list(self._requests)


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
        try:
            HTTP.request_with_timeout("GET", url)
        except RequestNetworkException:
            return
        raise AssertionError("Failed to fail!")

    def test_server_get_dies(self, mock_web_server: MockAPIServer):
        _r = MockAPIServerResponse()
        _r.close_obnoxiously = True
        mock_web_server.enqueue_response("GET", "/x/y/z", _r)

        url = mock_web_server.url("/x/y/z")
        try:
            HTTP.request_with_timeout("GET", url)
        except RequestNetworkException:
            return
        raise AssertionError("Failed to fail!")
