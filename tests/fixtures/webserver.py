import threading
from collections.abc import Generator
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from core.util.log import LoggerMixin


class MockAPIServerRequest:
    """A request made to a server."""

    headers: dict[str, str]
    payload: bytes
    method: str
    path: str

    def __init__(self) -> None:
        self.headers = {}
        self.payload = b""
        self.method = "GET"
        self.path = "/"


class MockAPIServerResponse:
    """A response returned from a server."""

    status_code: int
    content: bytes
    headers: dict[str, str]
    close_obnoxiously: bool

    def __init__(self) -> None:
        self.status_code = 200
        self.content = b""
        self.headers = {}
        self.close_obnoxiously = False

    def set_content(self, data: bytes) -> None:
        """A convenience method that automatically sets the correct content length for data."""
        self.content = data
        self.headers["content-length"] = str(len(data))


class MockAPIServerRequestHandler(BaseHTTPRequestHandler, LoggerMixin):
    """Basic request handler."""

    def _send_everything(self, _response: MockAPIServerResponse) -> None:
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
            header = self.headers.get(k, None)
            if header is not None:
                _request.headers[k] = header
        _request.path = self.path
        _readable = int(self.headers.get("Content-Length") or 0)
        if _readable > 0:
            _request.payload = self.rfile.read(_readable)
        return _request

    def _handle_everything(self) -> None:
        _request = self._read_everything()
        assert isinstance(self.server, MockAPIInternalServer)
        _response = self.server.mock_api_server.dequeue_response(_request)
        if _response is None:
            self.log.error(
                f"failed to find a response for {_request.method} {_request.path}"
            )
            raise AssertionError(
                f"No available response for {_request.method} {_request.path}!"
            )
        self._send_everything(_response)

    def do_GET(self) -> None:
        self.log.info("GET")
        self._handle_everything()

    def do_POST(self) -> None:
        self.log.info("POST")
        self._handle_everything()

    def do_PUT(self) -> None:
        self.log.info("PUT")
        self._handle_everything()

    def version_string(self) -> str:
        return ""

    def date_time_string(self, timestamp: int | None = 0) -> str:
        return "Sat, 1 January 2000 00:00:00 UTC"


class MockAPIInternalServer(HTTPServer):
    mock_api_server: "MockAPIServer"

    def __init__(self, server_address: tuple[str, int], bind_and_activate: bool):
        super().__init__(server_address, MockAPIServerRequestHandler, bind_and_activate)
        self.allow_reuse_address = True


class MockAPIServer(LoggerMixin):
    """Embedded web server."""

    _address: str
    _port: int
    _server: HTTPServer
    _server_thread: threading.Thread
    _responses: dict[str, dict[str, list[MockAPIServerResponse]]]
    _requests: list[MockAPIServerRequest]

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
        self.log.info(f"starting mock web server on {self.address()}:{self.port()}")
        self._server_thread.start()

    def stop(self) -> None:
        self.log.info(
            f"shutting down mock web server on {self.address()}:{self.port()}"
        )
        self._server.shutdown()
        self._server.server_close()
        self._server_thread.join(timeout=10)

    def enqueue_response(
        self, request_method: str, request_path: str, response: MockAPIServerResponse
    ) -> None:
        _by_method = self._responses.get(request_method) or {}
        _by_path = _by_method.get(request_path) or []
        _by_path.append(response)
        _by_method[request_path] = _by_path
        self._responses[request_method] = _by_method

    def dequeue_response(
        self, request: MockAPIServerRequest
    ) -> MockAPIServerResponse | None:
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

    def requests(self) -> list[MockAPIServerRequest]:
        return list(self._requests)


@pytest.fixture
def mock_web_server() -> Generator[MockAPIServer, None, None]:
    """A test fixture that yields a usable mock web server for the lifetime of the test."""
    _server = MockAPIServer("127.0.0.1", 10256)
    _server.start()
    yield _server
    _server.stop()
