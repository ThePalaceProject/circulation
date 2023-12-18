import logging
import os
import select
import ssl
from collections import deque
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from socket import AF_INET, SOCK_STREAM, socket
from typing import Any, Deque

import pytest


class TLSServerFixture:
    """A fixture for testing connections to TLS servers."""

    _responses: Deque[bytes]
    _address: Any

    def __init__(
        self,
        context,
        server_key: str,
        server_cert: str,
        client_key: str,
        client_cert: str,
        ca_cert: str,
    ):
        self._sock = socket(family=AF_INET, type=SOCK_STREAM)
        self._executor = ThreadPoolExecutor(max_workers=3)
        self._context = context
        self._server_key = server_key
        self._server_cert = server_cert
        self._client_key = client_key
        self._client_cert = client_cert
        self._ca_cert = ca_cert
        self._tls_socket = None
        self._address = None
        self._open = True
        self._responses = deque()

    def start(self):
        logging.debug("server: bind")
        self._sock.bind(("", 0))
        self._address = self._sock.getsockname()
        logging.debug(f"server: bound {str(self.address)}")
        self._sock.listen(10)
        self._sock.settimeout(2)
        self._tls_socket = self._context.wrap_socket(self._sock, server_side=True)
        self._executor.submit(self._server_main)

    def _server_main(self):
        while self._open:
            try:
                readable, _, _ = select.select([self._tls_socket], [], [], 1.0)
                if self._tls_socket in readable:
                    (client, _) = self._tls_socket.accept()
                    logging.debug("server: client connected")
                    self._executor.submit(lambda: self._client_main(client))
            except Exception as e:
                logging.debug("server: exception: " + str(e))

    def _client_main(self, client_socket: socket):
        try:
            client_socket.recv(1)
            client_socket.send(self._responses.popleft())
        finally:
            logging.debug("server: client finished")
            client_socket.close()

    def close(self):
        logging.debug("server: closing")
        self._open = False
        self._sock.close()
        self._tls_socket.close()
        self._executor.shutdown(wait=True)
        logging.debug("server: closed")

    @property
    def address(self):
        return self._address

    @property
    def port(self) -> int:
        return self._address[1]

    @property
    def server_key_file(self) -> str:
        return self._server_key

    @property
    def server_cert_file(self) -> str:
        return self._server_cert

    @property
    def client_key_file(self) -> str:
        return self._client_key

    @property
    def client_cert_file(self) -> str:
        return self._client_cert

    @property
    def ca_cert_file(self) -> str:
        return self._ca_cert

    def enqueue_response(self, data: bytes):
        """Enqueue a response that will be returned directly to the next connecting client."""
        self._responses.append(data)


@pytest.fixture(scope="function")
def tls_server() -> Generator[TLSServerFixture, Any, Any]:
    """A TLS server that serves a valid certificate (assuming you trust the CA)"""

    base_path = Path(__file__).parent
    ca_path = os.path.join(base_path, "fake_ca")
    ca_file_cert = os.path.join(ca_path, "fake_ca.pem")
    server_file_cert = os.path.join(ca_path, "fake_server.pem")
    server_file_key = os.path.join(ca_path, "fake_server.key")
    client_file_cert = os.path.join(ca_path, "fake_client.pem")
    client_file_key = os.path.join(ca_path, "fake_client.key")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_verify_locations(cafile=ca_file_cert)
    context.load_cert_chain(certfile=server_file_cert, keyfile=server_file_key)

    server = TLSServerFixture(
        context=context,
        server_key=server_file_key,
        server_cert=server_file_cert,
        client_key=client_file_key,
        client_cert=client_file_cert,
        ca_cert=ca_file_cert,
    )
    server.start()
    yield server
    server.close()


@pytest.fixture(scope="function")
def tls_server_wrong_cert() -> Generator[TLSServerFixture, Any, Any]:
    """A TLS server that serves a certificate with the wrong hostname (not "localhost")"""

    base_path = Path(__file__).parent
    ca_path = os.path.join(base_path, "fake_ca")
    ca_file_cert = os.path.join(ca_path, "fake_ca.pem")
    server_file_cert = os.path.join(ca_path, "fake_server_wrong_cn.pem")
    server_file_key = os.path.join(ca_path, "fake_server_wrong_cn.key")
    client_file_cert = os.path.join(ca_path, "fake_client.pem")
    client_file_key = os.path.join(ca_path, "fake_client.key")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_verify_locations(cafile=ca_file_cert)
    context.load_cert_chain(certfile=server_file_cert, keyfile=server_file_key)

    server = TLSServerFixture(
        context=context,
        server_key=server_file_key,
        server_cert=server_file_cert,
        client_key=client_file_key,
        client_cert=client_file_cert,
        ca_cert=ca_file_cert,
    )
    server.start()
    yield server
    server.close()
