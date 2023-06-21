import socket
import ssl

import pytest

from tests.fixtures.tls_server import TLSServerFixture


class TestTLSServerFixture:
    def test_connect_ok(self, tls_server: TLSServerFixture):
        """Connecting to a server that serves a certificate we trust, works."""
        expected_data = bytes("Hello.", "UTF-8")
        tls_server.enqueue_response(expected_data)

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cafile=tls_server.ca_cert_file)

        with socket.socket() as sock:
            ssock = context.wrap_socket(
                sock, server_side=False, server_hostname="localhost"
            )
            ssock.connect(tls_server.address)
            ssock.send(bytes("x", "UTF-8"))
            data = ssock.recv(1024)
            assert data == expected_data

    def test_connect_untrusted(self, tls_server: TLSServerFixture):
        """Connecting to a server that serves a certificate we don't trust, fails."""

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with socket.socket() as sock:
            ssock = context.wrap_socket(
                sock, server_side=False, server_hostname="localhost"
            )

            with pytest.raises(ssl.SSLCertVerificationError):
                ssock.connect(tls_server.address)

    def test_connect_untrusted_ignore(self, tls_server: TLSServerFixture):
        """Connecting to a server that serves a certificate we don't trust, works if we don't check."""
        expected_data = bytes("Hello.", "UTF-8")
        tls_server.enqueue_response(expected_data)

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with socket.socket() as sock:
            ssock = context.wrap_socket(
                sock, server_side=False, server_hostname="localhost"
            )
            ssock.connect(tls_server.address)
            ssock.send(bytes("x", "UTF-8"))
            data = ssock.recv(1024)
            assert data == expected_data

    def test_connect_wrong_cert(self, tls_server_wrong_cert: TLSServerFixture):
        """Connecting to a server that serves an invalid certificate, fails."""
        expected_data = bytes("Hello.", "UTF-8")
        tls_server_wrong_cert.enqueue_response(expected_data)

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cafile=tls_server_wrong_cert.ca_cert_file)

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        with socket.socket() as sock:
            ssock = context.wrap_socket(
                sock, server_side=False, server_hostname="localhost"
            )

            with pytest.raises(ssl.SSLCertVerificationError):
                ssock.connect(tls_server_wrong_cert.address)

    def test_connect_wrong_cert_ignore(self, tls_server_wrong_cert: TLSServerFixture):
        """Connecting to a server that serves an invalid certificate, works if we don't check."""
        expected_data = bytes("Hello.", "UTF-8")
        tls_server_wrong_cert.enqueue_response(expected_data)

        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE

        with socket.socket() as sock:
            ssock = context.wrap_socket(
                sock, server_side=False, server_hostname="localhost"
            )
            ssock.connect(tls_server_wrong_cert.address)
            ssock.send(bytes("x", "UTF-8"))
            data = ssock.recv(1024)
            assert data == expected_data
