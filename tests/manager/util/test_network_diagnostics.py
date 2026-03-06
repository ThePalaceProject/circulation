"""Tests for palace.manager.util.network_diagnostics."""

from __future__ import annotations

import socket
from unittest.mock import MagicMock, patch

import pytest

from palace.manager.core.exceptions import IntegrationException
from palace.manager.util.network_diagnostics import (
    check_dns_resolution,
    check_tcp_connection,
    run_network_diagnostics,
    run_network_diagnostics_url,
)


class TestCheckDnsResolution:
    def test_success_ipv4(self) -> None:
        fake_results = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
        ]
        with patch(
            "palace.manager.util.network_diagnostics.socket.getaddrinfo",
            return_value=fake_results,
        ):
            result = check_dns_resolution("example.com")
        assert "Resolved example.com to:" in result
        assert "93.184.216.34 (IPv4)" in result

    def test_success_multiple_addresses(self) -> None:
        fake_results = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0)),
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                6,
                "",
                ("2001:db8::1", 0, 0, 0),
            ),
            # Duplicate IPv4 entry should be deduplicated.
            (socket.AF_INET, socket.SOCK_DGRAM, 17, "", ("93.184.216.34", 0)),
        ]
        with patch(
            "palace.manager.util.network_diagnostics.socket.getaddrinfo",
            return_value=fake_results,
        ):
            result = check_dns_resolution("example.com")
        assert "93.184.216.34 (IPv4)" in result
        assert "2001:db8::1 (IPv6)" in result
        # Only two unique addresses.
        assert result.count("(IPv") == 2

    def test_gaierror_raises_integration_exception(self) -> None:
        with patch(
            "palace.manager.util.network_diagnostics.socket.getaddrinfo",
            side_effect=socket.gaierror(socket.EAI_NONAME, "Name not found"),
        ):
            with pytest.raises(IntegrationException) as exc_info:
                check_dns_resolution("bad.example.com")
            assert "Host not found" in str(exc_info.value)
            assert exc_info.value.debug_message is not None
            assert "misspelled" in exc_info.value.debug_message


class TestCheckTcpConnection:
    """Tests for check_tcp_connection."""

    FAKE_ADDR_INFO = [
        (
            socket.AF_INET,
            socket.SOCK_STREAM,
            6,
            "",
            ("93.184.216.34", 6010),
        )
    ]

    def test_success(self) -> None:
        mock_sock = MagicMock()
        with (
            patch(
                "palace.manager.util.network_diagnostics.socket.getaddrinfo",
                return_value=self.FAKE_ADDR_INFO,
            ),
            patch(
                "palace.manager.util.network_diagnostics.socket.socket",
                return_value=mock_sock,
            ),
        ):
            result = check_tcp_connection("example.com", 6010)
        assert "Successfully connected" in result
        assert "93.184.216.34" in result
        assert "6010" in result
        mock_sock.connect.assert_called_once()
        mock_sock.close.assert_called_once()

    def test_connection_refused(self) -> None:
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = ConnectionRefusedError("Connection refused")
        with (
            patch(
                "palace.manager.util.network_diagnostics.socket.getaddrinfo",
                return_value=self.FAKE_ADDR_INFO,
            ),
            patch(
                "palace.manager.util.network_diagnostics.socket.socket",
                return_value=mock_sock,
            ),
        ):
            with pytest.raises(IntegrationException) as exc_info:
                check_tcp_connection("example.com", 6010)
            assert "Connection refused" in str(exc_info.value)
            assert exc_info.value.debug_message is not None
            assert "actively refused" in exc_info.value.debug_message
        mock_sock.close.assert_called_once()

    def test_timeout(self) -> None:
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = TimeoutError("timed out")
        with (
            patch(
                "palace.manager.util.network_diagnostics.socket.getaddrinfo",
                return_value=self.FAKE_ADDR_INFO,
            ),
            patch(
                "palace.manager.util.network_diagnostics.socket.socket",
                return_value=mock_sock,
            ),
        ):
            with pytest.raises(IntegrationException) as exc_info:
                check_tcp_connection("example.com", 6010)
            assert "timed out" in str(exc_info.value)
            assert exc_info.value.debug_message is not None
            assert "silently" in exc_info.value.debug_message
        mock_sock.close.assert_called_once()

    def test_connection_reset(self) -> None:
        mock_sock = MagicMock()
        mock_sock.connect.side_effect = ConnectionResetError("reset")
        with (
            patch(
                "palace.manager.util.network_diagnostics.socket.getaddrinfo",
                return_value=self.FAKE_ADDR_INFO,
            ),
            patch(
                "palace.manager.util.network_diagnostics.socket.socket",
                return_value=mock_sock,
            ),
        ):
            with pytest.raises(IntegrationException) as exc_info:
                check_tcp_connection("example.com", 6010)
            assert "was reset" in str(exc_info.value)
            assert exc_info.value.debug_message is not None
            assert "abruptly closed" in exc_info.value.debug_message
        mock_sock.close.assert_called_once()

    def test_other_os_error(self) -> None:
        mock_sock = MagicMock()
        err = OSError("Something else")
        err.errno = 999
        mock_sock.connect.side_effect = err
        with (
            patch(
                "palace.manager.util.network_diagnostics.socket.getaddrinfo",
                return_value=self.FAKE_ADDR_INFO,
            ),
            patch(
                "palace.manager.util.network_diagnostics.socket.socket",
                return_value=mock_sock,
            ),
        ):
            with pytest.raises(IntegrationException) as exc_info:
                check_tcp_connection("example.com", 6010)
            assert "failed" in str(exc_info.value)
            assert exc_info.value.debug_message is not None
            assert "errno=999" in exc_info.value.debug_message
        mock_sock.close.assert_called_once()


class TestRunNetworkDiagnostics:
    """Tests for the run_network_diagnostics generator helper."""

    def test_both_succeed(self) -> None:
        with (
            patch(
                "palace.manager.util.network_diagnostics.check_dns_resolution",
                return_value="Resolved host to: 1.2.3.4 (IPv4)",
            ),
            patch(
                "palace.manager.util.network_diagnostics.check_tcp_connection",
                return_value="Successfully connected to host (1.2.3.4) on port 80 in 0.01s",
            ),
        ):
            results = list(run_network_diagnostics("host", 80))
        assert len(results) == 2
        assert results[0].name == "DNS Resolution (host)"
        assert results[0].success is True
        assert results[1].name == "TCP Connection (host:80)"
        assert results[1].success is True

    def test_dns_fails_skips_tcp(self) -> None:
        with patch(
            "palace.manager.util.network_diagnostics.check_dns_resolution",
            side_effect=IntegrationException("DNS failed", "help text"),
        ):
            results = list(run_network_diagnostics("badhost", 6010))
        assert len(results) == 1
        assert results[0].name == "DNS Resolution (badhost)"
        assert results[0].success is False
        assert results[0].exception is not None

    def test_dns_succeeds_tcp_fails(self) -> None:
        with (
            patch(
                "palace.manager.util.network_diagnostics.check_dns_resolution",
                return_value="Resolved host to: 1.2.3.4 (IPv4)",
            ),
            patch(
                "palace.manager.util.network_diagnostics.check_tcp_connection",
                side_effect=IntegrationException("Connection refused", "help text"),
            ),
        ):
            results = list(run_network_diagnostics("host", 6010))
        assert len(results) == 2
        assert results[0].success is True
        assert results[1].name == "TCP Connection (host:6010)"
        assert results[1].success is False
        assert results[1].exception is not None


class TestRunNetworkDiagnosticsUrl:
    """Tests for the run_network_diagnostics_url convenience wrapper."""

    def test_https_url_default_port(self) -> None:
        with patch(
            "palace.manager.util.network_diagnostics.run_network_diagnostics"
        ) as mock_diag:
            mock_diag.return_value = iter([])
            list(run_network_diagnostics_url("https://ils.example.org/api/"))
        mock_diag.assert_called_once_with("ils.example.org", 443)

    def test_http_url_default_port(self) -> None:
        with patch(
            "palace.manager.util.network_diagnostics.run_network_diagnostics"
        ) as mock_diag:
            mock_diag.return_value = iter([])
            list(run_network_diagnostics_url("http://ils.example.org/sirsi/"))
        mock_diag.assert_called_once_with("ils.example.org", 80)

    def test_explicit_port(self) -> None:
        with patch(
            "palace.manager.util.network_diagnostics.run_network_diagnostics"
        ) as mock_diag:
            mock_diag.return_value = iter([])
            list(run_network_diagnostics_url("https://ils.example.org:8443/api/"))
        mock_diag.assert_called_once_with("ils.example.org", 8443)

    def test_unparseable_url_yields_failure(self) -> None:
        results = list(run_network_diagnostics_url("not-a-url"))
        assert len(results) == 1
        assert results[0].success is False
        assert results[0].exception is not None
        assert "Unable to parse hostname" in str(results[0].exception)

    def test_empty_string_yields_failure(self) -> None:
        results = list(run_network_diagnostics_url(""))
        assert len(results) == 1
        assert results[0].success is False
