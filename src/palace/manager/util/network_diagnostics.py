"""Network diagnostic utilities for ILS integration self-tests.

Provides granular DNS and TCP connectivity checks that produce
actionable error messages for support teams to share with vendors.
"""

from __future__ import annotations

import socket
import time
from collections.abc import Generator
from urllib.parse import urlparse

from palace.manager.core.exceptions import IntegrationException
from palace.manager.core.selftest import HasSelfTests, SelfTestResult


def check_dns_resolution(hostname: str) -> str:
    """Resolve *hostname* via DNS and return a human-readable summary.

    :param hostname: The hostname to resolve.
    :return: A string listing the resolved addresses.
    :raises IntegrationException: If DNS resolution fails, with a
        ``debug_message`` containing actionable guidance.
    """
    try:
        results = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise IntegrationException(
            f"DNS resolution failed for '{hostname}': Host not found.",
            debug_message=(
                f"The hostname '{hostname}' could not be found in DNS.\n\n"
                "Common causes:\n"
                "  1. The hostname is misspelled in the integration settings.\n"
                "  2. The DNS record does not exist.\n\n"
                "Action: Double-check the hostname for typos. "
                "Ask the ILS vendor to confirm the correct server hostname."
            ),
        ) from e

    # Deduplicate and categorize addresses.
    seen: set[str] = set()
    addresses: list[str] = []
    for family, _type, _proto, _canonname, sockaddr in results:
        ip = str(sockaddr[0])
        if ip in seen:
            continue
        seen.add(ip)
        label = "IPv6" if family == socket.AF_INET6 else "IPv4"
        addresses.append(f"{ip} ({label})")

    return f"Resolved {hostname} to: {', '.join(addresses)}"


def check_tcp_connection(host: str, port: int, timeout: float = 4) -> str:
    """Open a raw TCP connection to *host*:*port* and report the result.

    DNS is resolved first so that the resolved IP can be included in
    any error messages, helping vendors verify the correct address.

    This function does not handle DNS failures itself — it's assumed
    that :func:`check_dns_resolution` (or :func:`run_network_diagnostics`)
    was called first. If DNS fails here, the raw ``socket.gaierror``
    will propagate.

    :param host: Hostname or IP address.
    :param port: TCP port number.
    :param timeout: Connection timeout in seconds.
    :return: A success message including resolved IP and elapsed time.
    :raises IntegrationException: On connection failure, with a
        ``debug_message`` containing actionable guidance.
    """

    addr_info = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    family, sock_type, proto, _canonname, sockaddr = addr_info[0]
    resolved_ip = sockaddr[0]

    sock = socket.socket(family, sock_type, proto)
    sock.settimeout(timeout)
    start = time.monotonic()
    try:
        sock.connect(sockaddr)
        elapsed = time.monotonic() - start
        return (
            f"Successfully connected to {host} ({resolved_ip}) "
            f"on port {port} in {elapsed:.2f}s"
        )
    except ConnectionRefusedError as e:
        raise IntegrationException(
            f"Connection refused by {host} ({resolved_ip}) on port {port}.",
            debug_message=(
                f"The server at {resolved_ip} is reachable but actively refused the "
                f"connection on port {port}. This means the host is up, but nothing is "
                "listening on that port, or a firewall is actively rejecting connections.\n\n"
                "Common causes:\n"
                f"  1. The ILS service is not running or not listening on port {port}.\n"
                f"  2. A firewall REJECT rule is blocking the connection.\n"
                f"  3. The port number is wrong.\n\n"
                f"Action: Ask the ILS vendor to confirm the service is running on port {port} "
                "and that connections from Palace's IP are allowed."
            ),
        ) from e
    except TimeoutError as e:
        elapsed = time.monotonic() - start
        raise IntegrationException(
            f"Connection to {host} ({resolved_ip}) on port {port} timed out after {elapsed:.0f}s.",
            debug_message=(
                "No response was received. The connection attempt was silently ignored "
                "rather than actively refused.\n\n"
                "Common causes:\n"
                f"  1. A firewall DROP rule is silently dropping packets to port {port}.\n"
                f"  2. The host is completely offline or powered down.\n\n"
                "Action: Ask the library's IT team or ILS vendor to verify that Palace's "
                f"IP address is whitelisted in their firewall rules for port {port}. "
                "A 'connection refused' error would indicate the host is at least reachable, "
                "but a timeout means traffic is not getting through at all."
            ),
        ) from e
    except ConnectionResetError as e:
        raise IntegrationException(
            f"Connection to {host} ({resolved_ip}) on port {port} was reset.",
            debug_message=(
                "The connection was started but the remote end abruptly closed it.\n\n"
                "Common causes:\n"
                "  1. A firewall or load balancer is terminating the connection "
                "after inspection.\n"
                "  2. The ILS service crashed or is misconfigured.\n\n"
                "Action: Ask the ILS vendor to check their service logs and "
                "firewall configuration."
            ),
        ) from e
    except OSError as e:
        err = getattr(e, "errno", None)
        raise IntegrationException(
            f"Connection to {host} ({resolved_ip}) on port {port} failed: {e}",
            debug_message=(
                f"An unexpected network error occurred (errno={err}): {e}.\n\n"
                "Action: Check that the hostname and port are correct."
            ),
        ) from e

    finally:
        sock.close()


def run_network_diagnostics(host: str, port: int) -> Generator[SelfTestResult]:
    """Yield :class:`SelfTestResult` objects for DNS and TCP checks.

    Intended for direct use inside ``_run_self_tests`` implementations.
    If the DNS check fails, the TCP check is skipped (it would also fail).

    :param host: Hostname to diagnose.
    :param port: TCP port to diagnose.
    """
    dns_result = HasSelfTests.run_test(
        f"DNS Resolution ({host})",
        check_dns_resolution,
        host,
    )
    yield dns_result

    if not dns_result.success:
        return

    yield HasSelfTests.run_test(
        f"TCP Connection ({host}:{port})",
        check_tcp_connection,
        host,
        port,
    )


def run_network_diagnostics_url(url: str) -> Generator[SelfTestResult]:
    """Convenience wrapper that extracts host and port from a URL.

    Parses the URL with :func:`urllib.parse.urlparse`, infers the default
    port from the scheme (443 for https, 80 for http), then delegates to
    :func:`run_network_diagnostics`.

    :param url: The full URL to diagnose (e.g. ``https://ils.example.org/api/``).
    """
    parsed = urlparse(url)
    host = parsed.hostname
    if host is None:

        def _raise_parse_error() -> None:
            raise IntegrationException(
                f"Unable to parse hostname from URL: {url}",
                debug_message=(
                    f"The URL '{url}' could not be parsed into a valid hostname.\n\n"
                    "Action: Verify the URL in the integration settings is correct "
                    "and includes a scheme (e.g. https://)."
                ),
            )

        yield HasSelfTests.run_test(f"Parse URL ({url})", _raise_parse_error)
        return

    default_port = 443 if parsed.scheme == "https" else 80
    port = parsed.port or default_port
    yield from run_network_diagnostics(host, port)
