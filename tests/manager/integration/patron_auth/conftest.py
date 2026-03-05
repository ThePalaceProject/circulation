from collections.abc import Generator

from palace.manager.core.selftest import SelfTestResult


def mock_network_diagnostics(host: str, port: int) -> Generator[SelfTestResult]:
    """Yield two successful SelfTestResult objects to stand in for network diagnostics."""
    dns = SelfTestResult(f"DNS Resolution ({host})")
    dns.success = True
    dns.result = f"Resolved {host} to: 1.2.3.4 (IPv4)"
    dns.end = dns.start
    yield dns
    tcp = SelfTestResult(f"TCP Connection ({host}:{port})")
    tcp.success = True
    tcp.result = f"Successfully connected to {host} (1.2.3.4) on port {port} in 0.01s"
    tcp.end = tcp.start
    yield tcp


def mock_network_diagnostics_url(url: str) -> Generator[SelfTestResult]:
    """Yield two successful SelfTestResult objects to stand in for network diagnostics."""
    dns = SelfTestResult("DNS Resolution (mock)")
    dns.success = True
    dns.result = "Resolved mock to: 1.2.3.4 (IPv4)"
    dns.end = dns.start
    yield dns
    tcp = SelfTestResult("TCP Connection (mock:80)")
    tcp.success = True
    tcp.result = "Successfully connected to mock (1.2.3.4) on port 80 in 0.01s"
    tcp.end = tcp.start
    yield tcp
