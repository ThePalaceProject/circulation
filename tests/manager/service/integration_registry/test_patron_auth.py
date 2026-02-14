"""Tests for patron authentication integration registry."""

from palace.manager.integration.patron_auth.oidc.provider import (
    OIDCAuthenticationProvider,
)
from palace.manager.service.integration_registry.patron_auth import PatronAuthRegistry


class TestPatronAuthRegistry:
    def test_oidc_provider_registered(self):
        """Test that OIDC provider is registered in the patron auth registry."""
        registry = PatronAuthRegistry()

        assert registry["api.oidc.provider"] == OIDCAuthenticationProvider
        assert registry["OIDCAuthenticationProvider"] == OIDCAuthenticationProvider
