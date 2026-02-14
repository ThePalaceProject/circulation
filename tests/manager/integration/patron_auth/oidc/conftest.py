"""Configuration for OIDC integration tests."""

from __future__ import annotations

import pytest
from pydantic import HttpUrl

from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.provider import (
    OIDCAuthenticationProvider,
)
from tests.fixtures.database import DatabaseTransactionFixture

# Test constants
TEST_ISSUER_URL = "https://oidc.test.example.com"
TEST_CLIENT_ID = "test-client-id"
TEST_CLIENT_SECRET = "test-client-secret"
TEST_REDIRECT_URI = "https://cm.example.com/oidc_callback"
TEST_SECRET_KEY = "test-secret-key"


@pytest.fixture
def oidc_provider(db: DatabaseTransactionFixture) -> OIDCAuthenticationProvider:
    """Create an OIDC authentication provider for testing."""
    library = db.default_library()
    settings = OIDCAuthSettings(
        issuer_url="https://idp.example.com",
        client_id="test-client-id",
        client_secret="test-client-secret",
    )
    library_settings = OIDCAuthLibrarySettings()
    return OIDCAuthenticationProvider(
        library_id=library.id,
        integration_id=1,
        settings=settings,
        library_settings=library_settings,
    )


@pytest.fixture
def oidc_minimal_manual_mode_auth_settings() -> OIDCAuthSettings:
    """OIDC settings with manual mode endpoint configuration."""
    return OIDCAuthSettings(
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
        issuer=TEST_ISSUER_URL,
        authorization_endpoint=HttpUrl(f"{TEST_ISSUER_URL}/authorize"),
        token_endpoint=HttpUrl(f"{TEST_ISSUER_URL}/token"),
        jwks_uri=HttpUrl(f"{TEST_ISSUER_URL}/.well-known/jwks.json"),
        userinfo_endpoint=HttpUrl(f"{TEST_ISSUER_URL}/userinfo"),
    )
