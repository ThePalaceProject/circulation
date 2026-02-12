"""Configuration for OIDC integration tests."""

from __future__ import annotations

import pytest

from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.provider import (
    OIDCAuthenticationProvider,
)
from tests.fixtures.database import DatabaseTransactionFixture


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
