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
from tests.fixtures.oidc import (  # noqa: F401
    mock_authorization_code,
    mock_discovery_document,
    mock_id_token,
    mock_id_token_claims,
    mock_jwks,
    mock_logout_token,
    mock_logout_token_claims,
    mock_oidc_provider,
    mock_pkce,
    mock_state_data,
    mock_token_response,
    mock_userinfo_response,
    oidc_test_keys,
)


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
