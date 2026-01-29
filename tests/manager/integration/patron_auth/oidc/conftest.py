"""Configuration for OIDC integration tests."""

from __future__ import annotations

# Import all OIDC fixtures to make them available to tests in this directory
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
