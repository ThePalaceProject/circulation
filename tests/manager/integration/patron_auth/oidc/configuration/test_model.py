"""Unit tests for OIDC configuration models."""

from __future__ import annotations

import re

import pytest

from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.util.problem_detail import ProblemDetailException


class TestOIDCAuthSettings:
    """Tests for OIDCAuthSettings validation and defaults."""

    def test_valid_configuration_with_issuer_url(self):
        """Test valid configuration using issuer URL for discovery."""
        settings = OIDCAuthSettings(
            issuer_url="https://accounts.google.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

        assert str(settings.issuer_url) == "https://accounts.google.com/"
        assert settings.client_id == "test-client-id"
        assert settings.client_secret == "test-client-secret"
        assert settings.scopes == ["openid", "profile", "email"]
        assert settings.patron_id_claim == "sub"
        assert settings.use_pkce is True
        assert settings.token_endpoint_auth_method == "client_secret_post"
        assert settings.access_type == "offline"

    def test_valid_configuration_with_manual_endpoints(self):
        """Test valid configuration using manual endpoint URLs."""
        settings = OIDCAuthSettings(
            authorization_endpoint="https://example.com/authorize",
            token_endpoint="https://example.com/token",
            jwks_uri="https://example.com/jwks",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

        assert str(settings.authorization_endpoint) == "https://example.com/authorize"
        assert str(settings.token_endpoint) == "https://example.com/token"
        assert str(settings.jwks_uri) == "https://example.com/jwks"
        assert settings.issuer_url is None

    def test_valid_configuration_with_optional_endpoints(self):
        """Test configuration with optional userinfo endpoint."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            userinfo_endpoint="https://example.com/userinfo",
        )

        assert str(settings.userinfo_endpoint) == "https://example.com/userinfo"

    def test_missing_client_id_raises_error(self):
        """Test that missing client_id raises validation error."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_secret="test-client-secret",
            )
        assert exc_info.value.problem_detail.detail is not None
        assert "Client ID" in exc_info.value.problem_detail.detail

    def test_missing_client_secret_raises_error(self):
        """Test that missing client_secret raises validation error."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
            )
        assert exc_info.value.problem_detail.detail is not None
        assert "Client Secret" in exc_info.value.problem_detail.detail

    def test_missing_issuer_and_authorization_endpoint_raises_error(self):
        """Test that missing both issuer_url and authorization_endpoint raises error."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                client_id="test-client-id",
                client_secret="test-client-secret",
            )
        assert exc_info.value.problem_detail.detail is not None
        assert "Authorization Endpoint" in exc_info.value.problem_detail.detail

    def test_missing_issuer_and_token_endpoint_raises_error(self):
        """Test that missing both issuer_url and token_endpoint raises error."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                authorization_endpoint="https://example.com/authorize",
                client_id="test-client-id",
                client_secret="test-client-secret",
            )
        assert exc_info.value.problem_detail.detail is not None
        assert "Token Endpoint" in exc_info.value.problem_detail.detail

    def test_missing_issuer_and_jwks_uri_raises_error(self):
        """Test that missing both issuer_url and jwks_uri raises error."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                authorization_endpoint="https://example.com/authorize",
                token_endpoint="https://example.com/token",
                client_id="test-client-id",
                client_secret="test-client-secret",
            )
        assert exc_info.value.problem_detail.detail is not None
        assert "JWKS URI" in exc_info.value.problem_detail.detail

    def test_scopes_validation_requires_openid(self):
        """Test that scopes must include 'openid'."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                scopes=["profile", "email"],
            )
        assert exc_info.value.problem_detail.detail is not None
        assert "openid" in exc_info.value.problem_detail.detail.lower()

    def test_scopes_validation_allows_openid_with_others(self):
        """Test that scopes can include openid with additional scopes."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            scopes=["openid", "profile", "email", "custom_scope"],
        )

        assert settings.scopes == ["openid", "profile", "email", "custom_scope"]

    def test_scopes_validation_allows_only_openid(self):
        """Test that scopes can be only openid."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            scopes=["openid"],
        )

        assert settings.scopes == ["openid"]

    def test_patron_id_regex_validation_requires_named_group(self):
        """Test that patron_id regex must have 'patron_id' named group."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                patron_id_regular_expression=re.compile(r"([^@]+)@example\.com"),
            )
        assert exc_info.value.problem_detail.detail is not None
        error_str = exc_info.value.problem_detail.detail
        assert "patron_id" in error_str.lower() or "named group" in error_str.lower()

    def test_patron_id_regex_validation_accepts_valid_pattern(self):
        """Test that valid patron_id regex with named group is accepted."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            patron_id_regular_expression=re.compile(
                r"(?P<patron_id>[^@]+)@example\.com"
            ),
        )

        assert settings.patron_id_regular_expression is not None
        assert "patron_id" in settings.patron_id_regular_expression.groupindex

    def test_patron_id_regex_validation_accepts_none(self):
        """Test that None is accepted for patron_id regex."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            patron_id_regular_expression=None,
        )

        assert settings.patron_id_regular_expression is None

    def test_filter_expression_validation_accepts_valid_syntax(self):
        """Test that valid filter expression is accepted."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            filter_expression="claims.get('email', '').endswith('@example.com')",
        )

        assert (
            settings.filter_expression
            == "claims.get('email', '').endswith('@example.com')"
        )

    def test_filter_expression_validation_accepts_complex_expressions(self):
        """Test that complex filter expressions are accepted."""
        expressions = [
            "'admin' in claims.get('roles', [])",
            "claims.get('email_verified') is True",
            "claims.get('age', 0) >= 18",
            "'university' in claims.get('email', '').lower()",
        ]

        for expr in expressions:
            settings = OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                filter_expression=expr,
            )
            assert settings.filter_expression == expr

    def test_filter_expression_validation_rejects_invalid_syntax(self):
        """Test that invalid filter expression syntax is rejected."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                filter_expression="claims.get('email' invalid syntax",
            )
        assert exc_info.value.problem_detail.detail is not None
        error_str = exc_info.value.problem_detail.detail.lower()
        assert "syntax" in error_str or "invalid" in error_str

    def test_filter_expression_validation_accepts_none(self):
        """Test that None is accepted for filter expression."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            filter_expression=None,
        )

        assert settings.filter_expression is None

    def test_session_lifetime_validation_accepts_positive_int(self):
        """Test that positive integer session lifetime is accepted."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            session_lifetime=30,
        )

        assert settings.session_lifetime == 30

    def test_session_lifetime_validation_rejects_zero(self):
        """Test that zero session lifetime is rejected."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                session_lifetime=0,
            )
        assert exc_info.value.problem_detail.detail is not None
        error_str = exc_info.value.problem_detail.detail
        assert "Session Lifetime" in error_str or "greater than 0" in error_str

    def test_session_lifetime_validation_rejects_negative(self):
        """Test that negative session lifetime is rejected."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                session_lifetime=-5,
            )
        assert exc_info.value.problem_detail.detail is not None
        error_str = exc_info.value.problem_detail.detail
        assert "Session Lifetime" in error_str or "greater than 0" in error_str

    def test_session_lifetime_validation_accepts_none(self):
        """Test that None is accepted for session lifetime."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            session_lifetime=None,
        )

        assert settings.session_lifetime is None

    def test_use_pkce_default_is_true(self):
        """Test that use_pkce defaults to True."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

        assert settings.use_pkce is True

    def test_use_pkce_can_be_disabled(self):
        """Test that use_pkce can be set to False."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            use_pkce=False,
        )

        assert settings.use_pkce is False

    def test_token_endpoint_auth_method_default(self):
        """Test that token_endpoint_auth_method defaults to client_secret_post."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

        assert settings.token_endpoint_auth_method == "client_secret_post"

    def test_token_endpoint_auth_method_can_be_basic(self):
        """Test that token_endpoint_auth_method can be set to client_secret_basic."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            token_endpoint_auth_method="client_secret_basic",
        )

        assert settings.token_endpoint_auth_method == "client_secret_basic"

    def test_access_type_default(self):
        """Test that access_type defaults to offline."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

        assert settings.access_type == "offline"

    def test_access_type_can_be_online(self):
        """Test that access_type can be set to online."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            access_type="online",
        )

        assert settings.access_type == "online"

    def test_patron_id_claim_default(self):
        """Test that patron_id_claim defaults to 'sub'."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )

        assert settings.patron_id_claim == "sub"

    def test_patron_id_claim_can_be_customized(self):
        """Test that patron_id_claim can be customized."""
        settings = OIDCAuthSettings(
            issuer_url="https://example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            patron_id_claim="email",
        )

        assert settings.patron_id_claim == "email"

    def test_custom_patron_id_claim_examples(self):
        """Test various custom patron_id_claim values."""
        custom_claims = [
            "email",
            "preferred_username",
            "eduPersonPrincipalName",
            "upn",
            "unique_name",
        ]

        for claim in custom_claims:
            settings = OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                patron_id_claim=claim,
            )
            assert settings.patron_id_claim == claim


class TestOIDCAuthLibrarySettings:
    """Tests for OIDCAuthLibrarySettings."""

    def test_library_settings_instantiation(self):
        """Test that OIDCAuthLibrarySettings can be instantiated."""
        settings = OIDCAuthLibrarySettings()

        assert settings is not None
        assert isinstance(settings, OIDCAuthLibrarySettings)

    def test_library_settings_is_empty(self):
        """Test that OIDCAuthLibrarySettings has no required fields."""
        # Should not raise any errors
        settings = OIDCAuthLibrarySettings()

        # Convert to dict to check it's empty (only has inherited fields if any)
        settings_dict = settings.model_dump()
        # Should be empty or only contain inherited base fields
        assert len(settings_dict) == 0 or all(
            k.startswith("_") for k in settings_dict.keys()
        )
