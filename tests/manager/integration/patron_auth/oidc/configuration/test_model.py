"""Unit tests for OIDC configuration models."""

from __future__ import annotations

import re
from contextlib import nullcontext
from typing import Any

import pytest
from pydantic import HttpUrl

from palace.manager.api.admin.problem_details import INVALID_CONFIGURATION_OPTION
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.util.problem_detail import ProblemDetailException


class TestOIDCAuthSettings:
    """Tests for OIDCAuthSettings validation and defaults."""

    @pytest.mark.parametrize(
        "test_mode",
        (
            pytest.param(True, id="test"),
            pytest.param(False, id="production"),
        ),
    )
    @pytest.mark.parametrize(
        "schema",
        (
            pytest.param("https", id="spec-compliant"),
            pytest.param("http", id="non-compliant"),
        ),
    )
    @pytest.mark.parametrize(
        "field, is_str",
        (
            pytest.param("issuer_url", False, id="issuer-url"),
            pytest.param("issuer", True, id="issuer-url"),
            pytest.param("authorization_endpoint", False, id="auth-endpoint"),
            pytest.param("token_endpoint", False, id="token-endpoint"),
            pytest.param("jwks_uri", False, id="jwks-uri"),
            pytest.param("userinfo_endpoint", False, id="userinfo-endpoint"),
        ),
    )
    def test_auth_url_validation(
        self,
        oidc_minimal_manual_mode_auth_settings: OIDCAuthSettings,
        test_mode: bool,
        schema: str,
        field: str,
        is_str: bool,
    ):
        url = f"{schema}://auth_url.example.com"
        field_value = url if is_str else HttpUrl(url)
        test_settings_update = {"test_mode": test_mode, field: field_value}

        # We should be able to get valid settings without failing.
        valid_settings = oidc_minimal_manual_mode_auth_settings

        # If we're not in test mode, we should get an error if the URL is not HTTPS.
        failure_expected = schema == "http" and not test_mode
        context = (
            pytest.raises(ProblemDetailException) if failure_expected else nullcontext()
        )
        with context as exc_info:
            # Here we can be sure that any errors were introduced by the test settings.
            settings = OIDCAuthSettings.model_validate(
                valid_settings.model_dump() | test_settings_update
            )
        if failure_expected:
            field_label = field.replace("_", " ").title()
            assert isinstance(exc_info.value, ProblemDetailException)
            problem_detail = exc_info.value.problem_detail
            assert exc_info.value.problem_detail.detail.endswith(
                f"'{field_label}' must be a valid HTTPS URL."
            )
            assert problem_detail == INVALID_CONFIGURATION_OPTION.detailed(
                f"'{field_label}' must be a valid HTTPS URL."
            )
        else:
            assert settings.model_dump()[field] == field_value

    @pytest.mark.parametrize("test_mode", [True, False])
    @pytest.mark.parametrize(
        "config_type,url_params,expected_values",
        (
            pytest.param(
                "discovery",
                {"issuer_url": "{scheme}://accounts.google.com"},
                {"issuer_url": "{scheme}://accounts.google.com/"},
                id="discovery-issuer-url",
            ),
            pytest.param(
                "manual",
                {
                    "issuer": "{scheme}://example.com",
                    "authorization_endpoint": "{scheme}://example.com/authorize",
                    "token_endpoint": "{scheme}://example.com/token",
                    "jwks_uri": "{scheme}://example.com/jwks",
                },
                {
                    "issuer": "{scheme}://example.com",
                    "authorization_endpoint": "{scheme}://example.com/authorize",
                    "token_endpoint": "{scheme}://example.com/token",
                    "jwks_uri": "{scheme}://example.com/jwks",
                },
                id="manual-all-endpoints",
            ),
            pytest.param(
                "discovery-optional",
                {
                    "issuer_url": "{scheme}://example.com",
                    "userinfo_endpoint": "{scheme}://example.com/userinfo",
                },
                {
                    "issuer_url": "{scheme}://example.com/",
                    "userinfo_endpoint": "{scheme}://example.com/userinfo",
                },
                id="discovery-with-userinfo",
            ),
        ),
    )
    def test_url_configurations(
        self, test_mode: bool, config_type: str, url_params: dict, expected_values: dict
    ):
        """Test URL configurations in production and test modes.

        Tests both discovery and manual modes with HTTPS (production) and HTTP (test mode).
        Verifies that URLs are properly validated and stored as HttpUrl or str types.
        """
        # Determine scheme based on test_mode
        scheme = "http" if test_mode else "https"

        # Build settings kwargs with scheme substitution
        settings_kwargs = {
            "test_mode": test_mode,
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        for key, value_template in url_params.items():
            settings_kwargs[key] = value_template.format(scheme=scheme)

        # Create settings
        settings = OIDCAuthSettings(**settings_kwargs)

        # Verify test_mode
        assert settings.test_mode == test_mode

        # Verify URLs
        for key, expected_template in expected_values.items():
            expected = expected_template.format(scheme=scheme)
            actual = getattr(settings, key)

            if key == "issuer":
                # issuer is str type, no conversion needed
                assert actual == expected
            else:
                # Other URL fields are HttpUrl type
                assert str(actual) == expected

        # Verify defaults for discovery mode test
        if config_type == "discovery" and not test_mode:
            assert settings.client_id == "test-client-id"
            assert settings.client_secret == "test-client-secret"
            assert settings.scopes == ["openid", "profile", "email"]
            assert settings.patron_id_claim == "sub"
            assert settings.use_pkce is True
            assert settings.token_endpoint_auth_method == "client_secret_post"
            assert settings.access_type == "offline"

        # Verify issuer_url is None for manual mode
        if config_type == "manual":
            assert settings.issuer_url is None

    @pytest.mark.parametrize(
        "settings_kwargs,expected_error_substring",
        (
            pytest.param(
                {
                    "issuer_url": "https://example.com",
                    "client_secret": "test-client-secret",
                },
                "Client ID",
                id="missing-client-id",
            ),
            pytest.param(
                {
                    "issuer_url": "https://example.com",
                    "client_id": "test-client-id",
                },
                "Client Secret",
                id="missing-client-secret",
            ),
            pytest.param(
                {
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                },
                "Issuer Identifier",
                id="missing-issuer-identifier",
            ),
            pytest.param(
                {
                    "issuer": "https://example.com",
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                },
                "Authorization Endpoint",
                id="missing-auth-endpoint",
            ),
            pytest.param(
                {
                    "issuer": "https://example.com",
                    "authorization_endpoint": "https://example.com/authorize",
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                },
                "Token Endpoint",
                id="missing-token-endpoint",
            ),
            pytest.param(
                {
                    "issuer": "https://example.com",
                    "authorization_endpoint": "https://example.com/authorize",
                    "token_endpoint": "https://example.com/token",
                    "client_id": "test-client-id",
                    "client_secret": "test-client-secret",
                },
                "JWKS URI",
                id="missing-jwks-uri",
            ),
        ),
    )
    def test_missing_required_configuration_raises_error(
        self, settings_kwargs, expected_error_substring
    ):
        """Test that missing required configuration raises validation error."""
        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(**settings_kwargs)
        assert exc_info.value.problem_detail.detail is not None
        assert expected_error_substring in exc_info.value.problem_detail.detail

    @pytest.mark.parametrize(
        "provided_fields,expected_in_error,not_expected_in_error",
        [
            pytest.param(
                {},
                [
                    "Issuer Identifier",
                    "Authorization Endpoint",
                    "Token Endpoint",
                    "JWKS URI",
                ],
                [],
                id="all-missing",
            ),
            pytest.param(
                {"issuer": "https://example.com"},
                ["Authorization Endpoint", "Token Endpoint", "JWKS URI"],
                ["Issuer Identifier"],
                id="issuer-only",
            ),
            pytest.param(
                {
                    "issuer": "https://example.com",
                    "authorization_endpoint": "https://example.com/authorize",
                },
                ["Token Endpoint", "JWKS URI"],
                ["Issuer Identifier", "Authorization Endpoint"],
                id="some-missing",
            ),
            pytest.param(
                {
                    "issuer": "https://example.com",
                    "token_endpoint": "https://example.com/token",
                    "jwks_uri": "https://example.com/jwks",
                },
                ["Authorization Endpoint"],
                ["Issuer Identifier", "Token Endpoint", "JWKS URI"],
                id="auth-endpoint-missing",
            ),
        ],
    )
    def test_missing_manual_fields_error_reporting(
        self,
        provided_fields: dict[str, str],
        expected_in_error: list[str],
        not_expected_in_error: list[str],
    ):
        """Test that missing manual mode fields are reported comprehensively.

        When manual mode is used (no issuer_url), all required fields must be provided.
        Tests that error messages list all missing fields and don't mention provided ones.
        """
        settings_kwargs = {
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            **provided_fields,
        }

        with pytest.raises(ProblemDetailException) as exc_info:
            OIDCAuthSettings(**settings_kwargs)

        assert exc_info.value.problem_detail.detail is not None
        error_msg = exc_info.value.problem_detail.detail

        # Check that all expected missing fields are mentioned
        for field in expected_in_error:
            assert field in error_msg, f"Expected '{field}' to be in error message"

        # Check that provided fields are not mentioned
        for field in not_expected_in_error:
            assert field not in error_msg, f"Did not expect '{field}' in error message"

    @pytest.mark.parametrize(
        "scopes,should_pass",
        [
            pytest.param(["profile", "email"], False, id="missing-openid"),
            pytest.param(["openid"], True, id="only-openid"),
            pytest.param(
                ["openid", "profile", "email"],
                True,
                id="openid-with-standard-scopes",
            ),
            pytest.param(
                ["openid", "profile", "email", "custom_scope"],
                True,
                id="openid-with-custom-scope",
            ),
        ],
    )
    def test_scopes_validation(self, scopes: list[str], should_pass: bool):
        """Test scopes validation with various combinations.

        The 'openid' scope is required for OIDC compliance.
        Tests that missing openid is rejected and valid combinations are accepted.
        """
        if should_pass:
            settings = OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                scopes=scopes,
            )
            assert settings.scopes == scopes
        else:
            with pytest.raises(ProblemDetailException) as exc_info:
                OIDCAuthSettings(
                    issuer_url="https://example.com",
                    client_id="test-client-id",
                    client_secret="test-client-secret",
                    scopes=scopes,
                )
            assert exc_info.value.problem_detail.detail is not None
            assert "openid" in exc_info.value.problem_detail.detail.lower()

    @pytest.mark.parametrize(
        "original_issuer",
        (
            pytest.param("https://example.com", id="no-trailing-slash"),
            pytest.param("https://example.com/", id="trailing-slash"),
            pytest.param("https://example.com/realms/myrealm", id="with-path"),
        ),
    )
    def test_issuer_preserves_exact_value(self, original_issuer):
        """Test that issuer value is preserved exactly as provided."""
        settings = OIDCAuthSettings(
            issuer=original_issuer,
            authorization_endpoint="https://example.com/authorize",
            token_endpoint="https://example.com/token",
            jwks_uri="https://example.com/jwks",
            client_id="test-client-id",
            client_secret="test-client-secret",
        )
        assert settings.issuer == original_issuer

    @pytest.mark.parametrize(
        "regex_pattern,should_pass",
        [
            pytest.param(None, True, id="none"),
            pytest.param(r"([^@]+)@example\.com", False, id="missing-named-group"),
            pytest.param(
                r"(?P<patron_id>[^@]+)@example\.com", True, id="valid-pattern"
            ),
        ],
    )
    def test_patron_id_regex_validation(
        self, regex_pattern: str | None, should_pass: bool
    ):
        """Test patron ID regex validation.

        The regex must contain a named group 'patron_id' to extract the patron identifier.
        Tests that missing named group is rejected, valid patterns are accepted, and None is allowed.
        """
        compiled_pattern = re.compile(regex_pattern) if regex_pattern else None

        if should_pass:
            settings = OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                patron_id_regular_expression=compiled_pattern,
            )
            if compiled_pattern is None:
                assert settings.patron_id_regular_expression is None
            else:
                assert settings.patron_id_regular_expression is not None
                assert "patron_id" in settings.patron_id_regular_expression.groupindex
        else:
            with pytest.raises(ProblemDetailException) as exc_info:
                OIDCAuthSettings(
                    issuer_url="https://example.com",
                    client_id="test-client-id",
                    client_secret="test-client-secret",
                    patron_id_regular_expression=compiled_pattern,
                )
            assert exc_info.value.problem_detail.detail is not None
            error_str = exc_info.value.problem_detail.detail
            assert (
                "patron_id" in error_str.lower() or "named group" in error_str.lower()
            )

    @pytest.mark.parametrize(
        "filter_expr,should_pass",
        [
            pytest.param(None, True, id="none"),
            pytest.param(
                "claims.get('email', '').endswith('@example.com')",
                True,
                id="simple-valid",
            ),
            pytest.param(
                "'admin' in claims.get('roles', [])", True, id="complex-roles"
            ),
            pytest.param(
                "claims.get('email_verified') is True", True, id="complex-boolean"
            ),
            pytest.param("claims.get('age', 0) >= 18", True, id="complex-comparison"),
            pytest.param(
                "'university' in claims.get('email', '').lower()",
                True,
                id="complex-string",
            ),
            pytest.param(
                "claims.get('email' invalid syntax", False, id="invalid-syntax"
            ),
        ],
    )
    def test_filter_expression_validation(
        self, filter_expr: str | None, should_pass: bool
    ):
        """Test filter expression validation with various expressions.

        Filter expressions are Python expressions evaluated to determine patron eligibility.
        Tests valid syntax, complex expressions, invalid syntax, and None.
        """
        if should_pass:
            settings = OIDCAuthSettings(
                issuer_url="https://example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                filter_expression=filter_expr,
            )
            assert settings.filter_expression == filter_expr
        else:
            with pytest.raises(ProblemDetailException) as exc_info:
                OIDCAuthSettings(
                    issuer_url="https://example.com",
                    client_id="test-client-id",
                    client_secret="test-client-secret",
                    filter_expression=filter_expr,
                )
            assert exc_info.value.problem_detail.detail is not None
            error_str = exc_info.value.problem_detail.detail.lower()
            assert "syntax" in error_str or "invalid" in error_str

    @pytest.mark.parametrize(
        "session_lifetime,should_pass",
        [
            pytest.param(None, True, id="none"),
            pytest.param(30, True, id="positive-int"),
            pytest.param(1, True, id="minimum-valid"),
            pytest.param(0, False, id="zero"),
            pytest.param(-5, False, id="negative"),
        ],
    )
    def test_session_lifetime_validation(
        self, session_lifetime: int | None, should_pass: bool
    ):
        """Test session lifetime validation with various values.

        Session lifetime must be a positive integer (days) or None.
        Tests that positive values are accepted, zero/negative are rejected, and None is allowed.
        """
        kwargs = {
            "issuer_url": "https://example.com",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
            "session_lifetime": session_lifetime,
        }
        if should_pass:
            settings = OIDCAuthSettings(**kwargs)
            assert settings.session_lifetime == session_lifetime
        else:
            with pytest.raises(ProblemDetailException) as exc_info:
                OIDCAuthSettings(**kwargs)
            assert exc_info.value.problem_detail.detail is not None
            error_str = exc_info.value.problem_detail.detail
            assert "Session Lifetime" in error_str or "greater than 0" in error_str

    @pytest.mark.parametrize(
        "use_pkce, expected",
        (
            pytest.param(None, True, id="default"),
            pytest.param(True, True, id="explicit-true"),
            pytest.param(False, False, id="explicit-false"),
        ),
    )
    def test_use_pkce(self, use_pkce: bool | None, expected: bool) -> None:
        """Test that use_pkce defaults to True and can be configured."""
        kwargs: dict[str, Any] = {
            "issuer_url": "https://example.com",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        if use_pkce is not None:
            kwargs["use_pkce"] = use_pkce

        settings = OIDCAuthSettings(**kwargs)
        assert settings.use_pkce is expected

    @pytest.mark.parametrize(
        "auth_method, expected",
        (
            pytest.param(None, "client_secret_post", id="default"),
            pytest.param(
                "client_secret_post", "client_secret_post", id="explicit-post"
            ),
            pytest.param("client_secret_basic", "client_secret_basic", id="basic"),
        ),
    )
    def test_token_endpoint_auth_method(self, auth_method, expected):
        """Test token_endpoint_auth_method default and configuration."""
        kwargs = {
            "issuer_url": "https://example.com",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        if auth_method is not None:
            kwargs["token_endpoint_auth_method"] = auth_method

        settings = OIDCAuthSettings(**kwargs)
        assert settings.token_endpoint_auth_method == expected

    @pytest.mark.parametrize(
        "access_type, expected",
        (
            pytest.param(None, "offline", id="default-offline"),
            pytest.param("online", "online", id="explicit-online"),
            pytest.param("offline", "offline", id="explicit-offline"),
        ),
    )
    def test_access_type(self, access_type, expected):
        """Test that access_type defaults to offline and can be set to online."""
        kwargs = {
            "issuer_url": "https://example.com",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        if access_type is not None:
            kwargs["access_type"] = access_type

        settings = OIDCAuthSettings(**kwargs)
        assert settings.access_type == expected

    @pytest.mark.parametrize(
        "patron_id_claim,expected",
        [
            pytest.param(None, "sub", id="default"),
            pytest.param("sub", "sub", id="explicit-sub"),
            pytest.param("email", "email", id="email"),
            pytest.param("eduPersonPrincipalName", "eduPersonPrincipalName", id="eppn"),
            pytest.param("preferred_username", "preferred_username", id="preferred"),
            pytest.param("unique_name", "unique_name", id="unique-name"),
            pytest.param("upn", "upn", id="upn"),
        ],
    )
    def test_patron_id_claim(self, patron_id_claim: str | None, expected: str):
        """Test `patron_id_claim` configuration with various claim names.

        Tests the default value ('sub') and various commonly used custom claim names.
        """
        kwargs: dict[str, Any] = {
            "issuer_url": "https://example.com",
            "client_id": "test-client-id",
            "client_secret": "test-client-secret",
        }
        if patron_id_claim is not None:
            kwargs["patron_id_claim"] = patron_id_claim

        settings = OIDCAuthSettings(**kwargs)
        assert settings.patron_id_claim == expected


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
