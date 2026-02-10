"""Tests for OIDC authentication provider."""

import re
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from palace.manager.api.authentication.base import PatronData, PatronLookupNotSupported
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.provider import (
    OIDC_CANNOT_DETERMINE_PATRON,
    OIDC_PATRON_FILTERED,
    OIDC_TOKEN_EXPIRED,
    OIDCAuthenticationProvider,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture


class TestOIDCAuthenticationProvider:
    def test_label(self):
        assert OIDCAuthenticationProvider.label() == "OpenID Connect"

    def test_description(self):
        description = OIDCAuthenticationProvider.description()
        assert "OpenID Connect authentication provider" in description
        assert "PKCE" in description

    def test_settings_class(self):
        assert OIDCAuthenticationProvider.settings_class() == OIDCAuthSettings

    def test_library_settings_class(self):
        assert (
            OIDCAuthenticationProvider.library_settings_class()
            == OIDCAuthLibrarySettings
        )

    def test_identifies_individuals(self, oidc_provider):
        assert oidc_provider.identifies_individuals is True

    def test_get_credential_from_header_with_bearer_token(self, oidc_provider):
        auth = MagicMock()
        auth.type = "Bearer"
        auth.token = "test-token"

        result = oidc_provider.get_credential_from_header(auth)

        assert result == "test-token"

    def test_get_credential_from_header_with_basic_auth(self, oidc_provider):
        auth = MagicMock()
        auth.type = "Basic"
        auth.token = "test-token"

        result = oidc_provider.get_credential_from_header(auth)

        assert result is None

    def test_get_credential_from_header_without_token(self, oidc_provider):
        auth = MagicMock()
        auth.type = "Bearer"
        auth.token = None

        result = oidc_provider.get_credential_from_header(auth)

        assert result is None

    def test_authentication_flow_document(
        self, db: DatabaseTransactionFixture, oidc_provider
    ):
        library = db.default_library()

        with patch(
            "palace.manager.integration.patron_auth.oidc.provider.url_for"
        ) as mock_url_for:
            mock_url_for.return_value = (
                f"https://example.com/{library.short_name}/oidc_authenticate"
            )

            result = oidc_provider._authentication_flow_document(db.session)

            assert (
                result["type"] == "http://thepalaceproject.org/authtype/OpenIDConnect"
            )
            assert result["description"] == "OpenID Connect"
            assert len(result["links"]) == 1

            link = result["links"][0]
            assert link["rel"] == "authenticate"
            assert library.short_name in link["href"]
            assert "oidc_authenticate" in link["href"]

            assert link["display_names"] == [
                {"value": "OpenID Connect", "language": "en"}
            ]
            assert link["descriptions"] == [
                {"value": "OpenID Connect", "language": "en"}
            ]
            assert link["information_urls"] == []
            assert link["privacy_statement_urls"] == []
            assert link["logo_urls"] == []

            mock_url_for.assert_called_once_with(
                "oidc_authenticate",
                _external=True,
                library_short_name=library.short_name,
                provider="OpenID Connect",
            )

    def test_authentication_flow_document_with_authorization_link_settings(
        self, db: DatabaseTransactionFixture
    ):
        """Test authentication flow document with custom UI settings."""
        library = db.default_library()

        settings = OIDCAuthSettings(
            issuer_url="https://idp.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            auth_link_display_name="University Single Sign-On",
            auth_link_description="Log in with your university credentials",
            auth_link_logo_url="https://university.example.com/logo.png",
            auth_link_information_url="https://help.university.example.com",
            auth_link_privacy_statement_url="https://university.example.com/privacy",
        )
        library_settings = OIDCAuthLibrarySettings()
        provider = OIDCAuthenticationProvider(
            library_id=library.id,
            integration_id=1,
            settings=settings,
            library_settings=library_settings,
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.provider.url_for"
        ) as mock_url_for:
            mock_url_for.return_value = (
                f"https://example.com/{library.short_name}/oidc/authenticate"
            )

            result = provider._authentication_flow_document(db.session)

            assert (
                result["type"] == "http://thepalaceproject.org/authtype/OpenIDConnect"
            )
            assert result["description"] == "OpenID Connect"
            assert len(result["links"]) == 1

            link = result["links"][0]
            assert link["rel"] == "authenticate"
            assert link["display_names"] == [
                {"value": "University Single Sign-On", "language": "en"}
            ]
            assert link["descriptions"] == [
                {"value": "Log in with your university credentials", "language": "en"}
            ]
            assert link["information_urls"] == [
                {"value": "https://help.university.example.com/", "language": "en"}
            ]
            assert link["privacy_statement_urls"] == [
                {"value": "https://university.example.com/privacy", "language": "en"}
            ]
            assert link["logo_urls"] == [
                {"value": "https://university.example.com/logo.png", "language": "en"}
            ]

    def test_authentication_flow_document_no_library(
        self, db: DatabaseTransactionFixture, oidc_provider
    ):
        oidc_provider.library_id = 999999

        with pytest.raises(ValueError, match="Library not found"):
            oidc_provider._authentication_flow_document(db.session)

    def test_run_self_tests(self, db: DatabaseTransactionFixture, oidc_provider):
        results = list(oidc_provider._run_self_tests(db.session))
        assert results == []

    def test_authenticated_patron_with_invalid_token_type(
        self, db: DatabaseTransactionFixture, oidc_provider
    ):
        result = oidc_provider.authenticated_patron(db.session, {"invalid": "dict"})
        assert result is None

    def test_authenticated_patron_with_expired_token(
        self, db: DatabaseTransactionFixture, oidc_provider
    ):
        result = oidc_provider.authenticated_patron(db.session, "invalid-token")
        assert result == OIDC_TOKEN_EXPIRED

    @freeze_time("2025-01-29 12:00:00")
    def test_authenticated_patron_with_valid_token(
        self, db: DatabaseTransactionFixture, oidc_provider
    ):
        patron = db.patron()
        patron.authorization_identifier = "test-user"

        DataSource.lookup(db.session, "OIDC", autocreate=True)

        id_token_claims = {
            "sub": "test-user",
            "email": "test@example.com",
            "iss": "https://idp.example.com",
            "aud": "test-client-id",
            "exp": 1737288000,
            "iat": 1737284400,
        }

        credential = oidc_provider._credential_manager.create_oidc_token(
            db.session,
            patron,
            id_token_claims,
            "test-access-token",
            "test-refresh-token",
            3600,
            86400,
        )
        db.session.commit()

        result = oidc_provider.authenticated_patron(db.session, credential.credential)

        assert result == patron

    def test_authenticated_patron_with_refresh_failure(
        self, db: DatabaseTransactionFixture, oidc_provider
    ):
        patron = db.patron()
        patron.authorization_identifier = "test-user"

        DataSource.lookup(db.session, "OIDC", autocreate=True)

        id_token_claims = {
            "sub": "test-user",
            "email": "test@example.com",
            "iss": "https://idp.example.com",
            "aud": "test-client-id",
            "exp": 1,
            "iat": 1,
        }

        credential = oidc_provider._credential_manager.create_oidc_token(
            db.session,
            patron,
            id_token_claims,
            "test-access-token",
            "test-refresh-token",
            1,
            86400,
        )
        db.session.commit()

        with patch.object(
            oidc_provider._credential_manager,
            "refresh_token_if_needed",
            side_effect=Exception("Refresh failed"),
        ):
            result = oidc_provider.authenticated_patron(
                db.session, credential.credential
            )

        assert result == OIDC_TOKEN_EXPIRED

    def test_remote_patron_lookup_from_oidc_claims_success(self, oidc_provider):
        id_token_claims = {
            "sub": "test-user-123",
            "email": "test@example.com",
        }

        patron_data = oidc_provider.remote_patron_lookup_from_oidc_claims(
            id_token_claims
        )

        assert patron_data.permanent_id == "test-user-123"
        assert patron_data.authorization_identifier == "test-user-123"
        assert patron_data.external_type == "A"
        assert patron_data.complete is True

    def test_remote_patron_lookup_from_oidc_claims_missing_patron_id(
        self, oidc_provider
    ):
        id_token_claims = {"email": "test@example.com"}

        with pytest.raises(ProblemDetailException) as exc_info:
            oidc_provider.remote_patron_lookup_from_oidc_claims(id_token_claims)

        assert exc_info.value.problem_detail == OIDC_CANNOT_DETERMINE_PATRON

    def test_remote_patron_lookup_from_oidc_claims_with_regex(self):
        settings = OIDCAuthSettings(
            issuer_url="https://idp.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            patron_id_claim="email",
            patron_id_regular_expression=re.compile(
                r"(?P<patron_id>[^@]+)@example\.com"
            ),
        )
        library_settings = OIDCAuthLibrarySettings()
        provider = OIDCAuthenticationProvider(
            library_id=1,
            integration_id=1,
            settings=settings,
            library_settings=library_settings,
        )

        id_token_claims = {"email": "user123@example.com"}

        patron_data = provider.remote_patron_lookup_from_oidc_claims(id_token_claims)

        assert patron_data.permanent_id == "user123"
        assert patron_data.authorization_identifier == "user123"

    def test_remote_patron_lookup_from_oidc_claims_regex_no_match(self):
        settings = OIDCAuthSettings(
            issuer_url="https://idp.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            patron_id_claim="email",
            patron_id_regular_expression=re.compile(
                r"(?P<patron_id>[^@]+)@example\.com"
            ),
        )
        library_settings = OIDCAuthLibrarySettings()
        provider = OIDCAuthenticationProvider(
            library_id=1,
            integration_id=1,
            settings=settings,
            library_settings=library_settings,
        )

        id_token_claims = {"email": "user@other.com"}

        with pytest.raises(ProblemDetailException) as exc_info:
            provider.remote_patron_lookup_from_oidc_claims(id_token_claims)

        assert exc_info.value.problem_detail == OIDC_CANNOT_DETERMINE_PATRON

    @pytest.mark.parametrize(
        "role,should_pass",
        [
            pytest.param("patron", True, id="allowed"),
            pytest.param("staff", False, id="denied"),
        ],
    )
    def test_remote_patron_lookup_from_oidc_claims_with_filter(self, role, should_pass):
        """Test remote patron lookup with role-based filter."""
        settings = OIDCAuthSettings(
            issuer_url="https://idp.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            filter_expression="claims.get('role') == 'patron'",
        )
        library_settings = OIDCAuthLibrarySettings()
        provider = OIDCAuthenticationProvider(
            library_id=1,
            integration_id=1,
            settings=settings,
            library_settings=library_settings,
        )

        id_token_claims = {"sub": "test-user", "role": role}

        if should_pass:
            patron_data = provider.remote_patron_lookup_from_oidc_claims(
                id_token_claims
            )
            assert patron_data.permanent_id == "test-user"
        else:
            with pytest.raises(ProblemDetailException) as exc_info:
                provider.remote_patron_lookup_from_oidc_claims(id_token_claims)
            assert exc_info.value.problem_detail == OIDC_PATRON_FILTERED

    def test_remote_patron_lookup_from_oidc_claims_with_filter_error(self):
        with pytest.raises(ProblemDetailException):
            settings = OIDCAuthSettings(
                issuer_url="https://idp.example.com",
                client_id="test-client-id",
                client_secret="test-client-secret",
                filter_expression="invalid python syntax",
            )

    def test_remote_patron_lookup_raises_not_supported(self, oidc_provider):
        with pytest.raises(PatronLookupNotSupported):
            oidc_provider.remote_patron_lookup(PatronData(permanent_id="test"))

    def test_oidc_callback(self, db: DatabaseTransactionFixture, oidc_provider):
        DataSource.lookup(db.session, "OIDC", autocreate=True)

        id_token_claims = {
            "sub": "test-user-456",
            "email": "test@example.com",
            "iss": "https://idp.example.com",
            "aud": "test-client-id",
            "exp": 1737288000,
            "iat": 1737284400,
        }

        credential, patron, patron_data = oidc_provider.oidc_callback(
            db.session,
            id_token_claims,
            "test-access-token",
            "test-refresh-token",
            3600,
        )

        assert patron.authorization_identifier == "test-user-456"
        assert patron_data.permanent_id == "test-user-456"
        assert credential.credential is not None
        assert credential.patron == patron

    def test_get_authentication_manager(self, oidc_provider):
        manager = oidc_provider.get_authentication_manager()

        assert manager is not None
        assert manager._settings == oidc_provider._settings
