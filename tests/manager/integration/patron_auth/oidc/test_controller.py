"""Tests for OIDC controller."""

import json
import logging
from unittest.mock import MagicMock, Mock, patch

import jwt
import pytest
from sqlalchemy.exc import SQLAlchemyError

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authenticator import BaseOIDCAuthenticationProvider
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND, UNKNOWN_OIDC_PROVIDER
from palace.manager.integration.patron_auth.oidc.auth import OIDCAuthenticationError
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.controller import (
    OIDC_INVALID_REQUEST,
    OIDC_INVALID_RESPONSE,
    OIDC_INVALID_STATE,
    OIDCController,
)
from palace.manager.integration.patron_auth.oidc.provider import (
    OIDC_CANNOT_DETERMINE_PATRON,
    OIDCAuthenticationProvider,
)
from palace.manager.integration.patron_auth.oidc.util import OIDCUtility
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture


class TestOIDCController:
    @pytest.fixture
    def mock_circulation_manager(self):
        cm = MagicMock()
        cm.index_controller.library_for_request = MagicMock()
        return cm

    @pytest.fixture
    def mock_authenticator(self):
        authenticator = MagicMock()
        authenticator.library_authenticators = {}
        return authenticator

    @pytest.fixture
    def controller(self, mock_circulation_manager, mock_authenticator):
        return OIDCController(mock_circulation_manager, mock_authenticator)

    def test_add_params_to_url(self, controller):
        url = "https://example.com/callback"
        params = {"token": "abc123", "patron_info": '{"name":"test"}'}

        result = controller._add_params_to_url(url, params)

        assert "token=abc123" in result
        assert "patron_info=" in result
        assert result.startswith("https://example.com/callback?")

    def test_add_params_to_url_with_existing_params(self, controller):
        url = "https://example.com/callback?existing=value"
        params = {"new": "param"}

        result = controller._add_params_to_url(url, params)

        assert "existing=value" in result
        assert "new=param" in result

    def test_add_params_to_url_param_override(self, controller, caplog):
        """Test that new params override existing params with the same key."""
        url = "https://example.com/callback?token=old_value&keep=this"
        params = {"token": "new_value", "extra": "param"}

        result = controller._add_params_to_url(url, params)

        # New param should override existing param
        assert "token=new_value" in result
        assert "token=old_value" not in result
        # Existing param without collision should be preserved
        assert "keep=this" in result
        # New param should be added
        assert "extra=param" in result
        # Warning should be logged for collision
        assert "Parameter collision in redirect_uri" in caplog.text
        assert "token" in caplog.text

    def test_error_uri(self, controller):
        redirect_uri = "https://app.example.com/callback"
        problem_detail = OIDC_INVALID_REQUEST

        result = controller._error_uri(redirect_uri, problem_detail)

        assert "error=" in result
        assert redirect_uri in result

    def test_get_request_parameter_success(self, controller):
        params = {
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
        }

        result = controller._get_request_parameter(params, "provider")

        assert result == "OpenID Connect"

    def test_get_request_parameter_missing(self, controller):
        params = {"redirect_uri": "https://app.example.com"}

        result = controller._get_request_parameter(params, "provider")

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert "provider" in result.detail

    def test_get_request_parameter_with_default(self, controller):
        params = {}

        result = controller._get_request_parameter(params, "optional", "default")

        assert result == "default"

    def test_redirect_with_error(self, controller):
        redirect_uri = "https://app.example.com/callback"
        problem_detail = OIDC_INVALID_STATE

        result = controller._redirect_with_error(redirect_uri, problem_detail)

        assert result.status_code == 302
        assert "error=" in result.location

    @pytest.mark.parametrize(
        "params,expected_error_field",
        [
            pytest.param(
                {"redirect_uri": "https://app.example.com"},
                "provider",
                id="missing-provider",
            ),
            pytest.param(
                {"provider": "OpenID Connect"},
                "redirect_uri",
                id="missing-redirect-uri",
            ),
        ],
    )
    def test_oidc_authentication_redirect_missing_parameter(
        self, controller, params, expected_error_field
    ):
        """Test OIDC authentication redirect with missing required parameters."""
        result = controller.oidc_authentication_redirect(params, MagicMock())

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert expected_error_field in result.detail

    @pytest.mark.parametrize(
        "state_data,expected_result",
        [
            pytest.param(
                {
                    "provider": "OpenID Connect",
                    "redirect_uri": "https://app.example.com",
                    "nonce": "test-nonce",
                },
                OIDC_INVALID_STATE,
                id="missing-library-short-name",
            ),
            pytest.param(
                {
                    "library_short_name": "default",
                    "redirect_uri": "https://app.example.com",
                    "nonce": "test-nonce",
                },
                OIDC_INVALID_STATE,
                id="missing-provider-name",
            ),
            pytest.param(
                {
                    "library_short_name": "default",
                    "provider": "OpenID Connect",
                    "nonce": "test-nonce",
                },
                OIDC_INVALID_STATE,
                id="missing-redirect-uri",
            ),
        ],
    )
    def test_oidc_authentication_callback_invalid_state_data(
        self, controller, db, state_data, expected_result
    ):
        """Test callback with incomplete state data."""
        library = db.default_library()
        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state = utility.generate_state(state_data, "test-secret")
        params = {"code": "test-code", "state": state}

        result = controller.oidc_authentication_callback(params, db.session)

        assert result == expected_result

    def test_oidc_authentication_redirect_unknown_provider(
        self, controller, mock_authenticator
    ):
        params = {"provider": "Unknown", "redirect_uri": "https://app.example.com"}
        mock_authenticator.oidc_provider_lookup.return_value = UNKNOWN_OIDC_PROVIDER

        result = controller.oidc_authentication_redirect(params, MagicMock())

        assert result.status_code == 302
        assert "error=" in result.location

    def test_oidc_authentication_redirect_success_without_pkce(
        self, db: DatabaseTransactionFixture, controller, oidc_provider
    ):
        library = db.default_library()
        params = {
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
        }

        mock_auth_manager = MagicMock()
        mock_auth_manager.build_authorization_url.return_value = "https://idp.example.com/authorize?client_id=test-client-id&response_type=code&state=test&nonce=test"

        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider
        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_redirect(params, db.session)

            assert result.status_code == 302
            assert "https://idp.example.com" in result.location
            assert "client_id=test-client-id" in result.location
            assert "response_type=code" in result.location

    def test_oidc_authentication_redirect_success_with_pkce(
        self, db: DatabaseTransactionFixture, controller
    ):
        library = db.default_library()
        settings = OIDCAuthSettings(
            issuer_url="https://idp.example.com",
            client_id="test-client-id",
            client_secret="test-client-secret",
            use_pkce=True,
        )
        library_settings = OIDCAuthLibrarySettings()
        oidc_provider = OIDCAuthenticationProvider(
            library_id=library.id,
            integration_id=1,
            settings=settings,
            library_settings=library_settings,
        )

        params = {
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
        }

        mock_auth_manager = MagicMock()
        mock_auth_manager.build_authorization_url.return_value = "https://idp.example.com/authorize?code_challenge=test&code_challenge_method=S256"

        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider
        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_redirect(params, db.session)

            assert result.status_code == 302
            assert "code_challenge=" in result.location
            assert "code_challenge_method=S256" in result.location

    @pytest.mark.parametrize(
        "params,expected_error_substring",
        [
            pytest.param(
                {"state": "test-state"},
                "authorization code",
                id="missing-code",
            ),
            pytest.param(
                {"code": "test-code"},
                "state",
                id="missing-state",
            ),
        ],
    )
    def test_oidc_authentication_callback_missing_parameter(
        self, controller, params, expected_error_substring
    ):
        """Test OIDC authentication callback with missing required parameters."""
        result = controller.oidc_authentication_callback(params, MagicMock())

        assert result.uri == OIDC_INVALID_RESPONSE.uri
        assert expected_error_substring in result.detail

    def test_oidc_authentication_callback_invalid_state(
        self, controller, mock_authenticator
    ):
        params = {"code": "test-code", "state": "invalid-state"}
        mock_authenticator.library_authenticators = {
            "default": MagicMock(bearer_token_signing_secret="test-secret")
        }

        result = controller.oidc_authentication_callback(params, MagicMock())

        assert result == OIDC_INVALID_STATE

    def test_oidc_authentication_callback_library_not_found(
        self, db: DatabaseTransactionFixture, controller
    ):
        params = {"code": "test-code", "state": "valid-state"}
        library = db.default_library()

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": "nonexistent",
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            LIBRARY_NOT_FOUND
        )

        result = controller.oidc_authentication_callback(params, db.session)

        assert result.status_code == 302
        assert "error=" in result.location

    def test_oidc_authentication_callback_unknown_provider(
        self, db: DatabaseTransactionFixture, controller
    ):
        params = {"code": "test-code"}
        library = db.default_library()

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": library.short_name,
            "provider": "Unknown",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            library
        )
        controller._authenticator.oidc_provider_lookup.return_value = (
            UNKNOWN_OIDC_PROVIDER
        )

        result = controller.oidc_authentication_callback(params, db.session)

        assert result.status_code == 302
        assert "error=" in result.location

    def test_oidc_authentication_callback_token_exchange_failure(
        self, db: DatabaseTransactionFixture, controller, oidc_provider
    ):
        params = {"code": "test-code"}
        library = db.default_library()

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": library.short_name,
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            library
        )
        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider

        mock_auth_manager = MagicMock()
        mock_auth_manager.exchange_authorization_code.side_effect = Exception(
            "Exchange failed"
        )

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_callback(params, db.session)

            assert result.status_code == 302
            assert "error=" in result.location

    def test_oidc_authentication_callback_missing_id_token(
        self, db: DatabaseTransactionFixture, controller, oidc_provider
    ):
        params = {"code": "test-code"}
        library = db.default_library()

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": library.short_name,
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            library
        )
        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider

        mock_auth_manager = MagicMock()
        mock_auth_manager.exchange_authorization_code.return_value = {
            "access_token": "test-token"
        }

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_callback(params, db.session)

            assert result.status_code == 302
            assert "error=" in result.location

    def test_oidc_authentication_callback_invalid_id_token(
        self, db: DatabaseTransactionFixture, controller, oidc_provider
    ):
        params = {"code": "test-code"}
        library = db.default_library()

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": library.short_name,
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            library
        )
        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider

        mock_auth_manager = MagicMock()
        mock_auth_manager.exchange_authorization_code.return_value = {
            "access_token": "test-token",
            "id_token": "invalid-id-token",
        }
        mock_auth_manager.validate_id_token.side_effect = Exception("Invalid token")

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_callback(params, db.session)

            assert result.status_code == 302
            assert "error=" in result.location

    def test_oidc_authentication_callback_patron_filtered(
        self, db: DatabaseTransactionFixture, controller, oidc_provider
    ):
        params = {"code": "test-code"}
        library = db.default_library()

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": library.short_name,
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            library
        )
        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider

        mock_auth_manager = MagicMock()
        mock_auth_manager.exchange_authorization_code.return_value = {
            "access_token": "test-token",
            "id_token": "test-id-token",
        }
        mock_auth_manager.validate_id_token.return_value = {"sub": "test-user"}

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
            patch.object(
                oidc_provider,
                "oidc_callback",
                side_effect=ProblemDetailException(
                    problem_detail=OIDC_CANNOT_DETERMINE_PATRON
                ),
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_callback(params, db.session)

            assert result.status_code == 302
            assert "error=" in result.location

    def test_oidc_authentication_callback_success(
        self, db: DatabaseTransactionFixture, controller, oidc_provider
    ):
        params = {"code": "test-code"}
        library = db.default_library()

        DataSource.lookup(db.session, "OIDC", autocreate=True)
        patron = db.patron()
        patron.authorization_identifier = "test-user"

        controller._authenticator.library_authenticators = {
            library.short_name: MagicMock(bearer_token_signing_secret="test-secret")
        }
        controller._authenticator.create_bearer_token.return_value = (
            "simplified-token-123"
        )

        utility = OIDCUtility(redis_client=None)
        state_data = {
            "library_short_name": library.short_name,
            "provider": "OpenID Connect",
            "redirect_uri": "https://app.example.com",
            "nonce": "test-nonce",
        }
        state = utility.generate_state(state_data, "test-secret")
        params["state"] = state

        controller._circulation_manager.index_controller.library_for_request.return_value = (
            library
        )
        controller._authenticator.oidc_provider_lookup.return_value = oidc_provider

        mock_auth_manager = MagicMock()
        mock_auth_manager.exchange_authorization_code.return_value = {
            "access_token": "test-access-token",
            "id_token": "test-id-token",
            "refresh_token": "test-refresh-token",
            "expires_in": 3600,
        }
        mock_auth_manager.validate_id_token.return_value = {
            "sub": "test-user",
            "email": "test@example.com",
        }

        credential = MagicMock()
        credential.credential = "oidc-credential-123"
        patron_data = PatronData(
            permanent_id="test-user", authorization_identifier="test-user"
        )

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch.object(
                oidc_provider,
                "get_authentication_manager",
                return_value=mock_auth_manager,
            ),
            patch.object(
                oidc_provider,
                "oidc_callback",
                return_value=(credential, patron, patron_data),
            ),
        ):
            mock_url_for.return_value = "https://cm.example.com/oidc_callback"

            result = controller.oidc_authentication_callback(params, db.session)

            assert result.status_code == 302
            assert "access_token=simplified-token-123" in result.location
            assert "patron_info=" in result.location
            assert "https://app.example.com" in result.location

            controller._authenticator.create_bearer_token.assert_called_once_with(
                "OpenID Connect", "oidc-credential-123"
            )


class TestOIDCControllerLogout:
    """Tests for OIDC logout flow."""

    @pytest.fixture
    def mock_redis(self, redis_fixture):
        return redis_fixture.client

    @pytest.fixture
    def mock_circulation_manager(self, mock_redis):
        cm = MagicMock()
        cm.index_controller.library_for_request = MagicMock()
        cm.services.redis.client.return_value = mock_redis
        return cm

    @pytest.fixture
    def mock_authenticator(self):
        authenticator = MagicMock()
        authenticator.library_authenticators = {}
        return authenticator

    @pytest.fixture
    def logout_controller(self, mock_circulation_manager, mock_authenticator):
        return OIDCController(mock_circulation_manager, mock_authenticator)

    def test_oidc_logout_initiate_success(self, logout_controller, db):
        controller = logout_controller
        patron = db.patron()
        patron.authorization_identifier = "user123@example.com"
        db.session.commit()

        library = db.default_library()

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "test-secret"
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()
        mock_provider.integration_id = 1

        mock_auth_manager = Mock()
        mock_auth_manager.validate_id_token_hint.return_value = {
            "sub": "user123@example.com"
        }
        mock_auth_manager.supports_rp_initiated_logout.return_value = True
        mock_auth_manager.build_logout_url.return_value = (
            "https://oidc.provider.test/logout"
            "?id_token_hint=test.token"
            "&post_logout_redirect_uri=https://cm.test/oidc_logout_callback&state=test-state"
        )
        mock_provider.get_authentication_manager.return_value = mock_auth_manager

        mock_patron = Mock()
        mock_patron.id = patron.id
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            mock_patron
        )
        mock_provider._credential_manager.invalidate_patron_credentials.return_value = 1

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            json.dumps(
                {
                    "id_token_claims": {"sub": "user123@example.com"},
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": "raw.id.token.jwt",
                }
            ),
        )

        # Set up library_authenticators dict
        controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
                return_value=library,
            ),
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
        ):
            mock_url_for.return_value = "https://cm.test/oidc_logout_callback"

            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            assert result.status_code == 302
            assert "https://oidc.provider.test/logout" in result.location

            # Verify credentials were invalidated
            mock_provider._credential_manager.invalidate_patron_credentials.assert_called_once_with(
                db.session, patron.id
            )
            # Verify refresh token revocation was attempted
            mock_auth_manager.revoke_token.assert_called_once_with("refresh-token")
            # Verify stored id_token was used as id_token_hint for RP-Initiated Logout
            mock_auth_manager.build_logout_url.assert_called_once_with(
                "raw.id.token.jwt",
                "https://cm.test/oidc_logout_callback",
                mock_auth_manager.build_logout_url.call_args[0][2],
            )

    def test_oidc_logout_initiate_revocation_only(
        self,
        logout_controller: OIDCController,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test logout for providers with revocation_endpoint but no end_session_endpoint.

        The flow should: invalidate CM credential, revoke token, redirect directly
        to post_logout_redirect_uri without going through the IdP.
        """
        caplog.set_level(logging.INFO)
        controller = logout_controller
        patron = db.patron()
        patron.authorization_identifier = "user123@example.com"
        db.session.commit()

        library = db.default_library()

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "test-secret"
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()
        mock_provider.integration_id = 1

        mock_auth_manager = Mock()
        # Provider has revocation but NOT RP-Initiated Logout
        mock_auth_manager.supports_rp_initiated_logout.return_value = False
        mock_provider.get_authentication_manager.return_value = mock_auth_manager

        mock_patron = Mock()
        mock_patron.id = patron.id
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            mock_patron
        )
        mock_provider._credential_manager.invalidate_patron_credentials.return_value = 1

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            json.dumps(
                {
                    "id_token_claims": {"sub": "user123@example.com"},
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                }
            ),
        )

        controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            # Should redirect directly, not to the IdP
            assert result.status_code == 302
            assert "https://app.example.com/logout/callback" in result.location
            assert "logout_status=success" in result.location
            assert "provider does not support it" in caplog.text

            # Verify RP-Initiated Logout was NOT attempted
            mock_auth_manager.build_logout_url.assert_not_called()

            # Verify token was revoked
            mock_auth_manager.revoke_token.assert_called_once_with("refresh-token")

            # Verify CM credentials were invalidated
            mock_provider._credential_manager.invalidate_patron_credentials.assert_called_once_with(
                db.session, patron.id
            )

    def test_oidc_logout_initiate_no_stored_id_token(
        self,
        logout_controller: OIDCController,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test logout when provider supports RP-Initiated Logout but no id_token is stored.

        Should redirect directly with logout_status=partial and log a warning.
        """
        controller = logout_controller
        patron = db.patron()
        patron.authorization_identifier = "user123@example.com"
        db.session.commit()

        library = db.default_library()

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "test-secret"
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()

        mock_auth_manager = Mock()
        mock_auth_manager.supports_rp_initiated_logout.return_value = True
        mock_provider.get_authentication_manager.return_value = mock_auth_manager

        mock_patron = Mock()
        mock_patron.id = patron.id
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            mock_patron
        )
        mock_provider._credential_manager.invalidate_patron_credentials.return_value = 1

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            json.dumps(
                {
                    "id_token_claims": {"sub": "user123@example.com"},
                    "access_token": "access-token",
                    # No id_token field
                }
            ),
        )

        controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            assert result.status_code == 302
            assert "https://app.example.com/logout/callback" in result.location
            assert "logout_status=partial" in result.location
            assert "no id_token stored in credential" in caplog.text

            mock_auth_manager.build_logout_url.assert_not_called()

    @pytest.mark.parametrize(
        "library_index,patron_email,logout_url",
        [
            pytest.param(
                0,
                "user1@example.com",
                "https://provider1.test/logout",
                id="default-library",
            ),
            pytest.param(
                1,
                "user2@example.com",
                "https://provider2.test/logout",
                id="non-default-library",
            ),
        ],
    )
    def test_oidc_logout_initiate_uses_correct_library(
        self, logout_controller, db, library_index, patron_email, logout_url
    ):
        """Test that logout uses the library from the request context."""
        controller = logout_controller

        # Create two libraries with separate providers
        libraries = [db.default_library(), db.library()]
        patrons = []
        mock_library_auths = []
        mock_auth_managers = []
        mock_providers = []

        for i, library in enumerate(libraries):
            # Create patron
            patron = db.patron()
            patron.library_id = library.id
            patron.authorization_identifier = f"user{i+1}@example.com"
            patrons.append(patron)

            # Set up provider and authenticator for this library
            mock_library_auth = Mock()
            mock_library_auth.bearer_token_signing_secret = f"secret{i+1}"

            mock_provider = Mock()
            mock_provider._settings = Mock()
            mock_provider._settings.patron_id_claim = "sub"
            mock_provider._credential_manager = Mock()
            mock_provider.integration_id = i + 1

            mock_auth_manager = Mock()
            mock_auth_manager.supports_rp_initiated_logout.return_value = True
            mock_auth_manager.build_logout_url.return_value = (
                f"https://provider{i+1}.test/logout"
            )
            mock_auth_managers.append(mock_auth_manager)

            mock_provider.get_authentication_manager.return_value = mock_auth_manager
            mock_provider._credential_manager.lookup_patron_by_identifier.return_value = Mock(
                id=patron.id
            )
            mock_provider._credential_manager.invalidate_patron_credentials.return_value = (
                1
            )
            mock_library_auth.decode_bearer_token.return_value = (
                "Test OIDC",
                json.dumps(
                    {
                        "id_token_claims": {"sub": patron.authorization_identifier},
                        "access_token": "access-token",
                        "refresh_token": "refresh-token",
                        "id_token": f"raw.id.token.{i+1}",
                    }
                ),
            )
            mock_library_auth.oidc_provider_lookup.return_value = mock_provider
            mock_library_auths.append(mock_library_auth)
            mock_providers.append(mock_provider)

            # Register library authenticator
            controller._authenticator.library_authenticators[library.short_name] = (
                mock_library_auth
            )

        db.session.commit()

        # Test logout from the specified library
        target_library = libraries[library_index]
        target_auth = mock_library_auths[library_index]
        target_auth_manager = mock_auth_managers[library_index]

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
                return_value=target_library,
            ),
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
        ):
            mock_url_for.return_value = "https://cm.test/oidc_logout_callback"

            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            # Verify correct library's authenticator was used
            target_auth.oidc_provider_lookup.assert_called_once_with("Test OIDC")

            # Verify other library's authenticator was not called
            for i, auth in enumerate(mock_library_auths):
                if i != library_index:
                    auth.oidc_provider_lookup.assert_not_called()

            # Verify correct provider's logout URL
            assert result.status_code == 302
            assert logout_url in result.location

    @pytest.mark.parametrize(
        "params,expected_message",
        [
            pytest.param(
                {
                    "post_logout_redirect_uri": "https://app.example.com/logout/callback",
                },
                "Missing 'provider' parameter in logout request",
                id="missing-provider",
            ),
            pytest.param(
                {
                    "provider": "Test OIDC",
                },
                "Missing 'post_logout_redirect_uri' parameter in logout request",
                id="missing-post-logout-redirect-uri",
            ),
        ],
    )
    def test_oidc_logout_initiate_missing_parameter(
        self, logout_controller, db, params, expected_message
    ):
        """Test OIDC logout initiate with missing required parameters."""
        result = logout_controller.oidc_logout_initiate(
            params, db.session, auth_header=""
        )

        assert result == OIDC_INVALID_REQUEST.detailed(expected_message)

    def test_oidc_logout_initiate_no_authenticator_for_library(
        self, logout_controller, db
    ):
        """Test logout initiate when library has no authenticator configured."""
        library = db.default_library()

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }

            result = logout_controller.oidc_logout_initiate(
                params, db.session, auth_header=""
            )

            assert result.uri == OIDC_INVALID_REQUEST.uri
            assert "No authenticator found for library" in result.detail

    @pytest.mark.parametrize(
        "auth_header,decode_side_effect,expected_detail",
        [
            pytest.param(
                None,
                None,
                "Missing or invalid Authorization header",
                id="missing-authorization-header",
            ),
            pytest.param(
                "NotBearer token",
                None,
                "Missing or invalid Authorization header",
                id="non-bearer-scheme",
            ),
            pytest.param(
                "Bearer invalid.jwt",
                jwt.exceptions.InvalidTokenError("bad token"),
                "Invalid bearer token",
                id="invalid-jwt",
            ),
            pytest.param(
                "Bearer valid.jwt",
                None,
                "Invalid credential data",
                id="invalid-token-json",
            ),
        ],
    )
    def test_oidc_logout_initiate_bearer_token_errors(
        self,
        logout_controller: OIDCController,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
        auth_header: str | None,
        decode_side_effect: jwt.exceptions.InvalidTokenError | None,
        expected_detail: str,
    ) -> None:
        """Test logout initiate bearer token validation errors."""
        caplog.set_level(logging.WARNING)
        library = db.default_library()

        mock_library_auth = Mock()
        mock_provider = Mock()

        if decode_side_effect:
            mock_library_auth.decode_bearer_token.side_effect = decode_side_effect
        else:
            # Return non-JSON so the token data parsing fails for the invalid-token-json case
            mock_library_auth.decode_bearer_token.return_value = (
                "Test OIDC",
                "not-valid-json",
            )

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider

        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        params = {
            "provider": "Test OIDC",
            "post_logout_redirect_uri": "https://app.example.com/logout/callback",
        }

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            result = logout_controller.oidc_logout_initiate(
                params,
                db.session,
                auth_header="" if auth_header is None else auth_header,
            )

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert result.detail is not None
        assert expected_detail in result.detail
        if decode_side_effect:
            assert "Invalid bearer token in logout request" in caplog.text

    def test_oidc_logout_initiate_unknown_provider(self, logout_controller, db):
        """Test logout initiate with unknown provider."""
        library = db.default_library()
        mock_library_auth = Mock()
        mock_library_auth.decode_bearer_token.return_value = (
            "Unknown",
            "provider-token",
        )
        mock_library_auth.oidc_provider_lookup.return_value = UNKNOWN_OIDC_PROVIDER

        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            params = {
                "provider": "Unknown",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = logout_controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            assert result == UNKNOWN_OIDC_PROVIDER

    def test_oidc_logout_initiate_provider_mismatch(
        self, logout_controller: OIDCController, db: DatabaseTransactionFixture
    ) -> None:
        """Test logout initiate when bearer token was issued by a different provider.

        A patron authenticated with provider A must not be able to initiate logout
        against provider B by supplying provider=B in the request parameters.
        """
        library = db.default_library()
        mock_library_auth = Mock()
        mock_library_auth.decode_bearer_token.return_value = (
            "Provider A",
            "provider-token",
        )
        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            params = {
                "provider": "Provider B",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = logout_controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert result.detail is not None
        assert "Provider mismatch in bearer token" in result.detail

    @pytest.mark.parametrize(
        "provider_token,expected_message",
        [
            pytest.param(
                "not-valid-json",
                "Invalid credential data",
                id="invalid-json-in-token",
            ),
            pytest.param(
                json.dumps(
                    {"id_token_claims": {"iss": "issuer"}, "access_token": "tok"}
                ),
                "Credential missing patron identifier claim",
                id="missing-patron-claim",
            ),
        ],
    )
    def test_oidc_logout_initiate_credential_data_errors(
        self,
        logout_controller,
        db,
        provider_token,
        expected_message,
    ):
        """Test logout initiate errors when token data in bearer token is invalid."""
        library = db.default_library()

        mock_library_auth = Mock()
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            provider_token,
        )
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()

        mock_auth_manager = Mock()
        mock_provider.get_authentication_manager.return_value = mock_auth_manager

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider
        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = logout_controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            assert result.uri == OIDC_INVALID_REQUEST.uri
            assert expected_message in result.detail

    def test_oidc_logout_initiate_missing_patron_claim_revokes_token(
        self, logout_controller: OIDCController, db: DatabaseTransactionFixture
    ) -> None:
        """Refresh token is revoked even when the patron identifier claim is absent.

        Without a patron identifier we cannot look up or invalidate the patron's
        credentials, but we can still revoke the refresh token to close the IdP session.
        """
        library = db.default_library()
        token_data = {
            "id_token_claims": {"iss": "issuer"},  # 'sub' / patron_id_claim absent
            "access_token": "access-token",
            "refresh_token": "refresh-token",
        }

        mock_library_auth = Mock()
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            json.dumps(token_data),
        )
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"

        mock_auth_manager = Mock()
        mock_provider.get_authentication_manager.return_value = mock_auth_manager
        mock_library_auth.oidc_provider_lookup.return_value = mock_provider

        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            result = logout_controller.oidc_logout_initiate(
                {
                    "provider": "Test OIDC",
                    "post_logout_redirect_uri": "https://app.example.com/logout/callback",
                },
                db.session,
                auth_header="Bearer valid.jwt.token",
            )

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert result.detail is not None
        assert "Credential missing patron identifier claim" in result.detail
        mock_auth_manager.revoke_token.assert_called_once_with("refresh-token")

    @pytest.mark.parametrize(
        "patron_found,invalidation_error,build_url_error,expected_status,expected_uri",
        [
            pytest.param(
                False,
                None,
                None,
                302,
                None,
                id="patron-not-found",
            ),
            pytest.param(
                True,
                SQLAlchemyError("Database error"),
                None,
                302,
                None,
                id="credential-invalidation-exception",
            ),
            pytest.param(
                True,
                None,
                OIDCAuthenticationError("Logout not supported"),
                None,
                "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/logout-not-supported",
                id="build-logout-url-exception",
            ),
        ],
    )
    def test_oidc_logout_initiate_exceptions(
        self,
        logout_controller: OIDCController,
        db: DatabaseTransactionFixture,
        patron_found: bool,
        invalidation_error: SQLAlchemyError | None,
        build_url_error: OIDCAuthenticationError | None,
        expected_status: int | None,
        expected_uri: str | None,
    ) -> None:
        """Test logout initiate with patron lookup and exception scenarios."""
        library = db.default_library()
        patron = db.patron()

        token_data: dict = {
            "id_token_claims": {"sub": "user@test.com"},
            "access_token": "access-token",
        }
        if build_url_error:
            # RP-Initiated Logout path — needs id_token and the method to return True
            token_data["id_token"] = "test.id.token"

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "test-secret"
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            json.dumps(token_data),
        )
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()

        if patron_found:
            mock_provider._credential_manager.lookup_patron_by_identifier.return_value = Mock(
                id=patron.id
            )
            if invalidation_error:
                mock_provider._credential_manager.invalidate_patron_credentials.side_effect = (
                    invalidation_error
                )
            else:
                mock_provider._credential_manager.invalidate_patron_credentials.return_value = (
                    1
                )
        else:
            mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
                None
            )

        mock_auth_manager = Mock()
        mock_auth_manager.supports_rp_initiated_logout.return_value = bool(
            build_url_error
        )
        if build_url_error:
            mock_auth_manager.build_logout_url.side_effect = build_url_error
        else:
            mock_auth_manager.build_logout_url.return_value = (
                "https://oidc.provider.test/logout"
            )
        mock_provider.get_authentication_manager.return_value = mock_auth_manager

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider
        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with (
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
                return_value=library,
            ),
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.OIDCUtility"
            ) as MockOIDCUtility,
        ):
            mock_url_for.return_value = "https://cm.test/oidc_logout_callback"

            params = {
                "provider": "Test OIDC",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }
            result = logout_controller.oidc_logout_initiate(
                params, db.session, auth_header="Bearer valid.jwt.token"
            )

            if expected_status:
                assert result.status_code == expected_status
                if not patron_found:
                    mock_provider._credential_manager.invalidate_patron_credentials.assert_not_called()
            else:
                assert result.uri == expected_uri
                if build_url_error:
                    mock_utility = MockOIDCUtility.return_value
                    stored_state = mock_utility.store_logout_state.call_args[0][0]
                    mock_utility.delete_logout_state.assert_called_once_with(
                        stored_state
                    )

    def test_oidc_logout_initiate_credential_invalidation_is_nonfatal(
        self, logout_controller: OIDCController, db: DatabaseTransactionFixture
    ) -> None:
        """Credential invalidation failure must not abort the logout flow.

        Token revocation and the final redirect must still happen even when
        invalidate_patron_credentials raises SQLAlchemyError.
        """
        library = db.default_library()
        patron = db.patron()

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "test-secret"
        mock_library_auth.decode_bearer_token.return_value = (
            "Test OIDC",
            json.dumps(
                {
                    "id_token_claims": {"sub": "user@test.com"},
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                }
            ),
        )
        mock_provider = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            Mock(id=patron.id)
        )
        mock_provider._credential_manager.invalidate_patron_credentials.side_effect = (
            SQLAlchemyError("DB error")
        )

        mock_auth_manager = Mock()
        mock_auth_manager.supports_rp_initiated_logout.return_value = False
        mock_provider.get_authentication_manager.return_value = mock_auth_manager
        mock_library_auth.oidc_provider_lookup.return_value = mock_provider
        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with patch(
            "palace.manager.integration.patron_auth.oidc.controller.get_request_library",
            return_value=library,
        ):
            result = logout_controller.oidc_logout_initiate(
                {
                    "provider": "Test OIDC",
                    "post_logout_redirect_uri": "https://app.example.com/logout/callback",
                },
                db.session,
                auth_header="Bearer valid.jwt.token",
            )

        assert result.status_code == 302
        assert "logout_status=success" in result.location
        mock_auth_manager.revoke_token.assert_called_once_with("refresh-token")

    def test_oidc_logout_callback_success(self, logout_controller, db):
        library = db.default_library()

        logout_state_data = {
            "provider_name": "Test OIDC",
            "library_short_name": library.short_name,
        }

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "test-secret"

        state_token = OIDCUtility.generate_state(logout_state_data, "test-secret")

        # Set up library_authenticators dict
        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        # Use the mock redis from the logout_controller fixture
        mock_redis = logout_controller._circulation_manager.services.redis.client()

        utility = OIDCUtility(mock_redis)
        utility.store_logout_state(
            state_token,
            "https://app.example.com/logout/callback",
        )

        params = {"state": state_token}

        result = logout_controller.oidc_logout_callback(params, db.session)

        assert result.status_code == 302
        assert "https://app.example.com/logout/callback" in result.location
        assert "logout_status=success" in result.location

    def test_oidc_logout_callback_missing_state(self, logout_controller, db):
        params = {}

        result = logout_controller.oidc_logout_callback(params, db.session)

        assert result == OIDC_INVALID_REQUEST.detailed(
            "Missing 'state' parameter in logout callback"
        )

    @pytest.mark.parametrize(
        "state_exists,state_metadata,error_message",
        [
            pytest.param(
                False,
                None,
                "Logout state not found or expired",
                id="state-not-found",
            ),
            pytest.param(
                True,
                {},
                "Missing redirect_uri in logout state",
                id="missing-redirect-uri",
            ),
            pytest.param(
                True,
                {"redirect_uri": "https://app.example.com"},
                "Missing library in logout state",
                id="missing-library",
            ),
        ],
    )
    def test_oidc_logout_callback_invalid_state(
        self, logout_controller, db, state_exists, state_metadata, error_message
    ):
        """Test logout callback with invalid or incomplete logout state."""
        library = db.default_library()

        if state_exists:
            logout_state_data = {"provider_name": "Test OIDC"}
            logout_state_data.update(state_metadata)
            state_token = OIDCUtility.generate_state(logout_state_data, "test-secret")

            mock_redis = logout_controller._circulation_manager.services.redis.client()
            utility = OIDCUtility(mock_redis)
            utility.store_logout_state(
                state_token, state_metadata.get("redirect_uri", "")
            )
        else:
            state_token = "nonexistent-state-token"

        params = {"state": state_token}

        result = logout_controller.oidc_logout_callback(params, db.session)

        assert result.uri == OIDC_INVALID_STATE.uri
        assert error_message in result.detail

    def test_oidc_logout_callback_no_authenticator_for_library(
        self, logout_controller, db
    ):
        """Test logout callback when library has no authenticator configured."""
        library = db.default_library()

        logout_state_data = {
            "provider_name": "Test OIDC",
            "library_short_name": library.short_name,
            "redirect_uri": "https://app.example.com/logout/callback",
        }

        state_token = OIDCUtility.generate_state(logout_state_data, "test-secret")

        mock_redis = logout_controller._circulation_manager.services.redis.client()
        utility = OIDCUtility(mock_redis)
        utility.store_logout_state(
            state_token,
            "https://app.example.com/logout/callback",
        )

        params = {"state": state_token}

        result = logout_controller.oidc_logout_callback(params, db.session)

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert "No authenticator found for library" in result.detail

    def test_oidc_logout_callback_malformed_state_payload(
        self, logout_controller: OIDCController, db: DatabaseTransactionFixture
    ) -> None:
        """Test logout callback when the state token cannot be decoded at all.

        A token that has no '.' separator fails decode_state_payload before we
        can even attempt signature validation.
        """
        malformed_state = "not-a-valid-state-token"

        mock_redis = logout_controller._circulation_manager.services.redis.client()
        utility = OIDCUtility(mock_redis)
        utility.store_logout_state(
            malformed_state, "https://app.example.com/logout/callback"
        )

        result = logout_controller.oidc_logout_callback(
            {"state": malformed_state}, db.session
        )

        assert result.uri == OIDC_INVALID_STATE.uri
        assert result.detail is not None
        assert "Invalid state parameter format" in result.detail

    def test_oidc_logout_callback_state_validation_exception(
        self, logout_controller, db
    ):
        """Test logout callback when state signature validation fails."""
        library = db.default_library()

        # Generate a structurally valid state token signed with the wrong secret
        # so that decode_state_payload succeeds but validate_state fails.
        logout_state_data = {
            "provider_name": "Test OIDC",
            "library_short_name": library.short_name,
            "redirect_uri": "https://app.example.com/logout/callback",
        }
        state_token = OIDCUtility.generate_state(logout_state_data, "wrong-secret")

        mock_library_auth = Mock()
        mock_library_auth.bearer_token_signing_secret = "correct-secret"

        logout_controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        mock_redis = logout_controller._circulation_manager.services.redis.client()
        utility = OIDCUtility(mock_redis)
        utility.store_logout_state(
            state_token,
            "https://app.example.com/logout/callback",
        )

        params = {"state": state_token}

        result = logout_controller.oidc_logout_callback(params, db.session)

        assert result.uri == OIDC_INVALID_STATE.uri
        assert "State validation failed" in result.detail


class TestOIDCControllerBackChannelLogout:
    """Tests for OIDC back-channel logout flow."""

    @pytest.fixture
    def mock_redis(self, redis_fixture):
        return redis_fixture.client

    @pytest.fixture
    def mock_circulation_manager(self, mock_redis):
        cm = MagicMock()
        cm.services.redis.client.return_value = mock_redis
        return cm

    @pytest.fixture
    def mock_authenticator(self):
        authenticator = MagicMock()
        authenticator.library_authenticators = {}
        return authenticator

    @pytest.fixture
    def backchannel_controller(self, mock_circulation_manager, mock_authenticator):
        return OIDCController(mock_circulation_manager, mock_authenticator)

    def test_oidc_backchannel_logout_success(self, backchannel_controller, db):
        """Test successful back-channel logout."""
        patron = db.patron()
        patron.authorization_identifier = "user123@example.com"
        db.session.commit()

        library = db.default_library()

        # Create mock provider with spec so isinstance checks work
        mock_provider = Mock(spec=BaseOIDCAuthenticationProvider)
        mock_provider.library_id = library.id
        mock_provider._authentication_manager_factory = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()

        # Mock auth manager that validates the logout token
        mock_auth_manager = Mock()
        mock_auth_manager.validate_logout_token.return_value = {
            "sub": "user123@example.com",
            "iss": "https://oidc.provider.test",
            "aud": "test-client-id",
            "iat": 1234567890,
            "jti": "unique-token-id",
            "events": {"http://schemas.openid.net/event/backchannel-logout": {}},
        }
        mock_provider._authentication_manager_factory.create.return_value = (
            mock_auth_manager
        )

        # Mock credential manager
        mock_patron = Mock()
        mock_patron.id = patron.id
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            mock_patron
        )
        mock_provider._credential_manager.invalidate_patron_credentials.return_value = 1
        mock_provider.label.return_value = "Test OIDC"

        # Set up library authenticator with the provider
        mock_library_auth = Mock()
        mock_library_auth.providers = [mock_provider]
        backchannel_controller._authenticator.library_authenticators[
            library.short_name
        ] = mock_library_auth

        # Send back-channel logout request
        form_data = {"logout_token": "test.logout.token"}

        body, status = backchannel_controller.oidc_backchannel_logout(
            form_data, db.session
        )

        assert status == 200
        assert body == ""
        mock_auth_manager.validate_logout_token.assert_called_once_with(
            "test.logout.token"
        )
        mock_provider._credential_manager.invalidate_patron_credentials.assert_called_once()

    def test_oidc_backchannel_logout_missing_token(self, backchannel_controller, db):
        """Test back-channel logout with missing logout token."""
        form_data = {}

        body, status = backchannel_controller.oidc_backchannel_logout(
            form_data, db.session
        )

        assert status == 400
        assert body == ""

    def test_oidc_backchannel_logout_invalid_token(self, backchannel_controller, db):
        """Test back-channel logout with invalid token."""
        library = db.default_library()

        # Create mock provider that rejects the token
        mock_provider = Mock()
        mock_provider._authentication_manager_factory = Mock()

        mock_auth_manager = Mock()
        mock_auth_manager.validate_logout_token.side_effect = Exception("Invalid token")
        mock_provider._authentication_manager_factory.create.return_value = (
            mock_auth_manager
        )
        mock_provider.label.return_value = "Test OIDC"

        # Set up library authenticator
        mock_library_auth = Mock()
        mock_library_auth.providers = [mock_provider]
        backchannel_controller._authenticator.library_authenticators[
            library.short_name
        ] = mock_library_auth

        form_data = {"logout_token": "invalid.token"}

        body, status = backchannel_controller.oidc_backchannel_logout(
            form_data, db.session
        )

        assert status == 400
        assert body == ""

    def test_oidc_backchannel_logout_patron_not_found(self, backchannel_controller, db):
        """Test back-channel logout when patron doesn't exist."""
        library = db.default_library()

        # Create mock provider with spec so isinstance checks work
        mock_provider = Mock(spec=BaseOIDCAuthenticationProvider)
        mock_provider.library_id = library.id
        mock_provider._authentication_manager_factory = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()

        mock_auth_manager = Mock()
        mock_auth_manager.validate_logout_token.return_value = {
            "sub": "nonexistent@example.com",
            "iss": "https://oidc.provider.test",
            "aud": "test-client-id",
            "iat": 1234567890,
            "jti": "unique-token-id",
            "events": {"http://schemas.openid.net/event/backchannel-logout": {}},
        }
        mock_provider._authentication_manager_factory.create.return_value = (
            mock_auth_manager
        )

        # Patron not found
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            None
        )
        mock_provider.label.return_value = "Test OIDC"

        # Set up library authenticator
        mock_library_auth = Mock()
        mock_library_auth.providers = [mock_provider]
        backchannel_controller._authenticator.library_authenticators[
            library.short_name
        ] = mock_library_auth

        form_data = {"logout_token": "test.logout.token"}

        body, status = backchannel_controller.oidc_backchannel_logout(
            form_data, db.session
        )

        # Should still return 200 even if patron not found
        assert status == 200
        assert body == ""

    def test_oidc_backchannel_logout_missing_patron_identifier_claim(
        self, backchannel_controller, db
    ):
        """Test back-channel logout when logout token is missing patron identifier claim."""
        library = db.default_library()

        mock_provider = Mock(spec=BaseOIDCAuthenticationProvider)
        mock_provider.library_id = library.id
        mock_provider._authentication_manager_factory = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"

        mock_auth_manager = Mock()
        mock_auth_manager.validate_logout_token.return_value = {
            "iss": "https://oidc.provider.test",
            "aud": "test-client-id",
            "iat": 1234567890,
            "jti": "unique-token-id",
            "events": {"http://schemas.openid.net/event/backchannel-logout": {}},
        }
        mock_provider._authentication_manager_factory.create.return_value = (
            mock_auth_manager
        )
        mock_provider.label.return_value = "Test OIDC"

        mock_library_auth = Mock()
        mock_library_auth.providers = [mock_provider]
        backchannel_controller._authenticator.library_authenticators[
            library.short_name
        ] = mock_library_auth

        form_data = {"logout_token": "test.logout.token"}

        body, status = backchannel_controller.oidc_backchannel_logout(
            form_data, db.session
        )

        assert status == 400
        assert body == ""

    def test_oidc_backchannel_logout_no_provider_validates_token(
        self, backchannel_controller, db
    ):
        """Test back-channel logout when no provider can validate the token."""
        library = db.default_library()

        # Create multiple OIDC providers that all reject the token
        mock_provider1 = Mock(spec=BaseOIDCAuthenticationProvider)
        mock_provider1._authentication_manager_factory = Mock()
        mock_auth_manager1 = Mock()
        mock_auth_manager1.validate_logout_token.side_effect = Exception(
            "Provider 1 cannot validate"
        )
        mock_provider1._authentication_manager_factory.create.return_value = (
            mock_auth_manager1
        )
        mock_provider1.label.return_value = "Test OIDC 1"

        mock_provider2 = Mock(spec=BaseOIDCAuthenticationProvider)
        mock_provider2._authentication_manager_factory = Mock()
        mock_auth_manager2 = Mock()
        mock_auth_manager2.validate_logout_token.side_effect = Exception(
            "Provider 2 cannot validate"
        )
        mock_provider2._authentication_manager_factory.create.return_value = (
            mock_auth_manager2
        )
        mock_provider2.label.return_value = "Test OIDC 2"

        mock_library_auth = Mock()
        mock_library_auth.providers = [mock_provider1, mock_provider2]
        backchannel_controller._authenticator.library_authenticators[
            library.short_name
        ] = mock_library_auth

        form_data = {"logout_token": "test.logout.token"}

        body, status = backchannel_controller.oidc_backchannel_logout(
            form_data, db.session
        )

        assert status == 400
        assert body == ""
