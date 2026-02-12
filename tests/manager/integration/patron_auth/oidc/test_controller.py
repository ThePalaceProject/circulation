"""Tests for OIDC controller."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from palace.manager.api.authentication.base import PatronData
from palace.manager.api.authenticator import BaseOIDCAuthenticationProvider
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND, UNKNOWN_OIDC_PROVIDER
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.controller import (
    OIDC_INVALID_ID_TOKEN_HINT,
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
        mock_provider._authentication_manager_factory = Mock()
        mock_provider._settings = Mock()
        mock_provider._settings.patron_id_claim = "sub"
        mock_provider._credential_manager = Mock()
        mock_provider.integration_id = 1

        mock_auth_manager = Mock()
        mock_auth_manager.validate_id_token_hint.return_value = {
            "sub": "user123@example.com"
        }
        mock_auth_manager.build_logout_url.return_value = (
            "https://oidc.provider.test/logout"
            "?id_token_hint=test.token"
            "&post_logout_redirect_uri=https://cm.test/oidc_logout_callback&state=test-state"
        )
        mock_provider._authentication_manager_factory.create.return_value = (
            mock_auth_manager
        )

        mock_patron = Mock()
        mock_patron.id = patron.id
        mock_provider._credential_manager.lookup_patron_by_identifier.return_value = (
            mock_patron
        )
        mock_provider._credential_manager.invalidate_patron_credentials.return_value = 1

        mock_library_auth.oidc_provider_lookup.return_value = mock_provider

        # Set up library_authenticators dict
        controller._authenticator.library_authenticators[library.short_name] = (
            mock_library_auth
        )

        with (
            patch.object(
                controller._circulation_manager.index_controller,
                "library_for_request",
                return_value=library,
            ),
            patch(
                "palace.manager.integration.patron_auth.oidc.controller.url_for"
            ) as mock_url_for,
        ):
            mock_url_for.return_value = "https://cm.test/oidc_logout_callback"

            params = {
                "provider": "Test OIDC",
                "id_token_hint": "test.id.token",
                "post_logout_redirect_uri": "https://app.example.com/logout/callback",
            }

            result = controller.oidc_logout_initiate(params, db.session)

            assert result.status_code == 302
            assert "https://oidc.provider.test/logout" in result.location

            # Verify credentials were invalidated
            mock_provider._credential_manager.invalidate_patron_credentials.assert_called_once_with(
                db.session, patron.id
            )

    @pytest.mark.parametrize(
        "params,error_constant_name,expected_message",
        [
            pytest.param(
                {
                    "id_token_hint": "test.id.token",
                    "post_logout_redirect_uri": "https://app.example.com/logout/callback",
                },
                "OIDC_INVALID_REQUEST",
                "Missing 'provider' parameter in logout request",
                id="missing-provider",
            ),
            pytest.param(
                {
                    "provider": "Test OIDC",
                    "post_logout_redirect_uri": "https://app.example.com/logout/callback",
                },
                "OIDC_INVALID_ID_TOKEN_HINT",
                "Missing 'id_token_hint' parameter in logout request",
                id="missing-id-token-hint",
            ),
        ],
    )
    def test_oidc_logout_initiate_missing_parameter(
        self, logout_controller, db, params, error_constant_name, expected_message
    ):
        """Test OIDC logout initiate with missing required parameters."""
        error_constant = (
            OIDC_INVALID_REQUEST
            if error_constant_name == "OIDC_INVALID_REQUEST"
            else OIDC_INVALID_ID_TOKEN_HINT
        )

        result = logout_controller.oidc_logout_initiate(params, db.session)

        assert result == error_constant.detailed(expected_message)

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
            metadata={"library_short_name": library.short_name},
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
