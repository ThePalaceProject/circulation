"""Tests for OIDC controller."""

from unittest.mock import MagicMock, patch

import pytest

from palace.manager.api.problem_details import LIBRARY_NOT_FOUND, UNKNOWN_OIDC_PROVIDER
from palace.manager.integration.patron_auth.oidc.controller import (
    OIDC_INVALID_REQUEST,
    OIDC_INVALID_RESPONSE,
    OIDC_INVALID_STATE,
    OIDCController,
)
from palace.manager.integration.patron_auth.oidc.provider import (
    OIDC_CANNOT_DETERMINE_PATRON,
)
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

    def test_oidc_authentication_redirect_missing_provider(self, controller):
        params = {"redirect_uri": "https://app.example.com"}

        result = controller.oidc_authentication_redirect(params, MagicMock())

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert "provider" in result.detail

    def test_oidc_authentication_redirect_missing_redirect_uri(self, controller):
        params = {"provider": "OpenID Connect"}

        result = controller.oidc_authentication_redirect(params, MagicMock())

        assert result.uri == OIDC_INVALID_REQUEST.uri
        assert "redirect_uri" in result.detail

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
        from palace.manager.integration.patron_auth.oidc.configuration.model import (
            OIDCAuthLibrarySettings,
            OIDCAuthSettings,
        )
        from palace.manager.integration.patron_auth.oidc.provider import (
            OIDCAuthenticationProvider,
        )

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

    def test_oidc_authentication_callback_missing_code(self, controller):
        params = {"state": "test-state"}

        result = controller.oidc_authentication_callback(params, MagicMock())

        assert result.uri == OIDC_INVALID_RESPONSE.uri
        assert "authorization code" in result.detail

    def test_oidc_authentication_callback_missing_state(self, controller):
        params = {"code": "test-code"}

        result = controller.oidc_authentication_callback(params, MagicMock())

        assert result.uri == OIDC_INVALID_RESPONSE.uri
        assert "state" in result.detail

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
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.api.authentication.base import PatronData
        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from unittest.mock import Mock, patch

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
        mock_auth_manager.build_logout_url.return_value = "https://oidc.provider.test/logout?id_token_hint=test.token&post_logout_redirect_uri=https://cm.test/oidc_logout_callback&state=test-state"
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

    def test_oidc_logout_initiate_missing_provider(self, logout_controller, db):
        from palace.manager.integration.patron_auth.oidc.controller import (
            OIDC_INVALID_REQUEST,
        )

        params = {
            "id_token_hint": "test.id.token",
            "post_logout_redirect_uri": "https://app.example.com/logout/callback",
        }

        result = logout_controller.oidc_logout_initiate(params, db.session)

        assert result == OIDC_INVALID_REQUEST.detailed(
            "Missing 'provider' parameter in logout request"
        )

    def test_oidc_logout_initiate_missing_id_token_hint(self, logout_controller, db):
        from palace.manager.integration.patron_auth.oidc.controller import (
            OIDC_INVALID_ID_TOKEN_HINT,
        )

        params = {
            "provider": "Test OIDC",
            "post_logout_redirect_uri": "https://app.example.com/logout/callback",
        }

        result = logout_controller.oidc_logout_initiate(params, db.session)

        assert result == OIDC_INVALID_ID_TOKEN_HINT.detailed(
            "Missing 'id_token_hint' parameter in logout request"
        )

    def test_oidc_logout_callback_success(self, logout_controller, db):
        from unittest.mock import Mock

        from palace.manager.integration.patron_auth.oidc.util import OIDCUtility

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
        from palace.manager.integration.patron_auth.oidc.controller import (
            OIDC_INVALID_REQUEST,
        )

        params = {}

        result = logout_controller.oidc_logout_callback(params, db.session)

        assert result == OIDC_INVALID_REQUEST.detailed(
            "Missing 'state' parameter in logout callback"
        )
