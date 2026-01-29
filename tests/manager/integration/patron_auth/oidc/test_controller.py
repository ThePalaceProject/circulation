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
