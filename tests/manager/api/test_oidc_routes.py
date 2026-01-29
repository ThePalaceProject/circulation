"""Tests for OIDC routes."""

from unittest.mock import MagicMock, patch

from werkzeug.datastructures import ImmutableMultiDict

from tests.fixtures.api_controller import ControllerFixture


class TestOIDCRoutes:
    def test_oidc_authenticate_route(self, controller_fixture: ControllerFixture):
        """Test that /oidc_authenticate route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/default/oidc_authenticate?provider=OpenID+Connect&redirect_uri=https://app.example.com"
            ),
            patch.object(
                controller_fixture.manager.oidc_controller,
                "oidc_authentication_redirect",
            ) as mock_redirect,
        ):
            mock_redirect.return_value = MagicMock(status_code=302)

            from palace.manager.api.routes import oidc_authenticate

            response = oidc_authenticate()

            assert response.status_code == 302
            mock_redirect.assert_called_once()
            call_args = mock_redirect.call_args
            assert isinstance(call_args[0][0], ImmutableMultiDict)
            assert call_args[0][0]["provider"] == "OpenID Connect"
            assert call_args[0][0]["redirect_uri"] == "https://app.example.com"

    def test_oidc_callback_route(self, controller_fixture: ControllerFixture):
        """Test that /oidc_callback route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/oidc_callback?code=test-code&state=test-state"
            ),
            patch.object(
                controller_fixture.manager.oidc_controller,
                "oidc_authentication_callback",
            ) as mock_callback,
        ):
            mock_callback.return_value = MagicMock(status_code=302)

            from palace.manager.api.routes import oidc_callback

            response = oidc_callback()

            assert response.status_code == 302
            mock_callback.assert_called_once()
            call_args = mock_callback.call_args
            assert isinstance(call_args[0][0], ImmutableMultiDict)
            assert call_args[0][0]["code"] == "test-code"
            assert call_args[0][0]["state"] == "test-state"
