"""Tests for OIDC routes."""

from unittest.mock import MagicMock, patch

from werkzeug.datastructures import ImmutableMultiDict

from tests.fixtures.api_controller import ControllerFixture


class TestOIDCRoutes:
    def test_oidc_authenticate_route(self, controller_fixture: ControllerFixture):
        """Test that /oidc/authenticate route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/default/oidc/authenticate?provider=OpenID+Connect&redirect_uri=https://app.example.com"
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
        """Test that /oidc/callback route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/oidc/callback?code=test-code&state=test-state"
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

    def test_oidc_logout_route(self, controller_fixture: ControllerFixture):
        """Test that /oidc/logout route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/default/oidc/logout?provider=OpenID+Connect&id_token_hint=test-token&post_logout_redirect_uri=https://app.example.com"
            ),
            patch.object(
                controller_fixture.manager.oidc_controller,
                "oidc_logout_initiate",
            ) as mock_logout,
        ):
            mock_logout.return_value = MagicMock(status_code=302)

            from palace.manager.api.routes import oidc_logout

            response = oidc_logout()

            assert response.status_code == 302
            mock_logout.assert_called_once()
            call_args = mock_logout.call_args
            assert isinstance(call_args[0][0], ImmutableMultiDict)
            assert call_args[0][0]["provider"] == "OpenID Connect"
            assert call_args[0][0]["id_token_hint"] == "test-token"
            assert (
                call_args[0][0]["post_logout_redirect_uri"] == "https://app.example.com"
            )

    def test_oidc_logout_callback_route(self, controller_fixture: ControllerFixture):
        """Test that /oidc/logout_callback route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/oidc/logout_callback?state=test-logout-state"
            ),
            patch.object(
                controller_fixture.manager.oidc_controller,
                "oidc_logout_callback",
            ) as mock_callback,
        ):
            mock_callback.return_value = MagicMock(status_code=302)

            from palace.manager.api.routes import oidc_logout_callback

            response = oidc_logout_callback()

            assert response.status_code == 302
            mock_callback.assert_called_once()
            call_args = mock_callback.call_args
            assert isinstance(call_args[0][0], ImmutableMultiDict)
            assert call_args[0][0]["state"] == "test-logout-state"

    def test_oidc_backchannel_logout_route(self, controller_fixture: ControllerFixture):
        """Test that /oidc/backchannel_logout route calls the controller correctly."""
        with (
            controller_fixture.app.test_request_context(
                "/oidc/backchannel_logout",
                method="POST",
                data={"logout_token": "test.logout.token"},
            ),
            patch.object(
                controller_fixture.manager.oidc_controller,
                "oidc_backchannel_logout",
            ) as mock_backchannel,
        ):
            mock_backchannel.return_value = ("", 200)

            from palace.manager.api.routes import oidc_backchannel_logout

            response = oidc_backchannel_logout()

            assert response == ("", 200)
            mock_backchannel.assert_called_once()
            call_args = mock_backchannel.call_args
            assert isinstance(call_args[0][0], ImmutableMultiDict)
            assert call_args[0][0]["logout_token"] == "test.logout.token"
