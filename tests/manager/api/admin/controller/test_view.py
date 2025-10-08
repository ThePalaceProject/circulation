import re
from unittest.mock import MagicMock, patch

import flask
import pytest
from _pytest.monkeypatch import MonkeyPatch
from werkzeug.http import dump_cookie

from palace.manager.api.admin.config import AdminClientFeatureFlags, AdminClientSettings
from palace.manager.api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from palace.manager.sqlalchemy.model.admin import AdminRole
from palace.manager.sqlalchemy.model.library import Library
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.test_utils import MonkeyPatchEnvFixture


class TestViewController:
    def test_setting_up(self, admin_ctrl_fixture: AdminControllerFixture):
        # Test that the view is in setting-up mode if there's no auth service
        # and no admin with a password.
        admin_ctrl_fixture.admin.password_hashed = None

        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert "settingUp: true" in html

    def test_not_setting_up(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(
                "collection", "book"
            )
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert "settingUp: false" in html

    def test_redirect_to_sign_in(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/web/collection/a/(b)/book/c/(d)"
        ):
            response = admin_ctrl_fixture.manager.admin_view_controller(
                "a/(b)", "c/(d)"
            )
            assert 302 == response.status_code
            location = response.headers.get("Location")
            assert "sign_in" in location
            assert "admin/web" in location
            assert "collection/a%252F(b)" in location
            assert "book/c%252F(d)" in location

    def test_redirect_to_library(self, admin_ctrl_fixture: AdminControllerFixture):
        # If the admin doesn't have access to any libraries, they get a message
        # instead of a redirect.
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            assert (
                "Your admin account doesn't have access to any libraries"
                in response.get_data(as_text=True)
            )

        # Unless there aren't any libraries yet. In that case, an admin needs to
        # get in to create one.
        for library in admin_ctrl_fixture.ctrl.db.session.query(Library):
            admin_ctrl_fixture.ctrl.db.session.delete(library)
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            assert "<body>" in response.get_data(as_text=True)

        l1 = admin_ctrl_fixture.ctrl.db.library(short_name="L1")
        l2 = admin_ctrl_fixture.ctrl.db.library(short_name="L2")
        l3 = admin_ctrl_fixture.ctrl.db.library(short_name="L3")
        admin_ctrl_fixture.admin.add_role(AdminRole.LIBRARIAN, l1)
        admin_ctrl_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, l3)
        # An admin with roles gets redirected to the oldest library they have access to.
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 302 == response.status_code
            location = response.headers.get("Location")
            assert "admin/web/collection/%s" % l1.short_name in location

        # Only the root url redirects - a non-library specific page with another
        # path won't.
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/web/config"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(
                None, None, "config"
            )
            assert 200 == response.status_code

    def test_csrf_token(self, admin_ctrl_fixture: AdminControllerFixture):
        admin_ctrl_fixture.admin.password_hashed = None
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)

            # The CSRF token value is random, but the cookie and the html have the same value.
            html_csrf_re = re.compile('csrfToken: "([^"]*)"')
            match = html_csrf_re.search(html)
            assert match is not None
            csrf = match.groups(0)[0]
            set_cookie = response.headers.get("Set-Cookie")
            assert csrf in set_cookie
            assert "HttpOnly" in set_cookie
            assert "SameSite=Lax" in set_cookie
            # In test mode (debug=False by default), secure flag should be set
            assert "Secure" in set_cookie

        admin_ctrl_fixture.admin.password = "password"
        # If there's an invalid CSRF token in the request cookie, a new one should be generated
        invalid_token = admin_ctrl_fixture.ctrl.db.fresh_str()
        cookie = dump_cookie("csrf_token", invalid_token)
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin", environ_base={"HTTP_COOKIE": cookie}
        ):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(
                "collection", "book"
            )
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            # The invalid token should NOT be in the HTML
            assert 'csrfToken: "%s"' % invalid_token not in html
            # A new token should be set in the cookie
            assert response.headers.get("Set-Cookie") is not None
            assert "csrf_token" in response.headers.get("Set-Cookie")

        # If there's a valid CSRF token in the request cookie, the response
        # should use that same token in the HTML but NOT set it again
        # (to prevent echoing user-controlled values back as cookies).
        from palace.manager.api.admin.controller.base import AdminController

        valid_token = AdminController.generate_csrf_token()
        cookie = dump_cookie("csrf_token", valid_token)
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin", environ_base={"HTTP_COOKIE": cookie}
        ):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(
                "collection", "book"
            )
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            # The valid token should be in the HTML
            assert 'csrfToken: "%s"' % valid_token in html
            # But should NOT be set again in the cookie header
            # (only new tokens are set, not echoed back from requests)
            assert response.headers.get("Set-Cookie") is None

    def test_csrf_token_validation(self, admin_ctrl_fixture: AdminControllerFixture):
        """Test that CSRF tokens are properly validated before being used."""
        from palace.manager.api.admin.controller.base import AdminController

        # Test various invalid token formats
        invalid_tokens = [
            "",  # empty string
            "short",  # too short
            "a" * 100,  # too long
            "not-base64-!@#$%^&*()",  # invalid base64
            "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo=",  # valid base64 but wrong length when decoded
            None,  # None value
        ]

        for invalid_token in invalid_tokens:
            if invalid_token is not None:
                assert not AdminController.validate_csrf_token(invalid_token)

        # Test a valid token
        valid_token = AdminController.generate_csrf_token()
        assert AdminController.validate_csrf_token(valid_token)
        assert len(valid_token) == 32

    def test_csrf_token_debug_mode(self, admin_ctrl_fixture: AdminControllerFixture):
        """Test that CSRF cookie security settings respect debug mode."""
        admin_ctrl_fixture.admin.password_hashed = None

        # Test with debug mode enabled
        admin_ctrl_fixture.ctrl.app.config["DEBUG"] = True
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            set_cookie = response.headers.get("Set-Cookie")
            assert set_cookie is not None
            assert "csrf_token" in set_cookie
            assert "HttpOnly" in set_cookie
            assert "SameSite=Lax" in set_cookie
            # In debug mode, secure flag should NOT be set
            assert "Secure" not in set_cookie

        # Test with debug mode disabled (production mode)
        admin_ctrl_fixture.ctrl.app.config["DEBUG"] = False
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            set_cookie = response.headers.get("Set-Cookie")
            assert set_cookie is not None
            assert "csrf_token" in set_cookie
            assert "HttpOnly" in set_cookie
            assert "SameSite=Lax" in set_cookie
            # In production mode, secure flag SHOULD be set
            assert "Secure" in set_cookie

    def test_show_circ_events_download(
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        # The local analytics provider will be configured by default if
        # there isn't one.
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(
                "collection", "book"
            )
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert "showCircEventsDownload: true" in html

    def test_roles(self, admin_ctrl_fixture: AdminControllerFixture):
        admin_ctrl_fixture.admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        admin_ctrl_fixture.admin.add_role(
            AdminRole.LIBRARY_MANAGER, admin_ctrl_fixture.ctrl.db.default_library()
        )
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = admin_ctrl_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = admin_ctrl_fixture.manager.admin_view_controller(
                "collection", "book"
            )
            assert 200 == response.status_code
            html = response.get_data(as_text=True)
            assert '"role": "librarian-all"' in html
            assert (
                '"role": "manager", "library": "%s"'
                % admin_ctrl_fixture.ctrl.db.default_library().short_name
                in html
            )

    @patch("palace.manager.api.admin.config.Configuration.admin_client_settings")
    @pytest.mark.parametrize(
        "url, text, expected_text",
        (
            pytest.param(
                "mailto:support@example.com?subject=support request",
                None,
                "Email support@example.com.",
                id="mailto-url-no-text",
            ),
            pytest.param(
                "https://support.example.com/path/to/support",
                None,
                AdminClientSettings.DEFAULT_SUPPORT_CONTACT_TEXT,
                id="non-mailto-url-no-text",
            ),
            pytest.param(
                "mailto:support@example.com?subject=support request",
                "Reach out to the support team.",
                "Reach out to the support team.",
                id="mailto-url-with-text",
            ),
            pytest.param(
                "https://support.example.com/path/to/support",
                "Get help at our web site.",
                "Get help at our web site.",
                id="non-mailto-url-with-text",
            ),
            pytest.param(
                None,
                None,
                None,
                id="no-url-no-text",
            ),
            pytest.param(
                None,
                "Contact us!",
                "Contact us!",
                id="no-url-with-text",
            ),
        ),
    )
    def test_support_contact(
        self,
        admin_client_settings: MagicMock,
        admin_ctrl_fixture: AdminControllerFixture,
        monkeypatch: pytest.MonkeyPatch,
        monkeypatch_env: MonkeyPatchEnvFixture,
        url: str | None,
        text: str | None,
        expected_text: str | None,
    ):
        admin_ctrl_fixture.admin.password_hashed = None

        monkeypatch_env("PALACE_ADMINUI_SUPPORT_CONTACT_URL", url)
        monkeypatch_env("PALACE_ADMINUI_SUPPORT_CONTACT_TEXT", text)

        def assert_expected(content: str, config_key: str, value: str | None):
            if value is None:
                assert f"{config_key}:" not in content
            else:
                assert f'{config_key}: "{value}"' in content

        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            # Ensure that we will get the most current values from the environment.
            admin_client_settings.return_value = AdminClientSettings()

            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)

            assert_expected(html, "supportContactText", expected_text)
            assert_expected(html, "supportContactUrl", url)
            # TODO: `support_contact_url` is deprecated in the admin client
            #  and will be removed in a future release. The following line
            #  can be removed at that time.
            assert_expected(html, "support_contact_url", url)

    def test_feature_flags_defaults(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        monkeypatch: pytest.MonkeyPatch,
    ):
        admin_ctrl_fixture.admin.password_hashed = None
        html_feature_flags_re = re.compile(
            r"featureFlags: {(.*)?}", re.MULTILINE | re.DOTALL
        )

        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)

            match = html_feature_flags_re.search(html)
            assert match is not None
            feature_flags: str = match.groups(0)[0]  # type: ignore[assignment]
            assert '"enableAutoList":true' in feature_flags
            assert '"showCircEventsDownload":true' in feature_flags
            assert '"reportsOnlyForSysadmins":true' in feature_flags
            assert '"quicksightOnlyForSysadmins":true' in feature_flags

    def test_feature_flags_overridden(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        monkeypatch: MonkeyPatch,
    ):
        admin_ctrl_fixture.admin.password_hashed = None
        html_feature_flags_re = re.compile(
            r"featureFlags: {(.*)?}", re.MULTILINE | re.DOTALL
        )

        monkeypatch.setenv("PALACE_ADMINUI_FEATURE_REPORTS_ONLY_FOR_SYSADMINS", "false")
        monkeypatch.setenv(
            "PALACE_ADMINUI_FEATURE_QUICKSIGHT_ONLY_FOR_SYSADMINS", "false"
        )

        with (
            patch(
                "palace.manager.api.admin.config.Configuration.admin_feature_flags"
            ) as admin_feature_flags,
            admin_ctrl_fixture.ctrl.app.test_request_context("/admin"),
        ):
            # Use fresh feature flags, instead of using a cached value.
            admin_feature_flags.return_value = AdminClientFeatureFlags()
            response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
            assert 200 == response.status_code
            html = response.get_data(as_text=True)

            match = html_feature_flags_re.search(html)
            assert match is not None

            feature_flags: str = match.groups(0)[0]  # type: ignore[assignment]
            assert '"enableAutoList":true' in feature_flags
            assert '"showCircEventsDownload":true' in feature_flags
            assert '"reportsOnlyForSysadmins":false' in feature_flags
            assert '"quicksightOnlyForSysadmins":false' in feature_flags
