import re
from unittest.mock import patch

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
            assert csrf in response.headers.get("Set-Cookie")
            assert "HttpOnly" in response.headers.get("Set-Cookie")

        admin_ctrl_fixture.admin.password = "password"
        # If there's a CSRF token in the request cookie, the response
        # should keep that same token.
        token = admin_ctrl_fixture.ctrl.db.fresh_str()
        cookie = dump_cookie("csrf_token", token)
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
            assert 'csrfToken: "%s"' % token in html
            assert token in response.headers.get("Set-Cookie")

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

    def test_support_contact_url(
        self,
        admin_ctrl_fixture: AdminControllerFixture,
        monkeypatch: pytest.MonkeyPatch,
    ):
        admin_ctrl_fixture.admin.password_hashed = None

        setting_env_var = "PALACE_ADMINUI_SUPPORT_CONTACT_URL"
        expected_support_contact_url = "mailto:helpdesk@example.com"

        with patch(
            "palace.manager.api.admin.config.Configuration.admin_client_settings"
        ) as admin_client_settings:
            # When the setting is set, the value should be passed to the admin client.
            monkeypatch.setenv(setting_env_var, expected_support_contact_url)
            with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
                # Ensure that we will get the most current values from the environment.
                admin_client_settings.return_value = AdminClientSettings()

                response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
                assert 200 == response.status_code
                html = response.get_data(as_text=True)
                assert f'support_contact_url: "{expected_support_contact_url}"' in html

            # When the setting is not set, the setting should not be passed at all.
            monkeypatch.delenv(setting_env_var)
            with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
                # Ensure that we will get the most current values from the environment.
                admin_client_settings.return_value = AdminClientSettings()

                response = admin_ctrl_fixture.manager.admin_view_controller(None, None)
                assert 200 == response.status_code
                html = response.get_data(as_text=True)
                assert f"support_contact_url:" not in html

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
