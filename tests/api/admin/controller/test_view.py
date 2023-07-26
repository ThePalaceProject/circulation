import re

import flask
from werkzeug.http import dump_cookie

from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.config import Configuration
from core.model import AdminRole, ConfigurationSetting, Library
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

    def test_tos_link(self, admin_ctrl_fixture: AdminControllerFixture):
        def assert_tos(expect_href, expect_text):
            with admin_ctrl_fixture.ctrl.app.test_request_context("/admin"):
                flask.session["admin_email"] = admin_ctrl_fixture.admin.email
                flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
                response = admin_ctrl_fixture.manager.admin_view_controller(
                    "collection", "book"
                )
                assert 200 == response.status_code
                html = response.get_data(as_text=True)

                assert ('tos_link_href: "%s",' % expect_href) in html
                assert ('tos_link_text: "%s",' % expect_text) in html

        # First, verify the default values, which very few circulation
        # managers will have any need to change.
        #
        # The default value has an apostrophe in it, which gets
        # escaped when the HTML is generated.
        assert_tos(
            Configuration.DEFAULT_TOS_HREF,
            Configuration.DEFAULT_TOS_TEXT.replace("'", "&#39;"),
        )

        # Now set some custom values.
        sitewide = ConfigurationSetting.sitewide
        sitewide(
            admin_ctrl_fixture.ctrl.db.session, Configuration.CUSTOM_TOS_HREF
        ).value = "http://tos/"
        sitewide(
            admin_ctrl_fixture.ctrl.db.session, Configuration.CUSTOM_TOS_TEXT
        ).value = "a tos"

        # Verify that those values are picked up and used to build the page.
        assert_tos("http://tos/", "a tos")

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
