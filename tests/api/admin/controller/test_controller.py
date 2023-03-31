import csv
import datetime
import json
import re
from datetime import timedelta
from io import StringIO
from typing import Optional
from unittest import mock

import feedparser
import flask
import pytest
from attrs import define
from werkzeug.datastructures import MultiDict
from werkzeug.http import dump_cookie

from api.admin.controller import (
    AdminAnnotator,
    CustomListsController,
    PatronController,
    SettingsController,
)
from api.admin.exceptions import *
from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.problem_details import *
from api.admin.validator import Validator
from api.adobe_vendor_id import AdobeVendorIDModel, AuthdataUtility
from api.authenticator import PatronData
from api.config import Configuration
from core.classifier import genres
from core.lane import Lane, Pagination
from core.model import (
    Admin,
    AdminRole,
    CirculationEvent,
    ConfigurationSetting,
    CustomList,
    CustomListEntry,
    DataSource,
    Edition,
    ExternalIntegration,
    Genre,
    Library,
    Timestamp,
    WorkGenre,
    create,
    get_one,
    get_one_or_create,
)
from core.model.collection import Collection
from core.query.customlist import CustomListQueries
from core.s3 import S3UploaderConfiguration
from core.util.datetime_helpers import utc_now
from core.util.problem_detail import ProblemDetail
from tests.core.util.test_flask_util import add_request_context
from tests.fixtures.api_admin import AdminControllerFixture, SettingsControllerFixture
from tests.fixtures.api_controller import ControllerFixture


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
            assert match != None
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


class TestAdminCirculationManagerController:
    def test_require_system_admin(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_system_admin,
            )

            admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            admin_ctrl_fixture.manager.admin_work_controller.require_system_admin()

    def test_require_sitewide_library_manager(
        self, admin_ctrl_fixture: AdminControllerFixture
    ):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_sitewide_library_manager,
            )

            admin_ctrl_fixture.admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
            admin_ctrl_fixture.manager.admin_work_controller.require_sitewide_library_manager()

    def test_require_library_manager(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_library_manager,
                admin_ctrl_fixture.ctrl.db.default_library(),
            )

            admin_ctrl_fixture.admin.add_role(
                AdminRole.LIBRARY_MANAGER, admin_ctrl_fixture.ctrl.db.default_library()
            )
            admin_ctrl_fixture.manager.admin_work_controller.require_library_manager(
                admin_ctrl_fixture.ctrl.db.default_library()
            )

    def test_require_librarian(self, admin_ctrl_fixture: AdminControllerFixture):
        with admin_ctrl_fixture.request_context_with_admin("/admin"):
            pytest.raises(
                AdminNotAuthorized,
                admin_ctrl_fixture.manager.admin_work_controller.require_librarian,
                admin_ctrl_fixture.ctrl.db.default_library(),
            )

            admin_ctrl_fixture.admin.add_role(
                AdminRole.LIBRARIAN, admin_ctrl_fixture.ctrl.db.default_library()
            )
            admin_ctrl_fixture.manager.admin_work_controller.require_librarian(
                admin_ctrl_fixture.ctrl.db.default_library()
            )


class SignInFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.password_hashed = None


@pytest.fixture(scope="function")
def sign_in_fixture(controller_fixture: ControllerFixture) -> SignInFixture:
    return SignInFixture(controller_fixture)


class TestSignInController:
    def test_admin_auth_providers(self, sign_in_fixture: SignInFixture):
        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            ctrl = sign_in_fixture.manager.admin_sign_in_controller

            # An admin exists, but they have no password and there's
            # no auth service set up.
            assert [] == ctrl.admin_auth_providers

            # Here's an admin with a password.
            pw_admin, ignore = create(
                sign_in_fixture.ctrl.db.session, Admin, email="pw@nypl.org"
            )
            pw_admin.password = "password"
            assert 1 == len(ctrl.admin_auth_providers)
            assert {
                PasswordAdminAuthenticationProvider.NAME,
            } == {provider.NAME for provider in ctrl.admin_auth_providers}

            # Only an admin with a password is left.
            sign_in_fixture.ctrl.db.session.delete(sign_in_fixture.admin)
            assert 1 == len(ctrl.admin_auth_providers)
            assert {
                PasswordAdminAuthenticationProvider.NAME,
            } == {provider.NAME for provider in ctrl.admin_auth_providers}

            # No admins. No one can log in anymore
            sign_in_fixture.ctrl.db.session.delete(pw_admin)
            assert 0 == len(ctrl.admin_auth_providers)

    def test_admin_auth_provider(self, sign_in_fixture: SignInFixture):
        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            ctrl = sign_in_fixture.manager.admin_sign_in_controller

            # We can't find a password auth provider, since no admin has a password.
            auth = ctrl.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
            assert None == auth

            # Here's another admin with a password.
            pw_admin, ignore = create(
                sign_in_fixture.ctrl.db.session, Admin, email="pw@nypl.org"
            )
            pw_admin.password = "password"

            # Now we can find an auth provider.
            auth = ctrl.admin_auth_provider(PasswordAdminAuthenticationProvider.NAME)
            assert isinstance(auth, PasswordAdminAuthenticationProvider)

    def test_authenticated_admin_from_request(self, sign_in_fixture: SignInFixture):
        # Returns an error if there is no admin auth providers.
        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            # You get back a problem detail when you're not authenticated.
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.authenticated_admin_from_request()
            )
            assert 500 == response.status_code
            assert ADMIN_AUTH_NOT_CONFIGURED.detail == response.detail

        # Works once the admin auth service exists.
        sign_in_fixture.admin.password = "password"
        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = sign_in_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.authenticated_admin_from_request()
            )
            assert sign_in_fixture.admin == response

        # Returns an error if the admin email or auth type is missing from the session.
        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.authenticated_admin_from_request()
            )
            assert 401 == response.status_code
            assert INVALID_ADMIN_CREDENTIALS.detail == response.detail

        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = sign_in_fixture.admin.email
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.authenticated_admin_from_request()
            )
            assert 401 == response.status_code
            assert INVALID_ADMIN_CREDENTIALS.detail == response.detail

        # Returns an error if the admin authentication type isn't configured.
        with sign_in_fixture.ctrl.app.test_request_context("/admin"):
            flask.session["admin_email"] = sign_in_fixture.admin.email
            flask.session["auth_type"] = "unknown"
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.authenticated_admin_from_request()
            )
            assert 400 == response.status_code
            assert ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail == response.detail

    def test_admin_signin(self, sign_in_fixture: SignInFixture):
        # Returns an error if there's no admin auth service.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in?redirect=foo"
        ):
            response = sign_in_fixture.manager.admin_sign_in_controller.sign_in()
            assert ADMIN_AUTH_NOT_CONFIGURED == response

        # Shows the login page if there's an auth service
        # but no signed in admin.
        sign_in_fixture.admin.password = "password"
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in?redirect=foo"
        ):
            response = sign_in_fixture.manager.admin_sign_in_controller.sign_in()
            assert 200 == response.status_code
            response_data = response.get_data(as_text=True)
            assert "Email" in response_data
            assert "Password" in response_data

        # Redirects to the redirect parameter if an admin is signed in.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in?redirect=foo"
        ):
            flask.session["admin_email"] = sign_in_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = sign_in_fixture.manager.admin_sign_in_controller.sign_in()
            assert 302 == response.status_code
            assert "foo" == response.headers["Location"]

    def test_admin_signin_no_external_domain(self, sign_in_fixture: SignInFixture):
        # We don't permit redirecting to an external domain
        sign_in_fixture.admin.password = "password"
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in?redirect=http%3A%2F%2Fwww.example.com%2Fxyz"
        ):
            flask.session["admin_email"] = sign_in_fixture.admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = sign_in_fixture.manager.admin_sign_in_controller.sign_in()
            assert 400 == response.status_code
            assert (
                "Redirecting to an external domain is not allowed."
                == response.get_data(as_text=True)
            )

    def test_password_sign_in(self, sign_in_fixture: SignInFixture):
        # Returns an error if there's no admin auth service and no admins.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in_with_password"
        ):
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.password_sign_in()
            )
            assert ADMIN_AUTH_NOT_CONFIGURED == response

        admin, ignore = create(
            sign_in_fixture.ctrl.db.session, Admin, email="admin@nypl.org"
        )
        admin.password = "password"

        # Returns an error if there's no admin with the provided email.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in_with_password", method="POST"
        ):
            flask.request.form = MultiDict(
                [
                    ("email", "notanadmin@nypl.org"),
                    ("password", "password"),
                    ("redirect", "foo"),
                ]
            )
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.password_sign_in()
            )
            assert 401 == response.status_code

        # Returns an error if the password doesn't match.
        sign_in_fixture.admin.password = "password"
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in_with_password", method="POST"
        ):
            flask.request.form = MultiDict(
                [
                    ("email", sign_in_fixture.admin.email),
                    ("password", "notthepassword"),
                    ("redirect", "foo"),
                ]
            )
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.password_sign_in()
            )
            assert 401 == response.status_code

        # Redirects if the admin email/password combination is valid.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in_with_password", method="POST"
        ):
            flask.request.form = MultiDict(
                [
                    ("email", sign_in_fixture.admin.email),
                    ("password", "password"),
                    ("redirect", "foo"),
                ]
            )
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.password_sign_in()
            )
            assert 302 == response.status_code
            assert "foo" == response.headers["Location"]

        # Refuses to redirect to an unsafe location.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in_with_password", method="POST"
        ):
            flask.request.form = MultiDict(
                [
                    ("email", sign_in_fixture.admin.email),
                    ("password", "password"),
                    ("redirect", "http://www.example.com/passwordstealer"),
                ]
            )
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.password_sign_in()
            )
            assert 400 == response.status_code

    def test_change_password(self, sign_in_fixture: SignInFixture):
        admin, ignore = create(
            sign_in_fixture.ctrl.db.session,
            Admin,
            email=sign_in_fixture.ctrl.db.fresh_str(),
        )
        admin.password = "old"
        with sign_in_fixture.request_context_with_admin(
            "/admin/change_password", admin=admin
        ):
            flask.request.form = MultiDict(
                [
                    ("password", "new"),
                ]
            )
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.change_password()
            )
            assert 200 == response.status_code
            assert admin == Admin.authenticate(
                sign_in_fixture.ctrl.db.session, admin.email, "new"
            )
            assert None == Admin.authenticate(
                sign_in_fixture.ctrl.db.session, admin.email, "old"
            )

    def test_sign_out(self, sign_in_fixture: SignInFixture):
        admin, ignore = create(
            sign_in_fixture.ctrl.db.session,
            Admin,
            email=sign_in_fixture.ctrl.db.fresh_str(),
        )
        admin.password = "pass"
        with sign_in_fixture.ctrl.app.test_request_context("/admin/sign_out"):
            flask.session["admin_email"] = admin.email
            flask.session["auth_type"] = PasswordAdminAuthenticationProvider.NAME
            response = sign_in_fixture.manager.admin_sign_in_controller.sign_out()
            assert 302 == response.status_code

            # The admin's credentials have been removed from the session.
            assert None == flask.session.get("admin_email")
            assert None == flask.session.get("auth_type")


class TestResetPasswordController:
    def test_forgot_password_get(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller

        # If there is no admin with password then there is no auth providers and we should get error response
        admin_ctrl_fixture.admin.password_hashed = None
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/forgot_password"):
            assert [] == reset_password_ctrl.admin_auth_providers

            response = reset_password_ctrl.forgot_password()

            assert response.status_code == 500
            assert response.uri == ADMIN_AUTH_NOT_CONFIGURED.uri

        # If auth providers are set we should get forgot password page - success path
        admin_ctrl_fixture.admin.password = "password"
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/forgot_password"):
            response = reset_password_ctrl.forgot_password()

            assert response.status_code == 200
            assert "Send reset password email" in response.get_data(as_text=True)

        # If admin is already signed in it gets redirected since it can use regular reset password flow
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/forgot_password"):
            flask.request.form = MultiDict(
                [
                    ("email", admin_ctrl_fixture.admin.email),
                    ("password", "password"),
                    ("redirect", "foo"),
                ]
            )
            sign_in_response = (
                admin_ctrl_fixture.manager.admin_sign_in_controller.password_sign_in()
            )

            # Check that sign in is successful
            assert sign_in_response.status_code == 302
            assert "foo" == sign_in_response.headers["Location"]

            response = reset_password_ctrl.forgot_password()
            assert response.status_code == 302

            assert "admin/web" in response.headers.get("Location")

    def test_forgot_password_post(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller

        # If there is no admin sent in the request we should get error response
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/forgot_password", method="POST"
        ):
            flask.request.form = MultiDict([])

            response = reset_password_ctrl.forgot_password()
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code
            assert str(INVALID_ADMIN_CREDENTIALS.detail) in response.get_data(
                as_text=True
            )

        # If the admin does not exist we should also get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/forgot_password", method="POST"
        ):
            flask.request.form = MultiDict([("email", "fake@admin.com")])

            response = reset_password_ctrl.forgot_password()
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code
            assert str(INVALID_ADMIN_CREDENTIALS.detail) in response.get_data(
                as_text=True
            )

        # When the real admin is used the email is sent and we get success message in the response
        with mock.patch(
            "api.admin.password_admin_authentication_provider.EmailManager"
        ) as mock_email_manager:
            with admin_ctrl_fixture.ctrl.app.test_request_context(
                "/admin/forgot_password", method="POST"
            ):
                flask.request.form = MultiDict(
                    [("email", admin_ctrl_fixture.admin.email)]
                )

                response = reset_password_ctrl.forgot_password()
                assert response.status_code == 200
                assert "Email successfully sent" in response.get_data(as_text=True)

                # Check the email is sent
                assert mock_email_manager.send_email.call_count == 1

                call_args, call_kwargs = mock_email_manager.send_email.call_args_list[0]

                # Check that the email is sent to the right admin
                _, receivers = call_args

                assert len(receivers) == 1
                assert receivers[0] == admin_ctrl_fixture.admin.email

    def test_reset_password_get(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller
        token = "token"

        # If there is no admin with password then there is no auth providers and we should get error response
        admin_ctrl_fixture.admin.password_hashed = None
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            assert [] == reset_password_ctrl.admin_auth_providers

            response = reset_password_ctrl.reset_password(token)

            assert (
                response.status_code == ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.status_code
            )
            assert str(ADMIN_AUTH_MECHANISM_NOT_CONFIGURED.detail) in response.get_data(
                as_text=True
            )

        # If admin is already signed in it gets redirected since it can use regular reset password flow
        admin_ctrl_fixture.admin.password = "password"
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            flask.request.form = MultiDict(
                [
                    ("email", admin_ctrl_fixture.admin.email),
                    ("password", "password"),
                    ("redirect", "foo"),
                ]
            )
            sign_in_response = (
                admin_ctrl_fixture.manager.admin_sign_in_controller.password_sign_in()
            )

            # Check that sign in is successful
            assert sign_in_response.status_code == 302
            assert "foo" == sign_in_response.headers["Location"]

            response = reset_password_ctrl.reset_password(token)
            assert response.status_code == 302
            assert "admin/web" in response.headers.get("Location")

        # If we use bad token we get an error response with "Try again" button
        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            response = reset_password_ctrl.reset_password(token)

            assert response.status_code == 401
            assert "Try again" in response.get_data(as_text=True)

        # Finally, if we use good token we get back view with the form for the new password
        # Let's get valid token first
        with mock.patch(
            "api.admin.password_admin_authentication_provider.EmailManager"
        ) as mock_email_manager:
            with admin_ctrl_fixture.ctrl.app.test_request_context(
                "/admin/forgot_password", method="POST"
            ):
                flask.request.form = MultiDict(
                    [("email", admin_ctrl_fixture.admin.email)]
                )

                response = reset_password_ctrl.forgot_password()
                assert response.status_code == 200

                call_args, call_kwargs = mock_email_manager.send_email.call_args_list[0]
                mail_text = call_kwargs["text"]

                token = self._extract_reset_pass_token_from_mail_text(mail_text)

        with admin_ctrl_fixture.ctrl.app.test_request_context("/admin/reset_password"):
            response = reset_password_ctrl.reset_password(token)

            assert response.status_code == 200

            response_body = response.get_data(as_text=True)
            assert "New Password" in response_body
            assert "Confirm New Password" in response_body

    def _extract_reset_pass_token_from_mail_text(self, mail_text):
        # Reset password url is in form of http[s]://url/admin/forgot_password/token
        reset_pass_url = re.search("(?P<url>https?://[^\\s]+)", mail_text).group("url")
        token = reset_pass_url.split("/")[-1]

        return token

    def test_reset_password_post(self, admin_ctrl_fixture: AdminControllerFixture):
        reset_password_ctrl = admin_ctrl_fixture.manager.admin_reset_password_controller

        # Let's get valid token first
        with mock.patch(
            "api.admin.password_admin_authentication_provider.EmailManager"
        ) as mock_email_manager:
            with admin_ctrl_fixture.ctrl.app.test_request_context(
                "/admin/forgot_password", method="POST"
            ):
                flask.request.form = MultiDict(
                    [("email", admin_ctrl_fixture.admin.email)]
                )

                response = reset_password_ctrl.forgot_password()
                assert response.status_code == 200

                call_args, call_kwargs = mock_email_manager.send_email.call_args_list[0]
                mail_text = call_kwargs["text"]

                token = self._extract_reset_pass_token_from_mail_text(mail_text)

        # If there is no passwords we get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = MultiDict([])

            response = reset_password_ctrl.reset_password(token)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

        # If there is only one password we get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = MultiDict([("password", "only_one")])

            response = reset_password_ctrl.reset_password(token)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

        # If there are both passwords but they do not match we also get an error
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = MultiDict(
                [("password", "something"), ("confirm_password", "something_different")]
            )

            response = reset_password_ctrl.reset_password(token)
            assert response.status_code == INVALID_ADMIN_CREDENTIALS.status_code

        # Finally, let's change that password!
        # Check current password
        assert admin_ctrl_fixture.admin.has_password("password")

        new_password = "new_password"
        with admin_ctrl_fixture.ctrl.app.test_request_context(
            "/admin/reset_password", method="POST"
        ):
            flask.request.form = MultiDict(
                [("password", new_password), ("confirm_password", new_password)]
            )

            response = reset_password_ctrl.reset_password(token)
            assert response.status_code == 200

            assert admin_ctrl_fixture.admin.has_password(new_password)


class PatronControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(AdminRole.LIBRARIAN, self.ctrl.db.default_library())


@pytest.fixture(scope="function")
def patron_controller_fixture(
    controller_fixture: ControllerFixture,
) -> PatronControllerFixture:
    return PatronControllerFixture(controller_fixture)


class TestPatronController:
    def test__load_patrondata(self, patron_controller_fixture: PatronControllerFixture):
        """Test the _load_patrondata helper method."""

        class MockAuthenticator:
            def __init__(self, providers):
                self.providers = providers

        class MockAuthenticationProvider:
            def __init__(self, patron_dict):
                self.patron_dict = patron_dict

            def remote_patron_lookup(self, patrondata):
                return self.patron_dict.get(patrondata.authorization_identifier)

        authenticator = MockAuthenticator([])
        auth_provider = MockAuthenticationProvider({})
        identifier = "Patron"

        form = MultiDict([("identifier", identifier)])
        m = patron_controller_fixture.manager.admin_patron_controller._load_patrondata

        # User doesn't have admin permission
        with patron_controller_fixture.ctrl.request_context_with_library("/"):
            pytest.raises(AdminNotAuthorized, m, authenticator)

        # No form data specified
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            response = m(authenticator)
            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert "Please enter a patron identifier" == response.detail

        # AuthenticationProvider has no Authenticators.
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert (
                "This library has no authentication providers, so it has no patrons."
                == response.detail
            )

        # Authenticator can't find patron with this identifier
        authenticator.providers.append(auth_provider)
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = m(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert (
                "No patron with identifier %s was found at your library" % identifier
                == response.detail
            )

    def test_lookup_patron(self, patron_controller_fixture: PatronControllerFixture):

        # Here's a patron.
        patron = patron_controller_fixture.ctrl.db.patron()
        patron.authorization_identifier = patron_controller_fixture.ctrl.db.fresh_str()

        # This PatronController will always return information about that
        # patron, no matter what it's asked for.
        class MockPatronController(PatronController):
            def _load_patrondata(self, authenticator):
                self.called_with = authenticator
                return PatronData(
                    authorization_identifier="An Identifier",
                    personal_name="A Patron",
                )

        controller = MockPatronController(patron_controller_fixture.manager)

        authenticator = object()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = MultiDict([("identifier", object())])
            response = controller.lookup_patron(authenticator)
            # The authenticator was passed into _load_patrondata()
            assert authenticator == controller.called_with

            # _load_patrondata() returned a PatronData object. We
            # converted it to a dictionary, which will be dumped to
            # JSON on the way out.
            assert "An Identifier" == response["authorization_identifier"]
            assert "A Patron" == response["personal_name"]

    def test_reset_adobe_id(self, patron_controller_fixture: PatronControllerFixture):
        # Here's a patron with two Adobe-relevant credentials.
        patron = patron_controller_fixture.ctrl.db.patron()
        patron.authorization_identifier = patron_controller_fixture.ctrl.db.fresh_str()

        patron_controller_fixture.ctrl.db.credential(
            patron=patron, type=AdobeVendorIDModel.VENDOR_ID_UUID_TOKEN_TYPE
        )
        patron_controller_fixture.ctrl.db.credential(
            patron=patron, type=AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER
        )

        # This PatronController will always return a specific
        # PatronData object, no matter what is asked for.
        class MockPatronController(PatronController):
            mock_patrondata = None

            def _load_patrondata(self, authenticator):
                self.called_with = authenticator
                return self.mock_patrondata

        controller = MockPatronController(patron_controller_fixture.manager)
        controller.mock_patrondata = PatronData(
            authorization_identifier=patron.authorization_identifier
        )

        # We reset their Adobe ID.
        authenticator = object()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            form = MultiDict([("identifier", patron.authorization_identifier)])
            flask.request.form = form

            response = controller.reset_adobe_id(authenticator)
            assert 200 == response.status_code

            # _load_patrondata was called and gave us information about
            # which Patron to modify.
            controller.called_with = authenticator

        # Both of the Patron's credentials are gone.
        assert patron.credentials == []

        # Here, the AuthenticationProvider finds a PatronData, but the
        # controller can't turn it into a Patron because it's too vague.
        controller.mock_patrondata = PatronData()
        with patron_controller_fixture.request_context_with_library_and_admin("/"):
            flask.request.form = form
            response = controller.reset_adobe_id(authenticator)

            assert 404 == response.status_code
            assert NO_SUCH_PATRON.uri == response.uri
            assert "Could not create local patron object" in response.detail


class TimestampsFixture:
    def __init__(self, admin_ctrl_fixture: AdminControllerFixture):
        self.admin_ctrl_fixture = admin_ctrl_fixture

        db = self.admin_ctrl_fixture.ctrl.db.session

        for timestamp in db.query(Timestamp):
            db.delete(timestamp)

        self.collection = self.admin_ctrl_fixture.ctrl.db.default_collection()
        self.start = utc_now()
        self.finish = utc_now()

        cp, ignore = create(
            db,
            Timestamp,
            service_type="coverage_provider",
            service="test_cp",
            start=self.start,
            finish=self.finish,
            collection=self.collection,
        )

        monitor, ignore = create(
            db,
            Timestamp,
            service_type="monitor",
            service="test_monitor",
            start=self.start,
            finish=self.finish,
            collection=self.collection,
            exception="stack trace string",
        )

        script, ignore = create(
            db,
            Timestamp,
            achievements="ran a script",
            service_type="script",
            service="test_script",
            start=self.start,
            finish=self.finish,
        )

        other, ignore = create(
            db,
            Timestamp,
            service="test_other",
            start=self.start,
            finish=self.finish,
        )


@pytest.fixture(scope="function")
def timestamps_fixture(admin_ctrl_fixture: AdminControllerFixture) -> TimestampsFixture:
    return TimestampsFixture(admin_ctrl_fixture)


class TestTimestampsController:
    def test_diagnostics_admin_not_authorized(
        self, timestamps_fixture: TimestampsFixture
    ):
        with timestamps_fixture.admin_ctrl_fixture.request_context_with_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                timestamps_fixture.admin_ctrl_fixture.manager.timestamps_controller.diagnostics,
            )

    def test_diagnostics(self, timestamps_fixture: TimestampsFixture):
        duration = (
            timestamps_fixture.finish - timestamps_fixture.start
        ).total_seconds()

        with timestamps_fixture.admin_ctrl_fixture.request_context_with_admin("/"):
            timestamps_fixture.admin_ctrl_fixture.admin.add_role(AdminRole.SYSTEM_ADMIN)
            response = (
                timestamps_fixture.admin_ctrl_fixture.manager.timestamps_controller.diagnostics()
            )

        assert set(response.keys()) == {
            "coverage_provider",
            "monitor",
            "script",
            "other",
        }

        cp_service = response["coverage_provider"]
        cp_name, cp_collection = list(cp_service.items())[0]
        assert cp_name == "test_cp"
        cp_collection_name, [cp_timestamp] = list(cp_collection.items())[0]
        assert cp_collection_name == timestamps_fixture.collection.name
        assert cp_timestamp.get("exception") == None
        assert cp_timestamp.get("start") == timestamps_fixture.start
        assert cp_timestamp.get("duration") == duration
        assert cp_timestamp.get("achievements") == None

        monitor_service = response["monitor"]
        monitor_name, monitor_collection = list(monitor_service.items())[0]
        assert monitor_name == "test_monitor"
        monitor_collection_name, [monitor_timestamp] = list(monitor_collection.items())[
            0
        ]
        assert monitor_collection_name == timestamps_fixture.collection.name
        assert monitor_timestamp.get("exception") == "stack trace string"
        assert monitor_timestamp.get("start") == timestamps_fixture.start
        assert monitor_timestamp.get("duration") == duration
        assert monitor_timestamp.get("achievements") == None

        script_service = response["script"]
        script_name, script_collection = list(script_service.items())[0]
        assert script_name == "test_script"
        script_collection_name, [script_timestamp] = list(script_collection.items())[0]
        assert script_collection_name == "No associated collection"
        assert script_timestamp.get("exception") == None
        assert script_timestamp.get("duration") == duration
        assert script_timestamp.get("start") == timestamps_fixture.start
        assert script_timestamp.get("achievements") == "ran a script"

        other_service = response["other"]
        other_name, other_collection = list(other_service.items())[0]
        assert other_name == "test_other"
        other_collection_name, [other_timestamp] = list(other_collection.items())[0]
        assert other_collection_name == "No associated collection"
        assert other_timestamp.get("exception") == None
        assert other_timestamp.get("duration") == duration
        assert other_timestamp.get("start") == timestamps_fixture.start
        assert other_timestamp.get("achievements") == None


class AdminLibrarianFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(
            AdminRole.LIBRARIAN, controller_fixture.db.default_library()
        )


@pytest.fixture(scope="function")
def admin_librarian_fixture(
    controller_fixture: ControllerFixture,
) -> AdminLibrarianFixture:
    return AdminLibrarianFixture(controller_fixture)


class TestFeedController:
    def test_suppressed(self, admin_librarian_fixture):
        suppressed_work = admin_librarian_fixture.ctrl.db.work(
            with_open_access_download=True
        )
        suppressed_work.license_pools[0].suppressed = True

        unsuppressed_work = admin_librarian_fixture.ctrl.db.work()

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_feed_controller.suppressed()
            )
            feed = feedparser.parse(response.get_data(as_text=True))
            entries = feed["entries"]
            assert 1 == len(entries)
            assert suppressed_work.title == entries[0]["title"]

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_feed_controller.suppressed,
            )

    def test_genres(self, admin_librarian_fixture):
        with admin_librarian_fixture.ctrl.app.test_request_context("/"):
            response = admin_librarian_fixture.manager.admin_feed_controller.genres()

            for name in genres:
                top = "Fiction" if genres[name].is_fiction else "Nonfiction"
                assert response[top][name] == dict(
                    {
                        "name": name,
                        "parents": [parent.name for parent in genres[name].parents],
                        "subgenres": [
                            subgenre.name for subgenre in genres[name].subgenres
                        ],
                    }
                )


class TestCustomListsController:
    def test_custom_lists_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        # This list has no associated Library and should not be included.
        no_library, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
        )

        auto_update_query = json.dumps(dict(query=dict(key="key", value="value")))
        one_entry, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            auto_update_enabled=True,
            auto_update_query=auto_update_query,
        )
        edition = admin_librarian_fixture.ctrl.db.edition()
        one_entry.add_entry(edition)
        collection = admin_librarian_fixture.ctrl.db.collection()
        collection.customlists = [one_entry]

        no_entries, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            auto_update_enabled=False,
        )

        # This will set the is_shared attribute
        shared_library = admin_librarian_fixture.ctrl.db.library()
        assert (
            CustomListQueries.share_locally_with_library(
                admin_librarian_fixture.ctrl.db.session, no_entries, shared_library
            )
            == True
        )

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert 2 == len(response.get("custom_lists"))
            lists = response.get("custom_lists")
            [l1, l2] = sorted(lists, key=lambda l: l.get("id"))

            assert one_entry.id == l1.get("id")
            assert one_entry.name == l1.get("name")
            assert 1 == l1.get("entry_count")
            assert 1 == len(l1.get("collections"))
            [c] = l1.get("collections")
            assert collection.name == c.get("name")
            assert collection.id == c.get("id")
            assert collection.protocol == c.get("protocol")
            assert True == l1.get("auto_update")
            assert auto_update_query == l1.get("auto_update_query")
            assert CustomList.INIT == l1.get("auto_update_status")
            assert False == l1.get("is_shared")
            assert True == l1.get("is_owner")

            assert no_entries.id == l2.get("id")
            assert no_entries.name == l2.get("name")
            assert 0 == l2.get("entry_count")
            assert 0 == len(l2.get("collections"))
            assert False == l2.get("auto_update")
            assert None == l2.get("auto_update_query")
            assert CustomList.INIT == l2.get("auto_update_status")
            assert True == l2.get("is_shared")
            assert True == l2.get("is_owner")

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists,
            )

    def test_custom_lists_post_errors(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", "4"),
                    ("name", "name"),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert MISSING_CUSTOM_LIST == response

        library = admin_librarian_fixture.ctrl.db.library()
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
        )
        list.library = library
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", list.id),
                    ("name", list.name),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert CANNOT_CHANGE_LIBRARY_FOR_CUSTOM_LIST == response

        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=admin_librarian_fixture.ctrl.db.default_library(),
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("name", list.name),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert CUSTOM_LIST_NAME_ALREADY_IN_USE == response

        l1, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=admin_librarian_fixture.ctrl.db.default_library(),
        )
        l2, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=admin_librarian_fixture.ctrl.db.default_library(),
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", l2.id),
                    ("name", l1.name),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert CUSTOM_LIST_NAME_ALREADY_IN_USE == response

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([12345])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert MISSING_COLLECTION == response

        admin, ignore = create(
            admin_librarian_fixture.ctrl.db.session, Admin, email="test@nypl.org"
        )
        library = admin_librarian_fixture.ctrl.db.library()
        with admin_librarian_fixture.request_context_with_admin(
            "/", method="POST", admin=admin
        ):
            flask.request.library = library
            form = MultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists,
            )

    def test_custom_lists_post_collection_with_wrong_library(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        # This collection is not associated with any libraries.
        collection = admin_librarian_fixture.ctrl.db.collection()
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("name", "name"),
                    ("collections", json.dumps([collection.id])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert COLLECTION_NOT_ASSOCIATED_WITH_LIBRARY == response

    def test_custom_lists_create(self, admin_librarian_fixture: AdminLibrarianFixture):
        work = admin_librarian_fixture.ctrl.db.work(with_open_access_download=True)
        collection = admin_librarian_fixture.ctrl.db.collection()
        collection.libraries = [admin_librarian_fixture.ctrl.db.default_library()]

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("name", "List"),
                    (
                        "entries",
                        json.dumps(
                            [dict(id=work.presentation_edition.primary_identifier.urn)]
                        ),
                    ),
                    ("collections", json.dumps([collection.id])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert 201 == response.status_code

            [list] = admin_librarian_fixture.ctrl.db.session.query(CustomList).all()
            assert list.id == int(response.get_data(as_text=True))
            assert admin_librarian_fixture.ctrl.db.default_library() == list.library
            assert "List" == list.name
            assert 1 == len(list.entries)
            assert work == list.entries[0].work
            assert work.presentation_edition == list.entries[0].edition
            assert True == list.entries[0].featured
            assert [collection] == list.collections
            assert False == list.auto_update_enabled

        # On an error of auto_update, rollbacks should occur
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("name", "400List"),
                    (
                        "entries",
                        "[]",
                    ),
                    ("collections", "[]"),
                    ("auto_update", True),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert 400 == response.status_code
            # List was not created
            assert None == get_one(
                admin_librarian_fixture.ctrl.db.session, CustomList, name="400List"
            )

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("name", "400List"),
                    (
                        "entries",
                        json.dumps(
                            [dict(id=work.presentation_edition.primary_identifier.urn)]
                        ),
                    ),
                    ("collections", json.dumps([collection.id])),
                    ("auto_update", True),
                    (
                        "auto_update_query",
                        json.dumps({"query": {"key": "title", "value": "A Title"}}),
                    ),
                    ("auto_update_facets", json.dumps({})),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert response == AUTO_UPDATE_CUSTOM_LIST_CANNOT_HAVE_ENTRIES
            assert 400 == response.status_code

        # Valid auto update query request
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ), mock.patch("api.admin.controller.CustomListQueries") as mock_query:
            form = MultiDict(
                [
                    ("name", "200List"),
                    ("collections", json.dumps([collection.id])),
                    ("auto_update", True),
                    (
                        "auto_update_query",
                        json.dumps({"query": {"key": "title", "value": "A Title"}}),
                    ),
                    ("auto_update_facets", json.dumps({})),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert 201 == response.status_code
            [list] = (
                admin_librarian_fixture.ctrl.db.session.query(CustomList)
                .filter(CustomList.name == "200List")
                .all()
            )
            assert True == list.auto_update_enabled
            assert (
                json.dumps({"query": {"key": "title", "value": "A Title"}})
                == list.auto_update_query
            )
            assert json.dumps({}) == list.auto_update_facets
            assert mock_query.populate_query_pages.call_count == 1

    def test_custom_list_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            data_source=data_source,
        )

        work1 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        work2 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        list.add_entry(work1)
        list.add_entry(work2)

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            feed = feedparser.parse(response.get_data())

            assert list.name == feed.feed.title
            assert 2 == len(feed.entries)

            [self_custom_list_link] = [
                x["href"] for x in feed.feed["links"] if x["rel"] == "self"
            ]
            assert self_custom_list_link == feed.feed.id

            [entry1, entry2] = feed.entries
            assert work1.title == entry1.get("title")
            assert work2.title == entry2.get("title")

            assert work1.presentation_edition.author == entry1.get("author")
            assert work2.presentation_edition.author == entry2.get("author")

    def test_custom_list_get_with_pagination(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            data_source=data_source,
        )

        pagination_size = Pagination.DEFAULT_SIZE

        for i in range(pagination_size + 1):
            work = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
            list.add_entry(work)

        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            feed = feedparser.parse(response.get_data())

            assert list.name == feed.feed.title

            [next_custom_list_link] = [
                x["href"] for x in feed.feed["links"] if x["rel"] == "next"
            ]

            # We remove the list_name argument of the url so we can add the after keyword and build the pagination link
            custom_list_url = feed.feed.id.rsplit("?", maxsplit=1)[0]
            next_page_url = f"{custom_list_url}?after={pagination_size}"

            assert next_custom_list_link == next_page_url

    def test_custom_list_get_errors(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                123
            )
            assert MISSING_CUSTOM_LIST == response

        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            library=admin_librarian_fixture.ctrl.db.default_library(),
            data_source=data_source,
        )

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list,
                list.id,
            )

    def test_custom_list_edit(self, admin_librarian_fixture: AdminLibrarianFixture):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
        )
        list.library = admin_librarian_fixture.ctrl.db.default_library()

        # Create a Lane that depends on this CustomList for its membership.
        lane = admin_librarian_fixture.ctrl.db.lane()
        lane.customlists.append(list)
        lane.size = 350

        admin_librarian_fixture.ctrl.controller.search_engine.docs = {}

        w1 = admin_librarian_fixture.ctrl.db.work(
            title="Alpha", with_license_pool=True, language="eng"
        )
        w2 = admin_librarian_fixture.ctrl.db.work(
            title="Bravo", with_license_pool=True, language="fre"
        )
        w3 = admin_librarian_fixture.ctrl.db.work(
            title="Charlie", with_license_pool=True
        )
        w2.presentation_edition.medium = Edition.AUDIO_MEDIUM
        w3.presentation_edition.permanent_work_id = (
            w2.presentation_edition.permanent_work_id
        )
        w3.presentation_edition.medium = Edition.BOOK_MEDIUM

        list.add_entry(w1)
        list.add_entry(w2)
        admin_librarian_fixture.ctrl.controller.search_engine.bulk_update([w1, w2, w3])

        # All three works should be indexed, but only w1 and w2 should be related to the list
        assert len(admin_librarian_fixture.ctrl.controller.search_engine.docs) == 3
        currently_indexed_on_list = [
            v["title"]
            for (
                k,
                v,
            ) in admin_librarian_fixture.ctrl.controller.search_engine.docs.items()
            if v["customlists"] is not None
        ]
        assert sorted(currently_indexed_on_list) == ["Alpha", "Bravo"]

        new_entries = [
            dict(
                id=work.presentation_edition.primary_identifier.urn,
                medium=Edition.medium_to_additional_type[
                    work.presentation_edition.medium
                ],
            )
            for work in [w2, w3]
        ]
        deletedEntries = [
            dict(
                id=work.presentation_edition.primary_identifier.urn,
                medium=Edition.medium_to_additional_type[
                    work.presentation_edition.medium
                ],
            )
            for work in [w1]
        ]

        c1 = admin_librarian_fixture.ctrl.db.collection()
        c1.libraries = [admin_librarian_fixture.ctrl.db.default_library()]
        c2 = admin_librarian_fixture.ctrl.db.collection()
        c2.libraries = [admin_librarian_fixture.ctrl.db.default_library()]
        list.collections = [c1]
        new_collections = [c2]

        # The lane size is set to a static value above. After this call it should
        # be reset to a value that reflects the number of documents in the search_engine,
        # regardless of filter, since that's what the mock search engine's count_works does.
        assert lane.size == 350

        # Test fails without expiring the ORM cache
        admin_librarian_fixture.ctrl.db.session.expire_all()

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "new name"),
                    ("entries", json.dumps(new_entries)),
                    ("deletedEntries", json.dumps(deletedEntries)),
                    ("collections", json.dumps([c.id for c in new_collections])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )

        # The works associated with the list in ES should have changed, though the total
        # number of documents in the index should be the same.
        assert len(admin_librarian_fixture.ctrl.controller.search_engine.docs) == 3
        currently_indexed_on_list = [
            v["title"]
            for (
                k,
                v,
            ) in admin_librarian_fixture.ctrl.controller.search_engine.docs.items()
            if v["customlists"] is not None
        ]
        assert sorted(currently_indexed_on_list) == ["Bravo", "Charlie"]

        assert 200 == response.status_code
        assert list.id == int(response.get_data(as_text=True))

        assert "new name" == list.name
        assert {w2, w3} == {entry.work for entry in list.entries}
        assert new_collections == list.collections

        # If we were using a real search engine instance, the lane's size would be set
        # to 2, since that's the number of works that would be associated with the
        # custom list that the lane is based on. In this case we're using an instance of
        # MockExternalSearchIndex, whose count_works() method (called in Lane.update_size())
        # returns the number of items in search_engine.docs. Testing that lane.size is now
        # set to 3 shows that .update_size() was called during the call to custom_list().
        assert lane.size == 3

        # Edit for auto update values
        update_query = {"query": {"key": "title", "value": "title"}}
        update_facets = {"order": "title"}
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "new name"),
                    ("collections", json.dumps([c.id for c in new_collections])),
                    ("auto_update", "true"),
                    ("auto_update_query", json.dumps(update_query)),
                    ("auto_update_facets", json.dumps(update_facets)),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )

        assert True == list.auto_update_enabled
        assert json.dumps(update_query) == list.auto_update_query
        assert json.dumps(update_facets) == list.auto_update_facets

        admin_librarian_fixture.admin.remove_role(
            AdminRole.LIBRARIAN, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "another new name"),
                    ("entries", json.dumps(new_entries)),
                    ("collections", json.dumps([c.id for c in new_collections])),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list,
                list.id,
            )

    def test_custom_list_auto_update_cases(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        list, _ = admin_librarian_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF,
        )
        list.library = admin_librarian_fixture.ctrl.db.default_library()

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ):
            form = MultiDict(
                [
                    ("id", str(list.id)),
                    ("name", "new name"),
                    ("entries", "[]"),
                    ("deletedEntries", "[]"),
                    ("collections", "[]"),
                    ("auto_update", "true"),
                    ("auto_update_query", None),
                ]
            )
            add_request_context(
                flask.request, CustomListsController.CustomListPostRequest, form=form
            )

            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert type(response) == ProblemDetail
            assert response.status_code == 400
            assert (
                response.detail
                == "auto_update_query must be present when auto_update is enabled"
            )

    def test_custom_list_delete_success(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        admin_librarian_fixture.admin.add_role(
            AdminRole.LIBRARY_MANAGER, admin_librarian_fixture.ctrl.db.default_library()
        )

        # Create a CustomList with two Works on it.
        library_staff = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=library_staff,
        )
        list.library = admin_librarian_fixture.ctrl.db.default_library()

        w1 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        w2 = admin_librarian_fixture.ctrl.db.work(with_license_pool=True)
        list.add_entry(w1)
        list.add_entry(w2)

        # Whenever the mocked search engine is asked how many
        # works are in a Lane, it will say there are two.
        admin_librarian_fixture.ctrl.controller.search_engine.docs = dict(
            id1="doc1", id2="doc2"
        )

        # Create a second CustomList, from another data source,
        # containing a single work.
        nyt = DataSource.lookup(admin_librarian_fixture.ctrl.db.session, DataSource.NYT)
        list2, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=nyt,
        )
        list2.library = admin_librarian_fixture.ctrl.db.default_library()
        list2.add_entry(w2)

        # Create a Lane which takes all of its contents from that
        # CustomList. When the CustomList is deleted, the Lane will
        # have no reason to exist, and it will be automatically
        # deleted as well.
        lane = admin_librarian_fixture.ctrl.db.lane(
            display_name="to be automatically removed"
        )
        lane.customlists.append(list)

        # This Lane is based on two different CustomLists. Its size
        # will be updated when the CustomList is deleted, but the Lane
        # itself will not be deleted, since it's still based on
        # something.
        lane2 = admin_librarian_fixture.ctrl.db.lane(
            display_name="to have size updated"
        )
        lane2.customlists.append(list)
        lane2.customlists.append(list2)
        lane2.size = 100

        # This lane is based on _all_ lists from a given data source.
        # It will also not be deleted when the CustomList is deleted,
        # because other lists from that data source might show up in
        # the future.
        lane3 = admin_librarian_fixture.ctrl.db.lane(
            display_name="All library staff lists"
        )
        lane3.list_datasource = list.data_source
        lane3.size = 150

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert 200 == response.status_code

        # The first CustomList and all of its entries have been removed.
        # Only the second one remains.
        assert [list2] == admin_librarian_fixture.ctrl.db.session.query(
            CustomList
        ).all()
        assert (
            list2.entries
            == admin_librarian_fixture.ctrl.db.session.query(CustomListEntry).all()
        )

        # The first lane was automatically removed when it became
        # based on an empty set of CustomLists.
        assert None == get_one(
            admin_librarian_fixture.ctrl.db.session, Lane, id=lane.id
        )

        # The second and third lanes were not removed, because they
        # weren't based solely on this specific list. But their .size
        # attributes were updated to reflect the removal of the list from
        # the lane.
        #
        # In the context of this test, this means that
        # MockExternalSearchIndex.count_works() was called, and we set
        # it up to always return 2.
        assert 2 == lane2.size
        assert 2 == lane3.size

    def test_custom_list_delete_errors(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            pytest.raises(
                AdminNotAuthorized,
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list,
                list.id,
            )

        admin_librarian_fixture.admin.add_role(
            AdminRole.LIBRARY_MANAGER, admin_librarian_fixture.ctrl.db.default_library()
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                123
            )
            assert MISSING_CUSTOM_LIST == response

        library = admin_librarian_fixture.ctrl.db.library()
        admin_librarian_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        CustomListQueries.share_locally_with_library(
            admin_librarian_fixture.ctrl.db.session, list, library
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE"
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.custom_list(
                list.id
            )
            assert response == CANNOT_DELETE_SHARED_LIST

    @define
    class ShareLocallySetup:
        shared_with: Optional[Library] = None
        primary_library: Optional[Library] = None
        collection1: Optional[Collection] = None
        list: Optional[CustomList] = None

    def _setup_share_locally(self, admin_librarian_fixture: AdminLibrarianFixture):
        shared_with = admin_librarian_fixture.ctrl.db.library("shared_with")
        primary_library = admin_librarian_fixture.ctrl.db.library("primary")
        collection1 = admin_librarian_fixture.ctrl.db.collection("c1")
        primary_library.collections.append(collection1)

        data_source = DataSource.lookup(
            admin_librarian_fixture.ctrl.db.session, DataSource.LIBRARY_STAFF
        )
        list, ignore = create(
            admin_librarian_fixture.ctrl.db.session,
            CustomList,
            name=admin_librarian_fixture.ctrl.db.fresh_str(),
            data_source=data_source,
            library=primary_library,
            collections=[collection1],
        )

        return self.ShareLocallySetup(
            shared_with=shared_with,
            primary_library=primary_library,
            collection1=collection1,
            list=list,
        )

    def _share_locally(
        self, customlist, library, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", library=library, method="POST"
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                customlist.id
            )
        return response

    def _share_locally_with_collection(
        self, customlist, collection, admin_librarian_fixture: AdminLibrarianFixture
    ):
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="POST"
        ) as c:
            flask.request.form = MultiDict(
                [
                    ("collection", collection.id),
                ]
            )
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally_with_library_collection(
                customlist.id
            )
        return response

    def test_share_locally_missing_collection(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["failures"] == 2
        assert response["successes"] == 0

    def test_share_locally_success(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["successes"] == 1
        assert response["failures"] == 1  # The default library

        admin_librarian_fixture.ctrl.db.session.refresh(s.list)
        assert len(s.list.shared_locally_with_libraries) == 1

        # Try again should have 0 more libraries as successes
        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["successes"] == 0
        assert response["failures"] == 1  # The default library

    def test_share_locally_with_invalid_entries(
        self, admin_librarian_fixture: AdminLibrarianFixture
    ):
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)

        # Second collection with work in list
        collection2 = admin_librarian_fixture.ctrl.db.collection()
        s.primary_library.collections.append(collection2)
        w = admin_librarian_fixture.ctrl.db.work(collection=collection2)
        s.list.add_entry(w)

        response = self._share_locally(
            s.list, s.primary_library, admin_librarian_fixture
        )
        assert response["failures"] == 2
        assert response["successes"] == 0

    def test_share_locally_get(self, admin_librarian_fixture: AdminLibrarianFixture):
        """Does the GET method fetch shared lists"""
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)

        resp = self._share_locally(s.list, s.primary_library, admin_librarian_fixture)
        assert resp["successes"] == 1

        admin_librarian_fixture.admin.add_role(AdminRole.LIBRARIAN, s.shared_with)
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="GET", library=s.shared_with
        ):
            response = (
                admin_librarian_fixture.manager.admin_custom_lists_controller.custom_lists()
            )
            assert len(response["custom_lists"]) == 1
            collections = [
                dict(id=c.id, name=c.name, protocol=c.protocol)
                for c in s.list.collections
            ]
            assert response["custom_lists"][0] == dict(
                id=s.list.id,
                name=s.list.name,
                collections=collections,
                entry_count=s.list.size,
                auto_update=False,
                auto_update_query=None,
                auto_update_facets=None,
                auto_update_status=CustomList.INIT,
                is_owner=False,
                is_shared=True,
            )

    def test_share_locally_delete(self, admin_librarian_fixture: AdminLibrarianFixture):
        """Test the deleting of a lists shared status"""
        s = self._setup_share_locally(admin_librarian_fixture)
        s.shared_with.collections.append(s.collection1)

        resp = self._share_locally(s.list, s.primary_library, admin_librarian_fixture)
        assert resp["successes"] == 1

        # First, we are shared with a library which uses the list
        # so we cannot delete the share status
        lane_with_shared, _ = create(
            admin_librarian_fixture.ctrl.db.session,
            Lane,
            library_id=s.shared_with.id,
            customlists=[s.list],
        )

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert type(response) == ProblemDetail

        # Second, we remove the lane that uses the shared list_
        # making it available to unshare
        admin_librarian_fixture.ctrl.db.session.delete(lane_with_shared)
        admin_librarian_fixture.ctrl.db.session.commit()

        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert response.status_code == 204

        assert s.list.shared_locally_with_libraries == []

        # Third, it is in use by the owner library (not the shared library)
        # so the list can still be unshared
        resp = self._share_locally(s.list, s.primary_library, admin_librarian_fixture)
        assert resp["successes"] == 1

        lane_with_primary, _ = create(
            admin_librarian_fixture.ctrl.db.session,
            Lane,
            library_id=s.primary_library.id,
            customlists=[s.list],
        )
        with admin_librarian_fixture.request_context_with_library_and_admin(
            "/", method="DELETE", library=s.primary_library
        ):
            response = admin_librarian_fixture.manager.admin_custom_lists_controller.share_locally(
                s.list.id
            )
            assert response.status_code == 204

        assert s.list.shared_locally_with_libraries == []

    def test_auto_update_edit(self, admin_librarian_fixture: AdminLibrarianFixture):
        w1 = admin_librarian_fixture.ctrl.db.work()
        custom_list: CustomList
        custom_list, _ = admin_librarian_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        custom_list.add_entry(w1)
        custom_list.auto_update_enabled = True
        custom_list.auto_update_query = '{"query":"...."}'
        custom_list.auto_update_status = CustomList.UPDATED
        admin_librarian_fixture.ctrl.db.session.commit()

        response = admin_librarian_fixture.manager.admin_custom_lists_controller._create_or_update_list(
            custom_list.library,
            custom_list.name,
            [],
            [],
            [],
            id=custom_list.id,
            auto_update=True,
            auto_update_query={"query": "...changed"},
        )

        assert response.status_code == 200
        assert custom_list.auto_update_query == '{"query": "...changed"}'
        assert custom_list.auto_update_status == CustomList.REPOPULATE
        assert [e.work_id for e in custom_list.entries] == [w1.id]


class AdminLibraryManagerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)
        self.admin.add_role(
            AdminRole.LIBRARY_MANAGER, controller_fixture.db.default_library()
        )


@pytest.fixture(scope="function")
def alm_fixture(controller_fixture: ControllerFixture) -> AdminLibraryManagerFixture:
    return AdminLibraryManagerFixture(controller_fixture)


class TestLanesController:
    def test_lanes_get(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        collection = alm_fixture.ctrl.db.collection()
        library.collections += [collection]

        english = alm_fixture.ctrl.db.lane(
            "English", library=library, languages=["eng"]
        )
        english.priority = 0
        english.size = 44
        english_fiction = alm_fixture.ctrl.db.lane(
            "Fiction", library=library, parent=english, fiction=True
        )
        english_fiction.visible = False
        english_fiction.size = 33
        english_sf = alm_fixture.ctrl.db.lane(
            "Science Fiction", library=library, parent=english_fiction
        )
        english_sf.add_genre("Science Fiction")
        english_sf.inherit_parent_restrictions = True
        english_sf.size = 22
        spanish = alm_fixture.ctrl.db.lane(
            "Spanish", library=library, languages=["spa"]
        )
        spanish.priority = 1
        spanish.size = 11

        w1 = alm_fixture.ctrl.db.work(
            with_license_pool=True,
            language="eng",
            genre="Science Fiction",
            collection=collection,
        )
        w2 = alm_fixture.ctrl.db.work(
            with_license_pool=True, language="eng", fiction=False, collection=collection
        )

        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = library
        lane_for_list = alm_fixture.ctrl.db.lane("List Lane", library=library)
        lane_for_list.customlists += [list]
        lane_for_list.priority = 2
        lane_for_list.size = 1

        with alm_fixture.request_context_with_library_and_admin("/"):
            flask.request.library = library
            # The admin is not a librarian for this library.
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.lanes,
            )
            alm_fixture.admin.add_role(AdminRole.LIBRARIAN, library)
            response = alm_fixture.manager.admin_lanes_controller.lanes()

            assert 3 == len(response.get("lanes"))
            [english_info, spanish_info, list_info] = response.get("lanes")

            assert english.id == english_info.get("id")
            assert english.display_name == english_info.get("display_name")
            assert english.visible == english_info.get("visible")
            assert 44 == english_info.get("count")
            assert [] == english_info.get("custom_list_ids")
            assert True == english_info.get("inherit_parent_restrictions")

            [fiction_info] = english_info.get("sublanes")
            assert english_fiction.id == fiction_info.get("id")
            assert english_fiction.display_name == fiction_info.get("display_name")
            assert english_fiction.visible == fiction_info.get("visible")
            assert 33 == fiction_info.get("count")
            assert [] == fiction_info.get("custom_list_ids")
            assert True == fiction_info.get("inherit_parent_restrictions")

            [sf_info] = fiction_info.get("sublanes")
            assert english_sf.id == sf_info.get("id")
            assert english_sf.display_name == sf_info.get("display_name")
            assert english_sf.visible == sf_info.get("visible")
            assert 22 == sf_info.get("count")
            assert [] == sf_info.get("custom_list_ids")
            assert True == sf_info.get("inherit_parent_restrictions")

            assert spanish.id == spanish_info.get("id")
            assert spanish.display_name == spanish_info.get("display_name")
            assert spanish.visible == spanish_info.get("visible")
            assert 11 == spanish_info.get("count")
            assert [] == spanish_info.get("custom_list_ids")
            assert True == spanish_info.get("inherit_parent_restrictions")

            assert lane_for_list.id == list_info.get("id")
            assert lane_for_list.display_name == list_info.get("display_name")
            assert lane_for_list.visible == list_info.get("visible")
            assert 1 == list_info.get("count")
            assert [list.id] == list_info.get("custom_list_ids")
            assert True == list_info.get("inherit_parent_restrictions")

    def test_lanes_post_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict([])
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert NO_DISPLAY_NAME_FOR_LANE == response

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("display_name", "lane"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert NO_CUSTOM_LISTS_FOR_LANE == response

        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = alm_fixture.ctrl.db.default_library()

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("id", "12345"),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert MISSING_LANE == response

        library = alm_fixture.ctrl.db.library()
        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.library = library
            flask.request.form = MultiDict(
                [
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.lanes,
            )

        lane1 = alm_fixture.ctrl.db.lane("lane1")
        lane2 = alm_fixture.ctrl.db.lane("lane2")
        lane1.customlists += [list]

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("id", lane1.id),
                    ("display_name", "lane2"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS == response

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("display_name", "lane2"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert LANE_WITH_PARENT_AND_DISPLAY_NAME_ALREADY_EXISTS == response

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("parent_id", "12345"),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert MISSING_LANE.uri == response.uri

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("parent_id", lane1.id),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps(["12345"])),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert MISSING_CUSTOM_LIST.uri == response.uri

    def test_lanes_create(self, alm_fixture: AdminLibraryManagerFixture):
        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = alm_fixture.ctrl.db.default_library()

        # The new lane's parent has a sublane already.
        parent = alm_fixture.ctrl.db.lane("parent")
        sibling = alm_fixture.ctrl.db.lane("sibling", parent=parent)

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("parent_id", parent.id),
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 201 == response.status_code

            [lane] = alm_fixture.ctrl.db.session.query(Lane).filter(
                Lane.display_name == "lane"
            )
            assert lane.id == int(response.get_data(as_text=True))
            assert alm_fixture.ctrl.db.default_library() == lane.library
            assert "lane" == lane.display_name
            assert parent == lane.parent
            assert None == lane.media
            assert 1 == len(lane.customlists)
            assert list == lane.customlists[0]
            assert False == lane.inherit_parent_restrictions
            assert 0 == lane.priority

            # The sibling's priority has been shifted down to put the new lane at the top.
            assert 1 == sibling.priority

    def test_lanes_create_shared_list(self, alm_fixture: AdminLibraryManagerFixture):
        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = alm_fixture.ctrl.db.default_library()
        library = alm_fixture.ctrl.db.library()
        alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library=library)

        with alm_fixture.request_context_with_library_and_admin(
            "/", method="POST", library=library
        ):
            flask.request.form = MultiDict(
                [
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 404 == response.status_code

        success = CustomListQueries.share_locally_with_library(
            alm_fixture.ctrl.db.session, list, library
        )
        assert success == True

        with alm_fixture.request_context_with_library_and_admin(
            "/", method="POST", library=library
        ):
            flask.request.form = MultiDict(
                [
                    ("display_name", "lane"),
                    ("custom_list_ids", json.dumps([list.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 201 == response.status_code
            lane_id = int(response.data)

        lane: Lane = get_one(alm_fixture.ctrl.db.session, Lane, id=lane_id)
        assert lane.customlists == [list]
        assert lane.library == library

    def test_lanes_edit(self, alm_fixture: AdminLibraryManagerFixture):

        work = alm_fixture.ctrl.db.work(with_license_pool=True)

        list1, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list1.library = alm_fixture.ctrl.db.default_library()
        list2, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list2.library = alm_fixture.ctrl.db.default_library()
        list2.add_entry(work)

        lane = alm_fixture.ctrl.db.lane("old name")
        lane.customlists += [list1]

        # When we add a list to the lane, the controller will ask the
        # search engine to update lane.size, and it will think there
        # are two works in the lane.
        assert 0 == lane.size
        alm_fixture.ctrl.controller.search_engine.docs = dict(
            id1="value1", id2="value2"
        )

        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("id", str(lane.id)),
                    ("display_name", "new name"),
                    ("custom_list_ids", json.dumps([list2.id])),
                    ("inherit_parent_restrictions", "true"),
                ]
            )

            response = alm_fixture.manager.admin_lanes_controller.lanes()
            assert 200 == response.status_code
            assert lane.id == int(response.get_data(as_text=True))

            assert "new name" == lane.display_name
            assert [list2] == lane.customlists
            assert True == lane.inherit_parent_restrictions
            assert None == lane.media
            assert 2 == lane.size

    def test_default_lane_edit(self, alm_fixture: AdminLibraryManagerFixture):
        """Default lanes only allow the display_name to be edited"""
        lane: Lane = alm_fixture.ctrl.db.lane("default")
        customlist, _ = alm_fixture.ctrl.db.customlist()
        with alm_fixture.request_context_with_library_and_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("id", str(lane.id)),
                    ("parent_id", "12345"),
                    ("display_name", "new name"),
                    ("custom_list_ids", json.dumps([customlist.id])),
                    ("inherit_parent_restrictions", "false"),
                ]
            )
            response = alm_fixture.manager.admin_lanes_controller.lanes()

        assert 200 == response.status_code
        assert lane.id == int(response.get_data(as_text=True))

        assert "new name" == lane.display_name
        # Nothing else changes
        assert [] == lane.customlists
        assert True == lane.inherit_parent_restrictions
        assert None == lane.parent_id

    def test_lane_delete_success(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        lane = alm_fixture.ctrl.db.lane("lane", library=library)
        list, ignore = alm_fixture.ctrl.db.customlist(
            data_source_name=DataSource.LIBRARY_STAFF, num_entries=0
        )
        list.library = library
        lane.customlists += [list]
        assert (
            1
            == alm_fixture.ctrl.db.session.query(Lane)
            .filter(Lane.library == library)
            .count()
        )

        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            response = alm_fixture.manager.admin_lanes_controller.lane(lane.id)
            assert 200 == response.status_code

            # The lane has been deleted.
            assert (
                0
                == alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .count()
            )

            # The custom list still exists though.
            assert (
                1
                == alm_fixture.ctrl.db.session.query(CustomList)
                .filter(CustomList.library == library)
                .count()
            )

        lane = alm_fixture.ctrl.db.lane("lane", library=library)
        lane.customlists += [list]
        child = alm_fixture.ctrl.db.lane("child", parent=lane, library=library)
        child.customlists += [list]
        grandchild = alm_fixture.ctrl.db.lane(
            "grandchild", parent=child, library=library
        )
        grandchild.customlists += [list]
        assert (
            3
            == alm_fixture.ctrl.db.session.query(Lane)
            .filter(Lane.library == library)
            .count()
        )

        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            response = alm_fixture.manager.admin_lanes_controller.lane(lane.id)
            assert 200 == response.status_code

            # The lanes have all been deleted.
            assert (
                0
                == alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .count()
            )

            # The custom list still exists though.
            assert (
                1
                == alm_fixture.ctrl.db.session.query(CustomList)
                .filter(CustomList.library == library)
                .count()
            )

    def test_lane_delete_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            response = alm_fixture.manager.admin_lanes_controller.lane(123)
            assert MISSING_LANE == response

        lane = alm_fixture.ctrl.db.lane("lane")
        library = alm_fixture.ctrl.db.library()
        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            flask.request.library = library
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.lane,
                lane.id,
            )

        with alm_fixture.request_context_with_library_and_admin("/", method="DELETE"):
            response = alm_fixture.manager.admin_lanes_controller.lane(lane.id)
            assert CANNOT_EDIT_DEFAULT_LANE == response

    def test_show_lane_success(self, alm_fixture: AdminLibraryManagerFixture):
        lane = alm_fixture.ctrl.db.lane("lane")
        lane.visible = False
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.show_lane(lane.id)
            assert 200 == response.status_code
            assert True == lane.visible

    def test_show_lane_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.show_lane(123)
            assert MISSING_LANE == response

        parent = alm_fixture.ctrl.db.lane("parent")
        parent.visible = False
        child = alm_fixture.ctrl.db.lane("lane")
        child.visible = False
        child.parent = parent
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.show_lane(child.id)
            assert CANNOT_SHOW_LANE_WITH_HIDDEN_PARENT == response

        alm_fixture.admin.remove_role(
            AdminRole.LIBRARY_MANAGER, alm_fixture.ctrl.db.default_library()
        )
        with alm_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.show_lane,
                parent.id,
            )

    def test_hide_lane_success(self, alm_fixture: AdminLibraryManagerFixture):
        lane = alm_fixture.ctrl.db.lane("lane")
        lane.visible = True
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.hide_lane(lane.id)
            assert 200 == response.status_code
            assert False == lane.visible

    def test_hide_lane_errors(self, alm_fixture: AdminLibraryManagerFixture):
        with alm_fixture.request_context_with_library_and_admin("/"):
            response = alm_fixture.manager.admin_lanes_controller.hide_lane(123456789)
            assert MISSING_LANE == response

        lane = alm_fixture.ctrl.db.lane()
        alm_fixture.admin.remove_role(
            AdminRole.LIBRARY_MANAGER, alm_fixture.ctrl.db.default_library()
        )
        with alm_fixture.request_context_with_library_and_admin("/"):
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.show_lane,
                lane.id,
            )

    def test_reset(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        old_lane = alm_fixture.ctrl.db.lane("old lane", library=library)

        with alm_fixture.request_context_with_library_and_admin("/"):
            flask.request.library = library
            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.reset,
            )

            alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = alm_fixture.manager.admin_lanes_controller.reset()
            assert 200 == response.status_code

            # The old lane is gone.
            assert (
                0
                == alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .filter(Lane.id == old_lane.id)
                .count()
            )
            # tests/test_lanes.py tests the default lane creation, but make sure some
            # lanes were created.
            assert (
                0
                < alm_fixture.ctrl.db.session.query(Lane)
                .filter(Lane.library == library)
                .count()
            )

    def test_change_order(self, alm_fixture: AdminLibraryManagerFixture):
        library = alm_fixture.ctrl.db.library()
        parent1 = alm_fixture.ctrl.db.lane("parent1", library=library)
        parent2 = alm_fixture.ctrl.db.lane("parent2", library=library)
        child1 = alm_fixture.ctrl.db.lane("child1", parent=parent2)
        child2 = alm_fixture.ctrl.db.lane("child2", parent=parent2)
        parent1.priority = 0
        parent2.priority = 1
        child1.priority = 0
        child2.priority = 1

        new_order = [
            {"id": parent2.id, "sublanes": [{"id": child2.id}, {"id": child1.id}]},
            {"id": parent1.id},
        ]

        with alm_fixture.request_context_with_library_and_admin("/"):
            flask.request.library = library
            flask.request.data = json.dumps(new_order)

            pytest.raises(
                AdminNotAuthorized,
                alm_fixture.manager.admin_lanes_controller.change_order,
            )

            alm_fixture.admin.add_role(AdminRole.LIBRARY_MANAGER, library)
            response = alm_fixture.manager.admin_lanes_controller.change_order()
            assert 200 == response.status_code

            assert 0 == parent2.priority
            assert 1 == parent1.priority
            assert 0 == child2.priority
            assert 1 == child1.priority


class DashboardFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)

        self.english_1 = self.ctrl.db.work(
            "Quite British",
            "John Bull",
            language="eng",
            fiction=True,
            with_open_access_download=True,
        )
        self.english_1.license_pools[0].collection = self.ctrl.collection
        self.works = [self.english_1]

        self.manager.external_search.bulk_update(self.works)


@pytest.fixture(scope="function")
def dashboard_fixture(controller_fixture: ControllerFixture) -> DashboardFixture:
    return DashboardFixture(controller_fixture)


class TestDashboardController:
    def test_circulation_events(self, dashboard_fixture: DashboardFixture):
        [lp] = dashboard_fixture.english_1.license_pools
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        ]
        time = utc_now() - timedelta(minutes=len(types))
        for type in types:
            get_one_or_create(
                dashboard_fixture.ctrl.db.session,
                CirculationEvent,
                license_pool=lp,
                type=type,
                start=time,
                end=time,
            )
            time += timedelta(minutes=1)

        with dashboard_fixture.request_context_with_library_and_admin("/"):
            response = (
                dashboard_fixture.manager.admin_dashboard_controller.circulation_events()
            )
            url = AdminAnnotator(
                dashboard_fixture.manager.d_circulation,
                dashboard_fixture.ctrl.db.default_library(),
            ).permalink_for(dashboard_fixture.english_1, lp, lp.identifier)

        events = response["circulation_events"]
        assert types[::-1] == [event["type"] for event in events]
        assert [dashboard_fixture.english_1.title] * len(types) == [
            event["book"]["title"] for event in events
        ]
        assert [url] * len(types) == [event["book"]["url"] for event in events]

        # request fewer events
        with dashboard_fixture.request_context_with_library_and_admin("/?num=2"):
            response = (
                dashboard_fixture.manager.admin_dashboard_controller.circulation_events()
            )
            url = AdminAnnotator(
                dashboard_fixture.manager.d_circulation,
                dashboard_fixture.ctrl.db.default_library(),
            ).permalink_for(dashboard_fixture.english_1, lp, lp.identifier)

        assert 2 == len(response["circulation_events"])

    def test_bulk_circulation_events(self, dashboard_fixture: DashboardFixture):
        [lp] = dashboard_fixture.english_1.license_pools
        edition = dashboard_fixture.english_1.presentation_edition
        identifier = dashboard_fixture.english_1.presentation_edition.primary_identifier
        genres = dashboard_fixture.ctrl.db.session.query(Genre).all()
        get_one_or_create(
            dashboard_fixture.ctrl.db.session,
            WorkGenre,
            work=dashboard_fixture.english_1,
            genre=genres[0],
            affinity=0.2,
        )

        time = utc_now() - timedelta(minutes=1)
        event, ignore = get_one_or_create(
            dashboard_fixture.ctrl.db.session,
            CirculationEvent,
            license_pool=lp,
            type=CirculationEvent.DISTRIBUTOR_CHECKOUT,
            start=time,
            end=time,
        )
        time += timedelta(minutes=1)

        # Try an end-to-end test, getting all circulation events for
        # the current day.
        with dashboard_fixture.ctrl.app.test_request_context("/"):
            (
                response,
                requested_date,
                date_end,
                library_short_name,
            ) = (
                dashboard_fixture.manager.admin_dashboard_controller.bulk_circulation_events()
            )
        reader = csv.reader(
            [row for row in response.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row
        assert 1 == len(rows)
        [row] = rows
        assert CirculationEvent.DISTRIBUTOR_CHECKOUT == row[1]
        assert identifier.identifier == row[2]
        assert identifier.type == row[3]
        assert edition.title == row[4]
        assert genres[0].name == row[12]

        # Now verify that this works by passing incoming query
        # parameters into a LocalAnalyticsExporter object.
        class MockLocalAnalyticsExporter:
            def export(self, _db, date_start, date_end, locations, library):
                self.called_with = (_db, date_start, date_end, locations, library)
                return "A CSV file"

        exporter = MockLocalAnalyticsExporter()
        with dashboard_fixture.ctrl.request_context_with_library(
            "/?date=2018-01-01&dateEnd=2018-01-04&locations=loc1,loc2"
        ):
            (
                response,
                requested_date,
                date_end,
                library_short_name,
            ) = dashboard_fixture.manager.admin_dashboard_controller.bulk_circulation_events(
                analytics_exporter=exporter
            )

            # export() was called with the arguments we expect.
            #
            args = list(exporter.called_with)
            assert dashboard_fixture.ctrl.db.session == args.pop(0)
            assert datetime.date(2018, 1, 1) == args.pop(0)
            # This is the start of the day _after_ the dateEnd we
            # specified -- we want all events that happened _before_
            # 2018-01-05.
            assert datetime.date(2018, 1, 5) == args.pop(0)
            assert "loc1,loc2" == args.pop(0)
            assert dashboard_fixture.ctrl.db.default_library() == args.pop(0)
            assert [] == args

            # The data returned is whatever export() returned.
            assert "A CSV file" == response

            # The other data is necessary to build a filename for the
            # "CSV file".
            assert "2018-01-01" == requested_date

            # Note that the date_end is the date we requested --
            # 2018-01-04 -- not the cutoff time passed in to export(),
            # which is the start of the subsequent day.
            assert "2018-01-04" == date_end
            assert (
                dashboard_fixture.ctrl.db.default_library().short_name
                == library_short_name
            )

    def test_stats_calls_with_correct_arguments(
        self, dashboard_fixture: DashboardFixture
    ):
        # Ensure that the injected statistics function is called properly.
        stats_mock = mock.MagicMock(return_value={})
        with dashboard_fixture.request_context_with_admin(
            "/", admin=dashboard_fixture.admin
        ):
            response = dashboard_fixture.manager.admin_dashboard_controller.stats(
                stats_function=stats_mock
            )
        assert 1 == stats_mock.call_count
        assert (
            dashboard_fixture.admin,
            dashboard_fixture.ctrl.db.session,
        ) == stats_mock.call_args.args
        assert {} == stats_mock.call_args.kwargs


class TestSettingsController:
    def test_get_integration_protocols(self):
        """Test the _get_integration_protocols helper method."""

        class Protocol:
            __module__ = "my name"
            NAME = "my label"
            DESCRIPTION = "my description"
            SITEWIDE = True
            SETTINGS = [1, 2, 3]
            CHILD_SETTINGS = [4, 5]
            LIBRARY_SETTINGS = [6]
            CARDINALITY = 1

        [result] = SettingsController._get_integration_protocols([Protocol])
        expect = dict(
            sitewide=True,
            description="my description",
            settings=[1, 2, 3],
            library_settings=[6],
            child_settings=[4, 5],
            label="my label",
            cardinality=1,
            name="my name",
        )
        assert expect == result

        # Remove the CARDINALITY setting
        del Protocol.CARDINALITY

        # And look in a different place for the name.
        [result] = SettingsController._get_integration_protocols(
            [Protocol], protocol_name_attr="NAME"
        )

        assert "my label" == result["name"]
        assert "cardinality" not in result

    def test_get_integration_info(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        """Test the _get_integration_info helper method."""
        m = (
            settings_ctrl_fixture.manager.admin_settings_controller._get_integration_info
        )

        # Test the case where there are integrations in the database
        # with the given goal, but none of them match the
        # configuration.
        goal = settings_ctrl_fixture.ctrl.db.fresh_str()
        integration = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol="a protocol", goal=goal
        )
        assert [] == m(goal, [dict(name="some other protocol")])

    def test_create_integration(self, settings_ctrl_fixture: SettingsControllerFixture):
        """Test the _create_integration helper method."""

        m = settings_ctrl_fixture.manager.admin_settings_controller._create_integration

        protocol_definitions = [
            dict(name="allow many"),
            dict(name="allow one", cardinality=1),
        ]
        goal = "some goal"

        # You get an error if you don't pass in a protocol.
        assert (NO_PROTOCOL_FOR_NEW_SERVICE, False) == m(
            protocol_definitions, None, goal
        )

        # You get an error if you do provide a protocol but no definition
        # for it can be found.
        assert (UNKNOWN_PROTOCOL, False) == m(
            protocol_definitions, "no definition", goal
        )

        # If the protocol has multiple cardinality you can create as many
        # integrations using that protocol as you want.
        i1, is_new1 = m(protocol_definitions, "allow many", goal)
        assert True == is_new1

        i2, is_new2 = m(protocol_definitions, "allow many", goal)
        assert True == is_new2

        assert i1 != i2
        for i in [i1, i2]:
            assert "allow many" == i.protocol
            assert goal == i.goal

        # If the protocol has single cardinality, you can only create one
        # integration using that protocol before you start getting errors.
        i1, is_new1 = m(protocol_definitions, "allow one", goal)
        assert True == is_new1

        i2, is_new2 = m(protocol_definitions, "allow one", goal)
        assert False == is_new2
        assert DUPLICATE_INTEGRATION == i2

    def test_validate_formats(self, settings_ctrl_fixture: SettingsControllerFixture):
        class MockValidator(Validator):
            def __init__(self):
                self.was_called = False
                self.args = []

            def validate(self, settings, content):
                self.was_called = True
                self.args.append(settings)
                self.args.append(content)

            def validate_error(self, settings, content):
                return INVALID_EMAIL

        validator = MockValidator()

        with settings_ctrl_fixture.request_context_with_admin("/", method="POST"):
            flask.request.form = MultiDict(
                [
                    ("name", "The New York Public Library"),
                    ("short_name", "nypl"),
                    (Configuration.WEBSITE_URL, "https://library.library/"),
                    (
                        Configuration.DEFAULT_NOTIFICATION_EMAIL_ADDRESS,
                        "email@example.com",
                    ),
                    (Configuration.HELP_EMAIL, "help@example.com"),
                ]
            )
            flask.request.files = MultiDict([(Configuration.LOGO, StringIO())])
            response = settings_ctrl_fixture.manager.admin_settings_controller.validate_formats(
                Configuration.LIBRARY_SETTINGS, validator
            )
            assert response == None
            assert validator.was_called == True
            assert validator.args[0] == Configuration.LIBRARY_SETTINGS
            assert validator.args[1] == {
                "files": flask.request.files,
                "form": flask.request.form,
            }

            validator.validate = validator.validate_error
            # If the validator returns an problem detail, validate_formats returns it.
            response = settings_ctrl_fixture.manager.admin_settings_controller.validate_formats(
                Configuration.LIBRARY_SETTINGS, validator
            )
            assert response == INVALID_EMAIL

    def test__mirror_integration_settings(
        self, settings_ctrl_fixture: SettingsControllerFixture
    ):
        # If no storage integrations are available, return none
        mirror_integration_settings = (
            settings_ctrl_fixture.manager.admin_settings_controller._mirror_integration_settings
        )

        assert None == mirror_integration_settings()

        # Storages created will appear for settings of any purpose
        storage1 = settings_ctrl_fixture.ctrl.db.external_integration(
            "protocol1",
            ExternalIntegration.STORAGE_GOAL,
            name="storage1",
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "covers",
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "open-access-books",
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "protected-access-books",
            },
        )

        settings = mirror_integration_settings()

        assert settings[0]["key"] == "covers_mirror_integration_id"
        assert settings[0]["label"] == "Covers Mirror"
        assert (
            settings[0]["options"][0]["key"]
            == settings_ctrl_fixture.manager.admin_settings_controller.NO_MIRROR_INTEGRATION
        )
        assert settings[0]["options"][1]["key"] == str(storage1.id)
        assert settings[1]["key"] == "books_mirror_integration_id"
        assert settings[1]["label"] == "Open Access Books Mirror"
        assert (
            settings[1]["options"][0]["key"]
            == settings_ctrl_fixture.manager.admin_settings_controller.NO_MIRROR_INTEGRATION
        )
        assert settings[1]["options"][1]["key"] == str(storage1.id)
        assert settings[2]["label"] == "Protected Access Books Mirror"
        assert (
            settings[2]["options"][0]["key"]
            == settings_ctrl_fixture.manager.admin_settings_controller.NO_MIRROR_INTEGRATION
        )
        assert settings[2]["options"][1]["key"] == str(storage1.id)

        storage2 = settings_ctrl_fixture.ctrl.db.external_integration(
            "protocol2",
            ExternalIntegration.STORAGE_GOAL,
            name="storage2",
            settings={
                S3UploaderConfiguration.BOOK_COVERS_BUCKET_KEY: "covers",
                S3UploaderConfiguration.OA_CONTENT_BUCKET_KEY: "open-access-books",
                S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "protected-access-books",
            },
        )
        settings = mirror_integration_settings()

        assert settings[0]["key"] == "covers_mirror_integration_id"
        assert settings[0]["label"] == "Covers Mirror"
        assert (
            settings[0]["options"][0]["key"]
            == settings_ctrl_fixture.manager.admin_settings_controller.NO_MIRROR_INTEGRATION
        )
        assert settings[0]["options"][1]["key"] == str(storage1.id)
        assert settings[0]["options"][2]["key"] == str(storage2.id)
        assert settings[1]["key"] == "books_mirror_integration_id"
        assert settings[1]["label"] == "Open Access Books Mirror"
        assert (
            settings[1]["options"][0]["key"]
            == settings_ctrl_fixture.manager.admin_settings_controller.NO_MIRROR_INTEGRATION
        )
        assert settings[1]["options"][1]["key"] == str(storage1.id)
        assert settings[1]["options"][2]["key"] == str(storage2.id)
        assert settings[2]["label"] == "Protected Access Books Mirror"
        assert (
            settings[2]["options"][0]["key"]
            == settings_ctrl_fixture.manager.admin_settings_controller.NO_MIRROR_INTEGRATION
        )
        assert settings[2]["options"][1]["key"] == str(storage1.id)

    def test_check_url_unique(self, settings_ctrl_fixture: SettingsControllerFixture):
        # Verify our ability to catch duplicate integrations for a
        # given URL.
        m = settings_ctrl_fixture.manager.admin_settings_controller.check_url_unique

        # Here's an ExternalIntegration.
        protocol = "a protocol"
        goal = "a goal"
        original = settings_ctrl_fixture.ctrl.db.external_integration(
            url="http://service/", protocol=protocol, goal=goal
        )
        protocol = original.protocol
        goal = original.goal

        # Here's another ExternalIntegration that might or might not
        # be about to become a duplicate of the original.
        new = settings_ctrl_fixture.ctrl.db.external_integration(
            protocol=protocol, goal="new goal"
        )
        new.goal = original.goal
        assert new != original

        # We're going to call this helper function multiple times to check if
        # different scenarios trip the "duplicate" logic.
        def is_dupe(url, protocol, goal):
            result = m(new, url, protocol, goal)
            if result is None:
                return False
            elif result is INTEGRATION_URL_ALREADY_IN_USE:
                return True
            else:
                raise Exception(
                    "check_url_unique must return either the problem detail or None"
                )

        # The original ExternalIntegration is not a duplicate of itself.
        assert None == m(original, original.url, protocol, goal)

        # However, any other ExternalIntegration with the same URL,
        # protocol, and goal is considered a duplicate.
        assert True == is_dupe(original.url, protocol, goal)

        # Minor URL differences are ignored when considering duplicates
        # -- this is with help from url_variants().
        assert True == is_dupe("https://service/", protocol, goal)
        assert True == is_dupe("https://service", protocol, goal)

        # Not all variants are handled in this way
        assert False == is_dupe("https://service/#fragment", protocol, goal)

        # If any of URL, protocol, and goal are different, then the
        # integration is not considered a duplicate.
        assert False == is_dupe("different url", protocol, goal)
        assert False == is_dupe(original.url, "different protocol", goal)
        assert False == is_dupe(original.url, protocol, "different goal")

        # If you're not considering a URL at all, we assume no
        # duplicate.
        assert False == is_dupe(None, protocol, goal)

    def test_url_variants(self):
        # Test the helper method that generates slight variants of
        # any given URL.
        def m(url):
            return list(SettingsController.url_variants(url))

        # No URL, no variants.
        assert [] == m(None)
        assert [] == m("not a url")

        # Variants of an HTTP URL with a trailing slash.
        assert ["http://url/", "http://url", "https://url/", "https://url"] == m(
            "http://url/"
        )

        # Variants of an HTTPS URL with a trailing slash.
        assert ["https://url/", "https://url", "http://url/", "http://url"] == m(
            "https://url/"
        )

        # Variants of a URL with no trailing slash.
        assert ["https://url", "https://url/", "http://url", "http://url/"] == m(
            "https://url"
        )
