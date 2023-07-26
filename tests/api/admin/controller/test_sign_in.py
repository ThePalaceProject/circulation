import flask
import pytest
from werkzeug.datastructures import ImmutableMultiDict

from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.problem_details import (
    ADMIN_AUTH_MECHANISM_NOT_CONFIGURED,
    ADMIN_AUTH_NOT_CONFIGURED,
    INVALID_ADMIN_CREDENTIALS,
)
from core.model import Admin, create
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


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

        admin_email = sign_in_fixture.admin.email
        assert isinstance(admin_email, str)

        # Returns an error if there's no admin with the provided email.
        with sign_in_fixture.ctrl.app.test_request_context(
            "/admin/sign_in_with_password", method="POST"
        ):
            flask.request.form = ImmutableMultiDict(
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
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", admin_email),
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
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", admin_email),
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
            flask.request.form = ImmutableMultiDict(
                [
                    ("email", admin_email),
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
            flask.request.form = ImmutableMultiDict(
                [
                    ("password", "new"),
                ]
            )
            response = (
                sign_in_fixture.manager.admin_sign_in_controller.change_password()
            )
            assert 200 == response.status_code
            assert isinstance(admin.email, str)
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
