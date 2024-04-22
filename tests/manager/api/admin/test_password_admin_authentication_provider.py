from unittest.mock import MagicMock, create_autospec

import pytest
from pytest import LogCaptureFixture

from palace.manager.api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from palace.manager.api.admin.problem_details import INVALID_ADMIN_CREDENTIALS
from palace.manager.service.email.email import send_email
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.util import create
from tests.fixtures.database import DatabaseTransactionFixture


class PasswordAdminAuthenticationProviderFixture:
    def __init__(self):
        self.mock_send_email = create_autospec(send_email)
        self.password_auth = PasswordAdminAuthenticationProvider(
            send_email=self.mock_send_email
        )


@pytest.fixture
def password_auth_provider() -> PasswordAdminAuthenticationProviderFixture:
    return PasswordAdminAuthenticationProviderFixture()


class TestPasswordAdminAuthenticationProvider:
    def test_sign_in(
        self,
        db: DatabaseTransactionFixture,
        password_auth_provider: PasswordAdminAuthenticationProviderFixture,
    ):
        password_auth = password_auth_provider.password_auth

        # There are two admins with passwords.
        admin1, ignore = create(db.session, Admin, email="admin1@example.org")
        admin1.password = "pass1"
        admin2, ignore = create(db.session, Admin, email="admin2@example.org")
        admin2.password = "pass2"

        # This admin doesn't have a password.
        admin3, ignore = create(db.session, Admin, email="admin3@example.org")

        # Both admins with passwords can sign in.
        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(email="admin1@example.org", password="pass1", redirect="foo"),
        )
        assert "admin1@example.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(email="admin2@example.org", password="pass2", redirect="foo"),
        )
        assert "admin2@example.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

        # An admin can't sign in with an incorrect password..
        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(
                email="admin1@example.org", password="not-the-password", redirect="foo"
            ),
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # An admin can't sign in with a different admin's password.
        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(email="admin1@example.org", password="pass2", redirect="foo"),
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # The admin with no password can't sign in.
        admin_details, redirect = password_auth.sign_in(
            db.session, dict(email="admin3@example.org", redirect="foo")
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # An admin email that's not in the db at all can't sign in.
        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(email="admin4@example.org", password="pass1", redirect="foo"),
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # Test with "empty" redirect urls, should redirect to admin home page
        for redirect in (None, "None", "null"):
            admin_details, redirect = password_auth.sign_in(
                db.session,
                dict(email="admin1@example.org", password="pass1", redirect=redirect),
            )
            assert redirect == "/admin/web"

    def test_sign_in_case_insensitive(
        self,
        db: DatabaseTransactionFixture,
        password_auth_provider: PasswordAdminAuthenticationProviderFixture,
    ):
        password_auth = password_auth_provider.password_auth

        # There are two admins with passwords.
        admin1, ignore = create(db.session, Admin, email="admin1@example.org")
        admin1.password = "pass1"
        admin2, ignore = create(db.session, Admin, email="ADMIN2@example.org")
        admin2.password = "pass2"

        # Case insensitive test, both ways
        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(email="ADmin1@example.org", password="pass1", redirect="foo"),
        )
        assert "admin1@example.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

        admin_details, redirect = password_auth.sign_in(
            db.session,
            dict(email="admin2@example.org", password="pass2", redirect="foo"),
        )
        assert "ADMIN2@example.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

    def test_send_reset_password_email(
        self,
        password_auth_provider: PasswordAdminAuthenticationProviderFixture,
        caplog: LogCaptureFixture,
    ):
        password_auth = password_auth_provider.password_auth
        mock_admin = MagicMock()
        mock_admin.email = None
        assert (
            password_auth.send_reset_password_email(mock_admin, "reset_password_url")
            is None
        )
        assert (
            "Admin has no email address, cannot send reset password email"
            in caplog.text
        )
