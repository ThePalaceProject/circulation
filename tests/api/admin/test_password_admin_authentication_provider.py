from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.problem_details import *
from core.model import Admin, create
from tests.fixtures.database import DatabaseTransactionFixture


class TestPasswordAdminAuthenticationProvider:
    def test_sign_in(self, db: DatabaseTransactionFixture):
        password_auth = PasswordAdminAuthenticationProvider(secret_key="secret_key")

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

    def test_sign_in_case_insensitive(self, db: DatabaseTransactionFixture):
        password_auth = PasswordAdminAuthenticationProvider(secret_key="secret_key")

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
