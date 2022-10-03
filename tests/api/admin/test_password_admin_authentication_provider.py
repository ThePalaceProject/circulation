from api.admin.password_admin_authentication_provider import (
    PasswordAdminAuthenticationProvider,
)
from api.admin.problem_details import *
from core.model import Admin, create
from core.testing import DatabaseTest


class TestPasswordAdminAuthenticationProvider(DatabaseTest):
    def test_sign_in(self):
        password_auth = PasswordAdminAuthenticationProvider(None)

        # There are two admins with passwords.
        admin1, ignore = create(self._db, Admin, email="admin1@nypl.org")
        admin1.password = "pass1"
        admin2, ignore = create(self._db, Admin, email="admin2@nypl.org")
        admin2.password = "pass2"

        # This admin doesn't have a password.
        admin3, ignore = create(self._db, Admin, email="admin3@nypl.org")

        # Both admins with passwords can sign in.
        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="admin1@nypl.org", password="pass1", redirect="foo")
        )
        assert "admin1@nypl.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="admin2@nypl.org", password="pass2", redirect="foo")
        )
        assert "admin2@nypl.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

        # An admin can't sign in with an incorrect password..
        admin_details, redirect = password_auth.sign_in(
            self._db,
            dict(email="admin1@nypl.org", password="not-the-password", redirect="foo"),
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # An admin can't sign in with a different admin's password.
        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="admin1@nypl.org", password="pass2", redirect="foo")
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # The admin with no password can't sign in.
        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="admin3@nypl.org", redirect="foo")
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # An admin email that's not in the db at all can't sign in.
        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="admin4@nypl.org", password="pass1", redirect="foo")
        )
        assert INVALID_ADMIN_CREDENTIALS == admin_details
        assert None == redirect

        # Test with "empty" redirect urls, should redirect to admin home page
        for redirect in (None, "None", "null"):
            admin_details, redirect = password_auth.sign_in(
                self._db,
                dict(email="admin1@nypl.org", password="pass1", redirect=redirect),
            )
            assert redirect == "/admin/web"

    def test_sign_in_case_insensitive(self):
        password_auth = PasswordAdminAuthenticationProvider(None)

        # There are two admins with passwords.
        admin1, ignore = create(self._db, Admin, email="admin1@nypl.org")
        admin1.password = "pass1"
        admin2, ignore = create(self._db, Admin, email="ADMIN2@nypL.Org")
        admin2.password = "pass2"

        # Case insensitive test, both ways
        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="ADmin1@nyPL.Org", password="pass1", redirect="foo")
        )
        assert "admin1@nypl.org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect

        admin_details, redirect = password_auth.sign_in(
            self._db, dict(email="admin2@nypl.org", password="pass2", redirect="foo")
        )
        assert "ADMIN2@nypL.Org" == admin_details.get("email")
        assert PasswordAdminAuthenticationProvider.NAME == admin_details.get("type")
        assert "foo" == redirect
