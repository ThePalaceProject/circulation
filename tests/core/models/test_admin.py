from datetime import timedelta

import pytest
from freezegun import freeze_time

from core.model import create
from core.model.admin import Admin, AdminRole
from core.problem_details import INVALID_RESET_PASSWORD_TOKEN
from core.util.datetime_helpers import utc_now
from core.util.problem_detail import ProblemDetail
from tests.fixtures.database import DatabaseTransactionFixture


class AdminFixture:
    admin: Admin
    db: DatabaseTransactionFixture

    def __init__(self, admin: Admin, db: DatabaseTransactionFixture):
        self.admin = admin
        self.db = db


@pytest.fixture()
def admin_fixture(db: DatabaseTransactionFixture) -> AdminFixture:
    a, ignore = create(db.session, Admin, email="admin@nypl.org")
    a.password = "password"
    return AdminFixture(a, db)


class TestAdmin:
    def test_password_hashed(self, admin_fixture: AdminFixture):
        pytest.raises(NotImplementedError, lambda: admin_fixture.admin.password)
        assert isinstance(admin_fixture.admin.password_hashed, str)
        assert admin_fixture.admin.password_hashed.startswith("$2b$")

    def test_with_password(self, admin_fixture: AdminFixture):
        session = admin_fixture.db.session
        session.delete(admin_fixture.admin)
        assert [] == Admin.with_password(session).all()

        admin, ignore = create(session, Admin, email="admin@nypl.org")
        assert [] == Admin.with_password(session).all()

        admin.password = "password"
        assert [admin] == Admin.with_password(session).all()

        admin2, ignore = create(session, Admin, email="admin2@nypl.org")
        assert [admin] == Admin.with_password(session).all()

        admin2.password = "password2"
        assert {admin, admin2} == set(Admin.with_password(session).all())

    def test_with_email_spaces(self, admin_fixture: AdminFixture):
        admin_spaces, ignore = create(
            admin_fixture.db.session, Admin, email="test@email.com "
        )
        assert "test@email.com" == admin_spaces.email

    def test_has_password(self, admin_fixture: AdminFixture):
        assert True == admin_fixture.admin.has_password("password")
        assert False == admin_fixture.admin.has_password("banana")

    def test_authenticate(self, admin_fixture: AdminFixture):
        session = admin_fixture.db.session
        other_admin, ignore = create(session, Admin, email="other@nypl.org")
        other_admin.password = "banana"
        assert admin_fixture.admin == Admin.authenticate(
            session, "admin@nypl.org", "password"
        )
        assert None == Admin.authenticate(session, "other@nypl.org", "password")
        assert None == Admin.authenticate(session, "example@nypl.org", "password")

    def test_roles(self, admin_fixture: AdminFixture):
        library = admin_fixture.db.default_library()
        other_library = admin_fixture.db.library()

        # The admin has no roles yet.
        admin = admin_fixture.admin
        assert False == admin.is_system_admin()
        assert False == admin.is_library_manager(library)
        assert False == admin.is_librarian(library)

        admin.add_role(AdminRole.SYSTEM_ADMIN)
        assert True == admin.is_system_admin()
        assert True == admin.is_sitewide_library_manager()
        assert True == admin.is_sitewide_librarian()
        assert True == admin.is_library_manager(library)
        assert True == admin.is_librarian(library)

        admin.remove_role(AdminRole.SYSTEM_ADMIN)
        admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        assert False == admin.is_system_admin()
        assert True == admin.is_sitewide_library_manager()
        assert True == admin.is_sitewide_librarian()
        assert True == admin.is_library_manager(library)
        assert True == admin.is_librarian(library)

        admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert False == admin.is_system_admin()
        assert False == admin.is_sitewide_library_manager()
        assert True == admin.is_sitewide_librarian()
        assert False == admin.is_library_manager(library)
        assert True == admin.is_librarian(library)

        admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        admin.add_role(AdminRole.LIBRARY_MANAGER, library)
        assert False == admin.is_system_admin()
        assert False == admin.is_sitewide_library_manager()
        assert False == admin.is_sitewide_librarian()
        assert True == admin.is_library_manager(library)
        assert True == admin.is_librarian(library)

        admin.remove_role(AdminRole.LIBRARY_MANAGER, library)
        admin.add_role(AdminRole.LIBRARIAN, library)
        assert False == admin.is_system_admin()
        assert False == admin.is_sitewide_library_manager()
        assert False == admin.is_sitewide_librarian()
        assert False == admin.is_library_manager(library)
        assert True == admin.is_librarian(library)

        admin.remove_role(AdminRole.LIBRARIAN, library)
        assert False == admin.is_system_admin()
        assert False == admin.is_sitewide_library_manager()
        assert False == admin.is_sitewide_librarian()
        assert False == admin.is_library_manager(library)
        assert False == admin.is_librarian(library)

        admin.add_role(AdminRole.LIBRARY_MANAGER, other_library)
        assert False == admin.is_library_manager(library)
        assert True == admin.is_library_manager(other_library)
        admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert False == admin.is_library_manager(library)
        assert True == admin.is_library_manager(other_library)
        assert True == admin.is_librarian(library)
        assert True == admin.is_librarian(other_library)
        admin.remove_role(AdminRole.LIBRARY_MANAGER, other_library)
        assert False == admin.is_library_manager(library)
        assert False == admin.is_library_manager(other_library)
        assert True == admin.is_librarian(library)
        assert True == admin.is_librarian(other_library)

    def test_can_see_collection(self, admin_fixture: AdminFixture):
        # This collection is only visible to system admins since it has no libraries.
        c1 = admin_fixture.db.collection()

        # This collection is visible to libraries of its library.
        c2 = admin_fixture.db.collection()
        c2.libraries += [admin_fixture.db.default_library()]

        # The admin has no roles yet.
        admin = admin_fixture.admin
        assert False == admin.can_see_collection(c1)
        assert False == admin.can_see_collection(c2)

        admin.add_role(AdminRole.SYSTEM_ADMIN)
        assert True == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.SYSTEM_ADMIN)
        admin.add_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        assert False == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.SITEWIDE_LIBRARY_MANAGER)
        admin.add_role(AdminRole.SITEWIDE_LIBRARIAN)
        assert False == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.SITEWIDE_LIBRARIAN)
        admin.add_role(AdminRole.LIBRARY_MANAGER, admin_fixture.db.default_library())
        assert False == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.LIBRARY_MANAGER, admin_fixture.db.default_library())
        admin.add_role(AdminRole.LIBRARIAN, admin_fixture.db.default_library())
        assert False == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.LIBRARIAN, admin_fixture.db.default_library())
        assert False == admin.can_see_collection(c1)
        assert False == admin.can_see_collection(c2)

    def test_validate_reset_password_token_and_fetch_admin(
        self, admin_fixture: AdminFixture
    ):
        admin = admin_fixture.admin
        db_session = admin_fixture.db.session
        secret_key = "secret"
        admin_id = admin.id
        assert isinstance(admin_id, int)

        # Random manually generated token - unsuccessful validation
        random_token = "random"
        invalid_token = Admin.validate_reset_password_token_and_fetch_admin(
            random_token, admin_id, db_session, secret_key
        )
        assert isinstance(invalid_token, ProblemDetail)
        assert invalid_token == INVALID_RESET_PASSWORD_TOKEN

        # Generated valid token but manually changed - unsuccessful validation
        tampered_token = f"tampered-{admin.generate_reset_password_token(secret_key)}"
        invalid_token = Admin.validate_reset_password_token_and_fetch_admin(
            tampered_token, admin_id, db_session, secret_key
        )
        assert isinstance(invalid_token, ProblemDetail)
        assert invalid_token == INVALID_RESET_PASSWORD_TOKEN

        # Valid token but too much time has passed - unsuccessful validation with "expired" keyword
        valid_token = admin.generate_reset_password_token(secret_key)
        with freeze_time(
            utc_now() + timedelta(seconds=Admin.RESET_PASSWORD_TOKEN_MAX_AGE + 1)
        ):
            expired_token = Admin.validate_reset_password_token_and_fetch_admin(
                valid_token, admin_id, db_session, secret_key
            )
            assert isinstance(expired_token, ProblemDetail)
            assert expired_token.uri == INVALID_RESET_PASSWORD_TOKEN.uri
            assert expired_token.detail is not None
            assert "expired" in expired_token.detail

        # Valid token but invalid admin id - unsuccessful validation
        valid_token = admin.generate_reset_password_token(secret_key)
        invalid_admin_id = admin_id + 1
        invalid_token = Admin.validate_reset_password_token_and_fetch_admin(
            valid_token, invalid_admin_id, db_session, secret_key
        )
        assert isinstance(invalid_token, ProblemDetail)
        assert invalid_token == INVALID_RESET_PASSWORD_TOKEN

        # Valid token but the admin email has changed in the meantime - strange situation - unsuccessful validation
        admin.email = "changed@email.com"
        invalid_email = Admin.validate_reset_password_token_and_fetch_admin(
            valid_token, admin_id, db_session, secret_key
        )
        assert isinstance(invalid_email, ProblemDetail)
        assert invalid_email == INVALID_RESET_PASSWORD_TOKEN

        # Valid token - admin is successfully extracted from token
        valid_token = admin.generate_reset_password_token(secret_key)
        extracted_admin = Admin.validate_reset_password_token_and_fetch_admin(
            valid_token, admin_id, db_session, secret_key
        )
        assert isinstance(extracted_admin, Admin)
        assert extracted_admin == admin
