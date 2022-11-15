import pytest

from core.model import create
from core.model.admin import Admin, AdminRole
from tests.fixtures.database import DatabaseTransactionFixture


class AdminFixture:
    admin: Admin
    database_fixture: DatabaseTransactionFixture

    def __init__(self, admin: Admin, database_transaction: DatabaseTransactionFixture):
        self.admin = admin
        self.database_fixture = database_transaction


@pytest.fixture()
def admin_fixture(db) -> AdminFixture:
    a, ignore = create(db.session(), Admin, email="admin@nypl.org")
    a.password = "password"
    return AdminFixture(a, db)


class TestAdmin:
    def test_password_hashed(self, admin_fixture: AdminFixture):
        pytest.raises(NotImplementedError, lambda: admin_fixture.admin.password)
        assert admin_fixture.admin.password_hashed.startswith("$2a$")

    def test_with_password(self, admin_fixture: AdminFixture):
        session = admin_fixture.database_fixture.session()
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
        session = admin_fixture.database_fixture.session()
        admin_spaces, ignore = create(session, Admin, email="test@email.com ")
        assert "test@email.com" == admin_spaces.email

    def test_has_password(self, admin_fixture: AdminFixture):
        assert True == admin_fixture.admin.has_password("password")
        assert False == admin_fixture.admin.has_password("banana")

    def test_authenticate(self, admin_fixture: AdminFixture):
        session = admin_fixture.database_fixture.session()
        other_admin, ignore = create(session, Admin, email="other@nypl.org")
        other_admin.password = "banana"
        assert admin_fixture.admin == Admin.authenticate(
            session, "admin@nypl.org", "password"
        )
        assert None == Admin.authenticate(session, "other@nypl.org", "password")
        assert None == Admin.authenticate(session, "example@nypl.org", "password")

    def test_roles(self, admin_fixture: AdminFixture):
        database_fixture = admin_fixture.database_fixture
        library = database_fixture.default_library()
        other_library = database_fixture.library()

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
        database_fixture = admin_fixture.database_fixture

        # This collection is only visible to system admins since it has no libraries.
        c1 = database_fixture.collection()

        # This collection is visible to libraries of its library.
        c2 = database_fixture.collection()
        c2.libraries += [database_fixture.default_library()]

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
        admin.add_role(AdminRole.LIBRARY_MANAGER, database_fixture.default_library())
        assert False == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.LIBRARY_MANAGER, database_fixture.default_library())
        admin.add_role(AdminRole.LIBRARIAN, database_fixture.default_library())
        assert False == admin.can_see_collection(c1)
        assert True == admin.can_see_collection(c2)

        admin.remove_role(AdminRole.LIBRARIAN, database_fixture.default_library())
        assert False == admin.can_see_collection(c1)
        assert False == admin.can_see_collection(c2)
