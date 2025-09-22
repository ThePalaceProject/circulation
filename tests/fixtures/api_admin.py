from contextlib import contextmanager

import pytest

from palace.manager.api.admin.controller import setup_admin_controllers
from palace.manager.api.app import initialize_admin
from palace.manager.api.circulation_manager import CirculationManager
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.util import create
from tests.fixtures.api_controller import ControllerFixture, WorkSpec


class AdminControllerFixture:
    ctrl: ControllerFixture
    admin: Admin
    manager: CirculationManager

    BOOKS: list[WorkSpec] = []

    def __init__(self, controller_fixture: ControllerFixture):
        self.ctrl = controller_fixture
        self.manager = self.ctrl.manager
        initialize_admin(controller_fixture.db.session)
        setup_admin_controllers(controller_fixture.manager)
        self.admin, ignore = create(
            controller_fixture.db.session,
            Admin,
            email="example@nypl.org",
        )
        # This is a hash for 'password', we use the hash directly to avoid the cost
        # of doing the password hashing during test setup.
        self.admin.password_hashed = (
            "$2a$12$Dw74btoAgh49.vtOB56xPuumtcOY9HCZKS3RYImR42lR5IiT7PIOW"
        )

    @contextmanager
    def request_context_with_admin(self, route, *args, **kwargs):
        admin = self.admin
        if "admin" in kwargs:
            admin = kwargs.pop("admin")
        with self.ctrl.app.test_request_context(route, *args, **kwargs) as c:
            c.request.form = {}
            c.request.files = {}
            self.ctrl.db.session.begin_nested()
            setattr(c.request, "admin", admin)
            try:
                yield c
            finally:
                self.ctrl.db.session.commit()

    @contextmanager
    def request_context_with_library_and_admin(self, route, *args, **kwargs):
        admin = self.admin
        if "admin" in kwargs:
            admin = kwargs.pop("admin")
        with self.ctrl.request_context_with_library(route, *args, **kwargs) as c:
            c.request.form = {}
            c.request.files = {}
            setattr(c.request, "admin", admin)
            yield c


@pytest.fixture(scope="function")
def admin_ctrl_fixture(controller_fixture: ControllerFixture) -> AdminControllerFixture:
    return AdminControllerFixture(controller_fixture)


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
