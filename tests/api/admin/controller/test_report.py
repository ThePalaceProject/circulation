from http import HTTPStatus

import pytest

from core.model import create
from core.model.admin import Admin, AdminRole
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class ReportControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)


@pytest.fixture
def report_fixture(
    controller_fixture: ControllerFixture,
) -> ReportControllerFixture:
    return ReportControllerFixture(controller_fixture)


class TestReportController:
    def test_generate_inventory_report(self, report_fixture: ReportControllerFixture):
        ctrl = report_fixture.manager.admin_report_controller
        db = report_fixture.ctrl.db

        system_admin, _ = create(db.session, Admin, email="admin@email.com")
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        default = db.default_library()
        library1 = db.library()
        with report_fixture.request_context_with_admin(
            f"/",
            admin=system_admin,
        ) as ctx:
            response = ctrl.generate_inventory_report()
            assert response.status_code == HTTPStatus.ACCEPTED
            assert response.response["message"].__contains__("admin@email.com")
            assert not response.response["message"].__contains__("already")

        # check that when generating a duplicate request a 409 is returned.
        with report_fixture.request_context_with_admin(
            f"/",
            admin=system_admin,
        ) as ctx:
            response = ctrl.generate_inventory_report()
            assert response.status_code == HTTPStatus.CONFLICT
            assert response.response["message"].__contains__("admin@email.com")
            assert response.response["message"].__contains__("already")
