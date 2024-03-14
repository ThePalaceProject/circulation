import json
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
        library = report_fixture.ctrl.db.default_library()
        system_admin, _ = create(db.session, Admin, email="admin@email.com")
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        with report_fixture.request_context_with_library_and_admin(
            f"/",
            admin=system_admin,
        ) as ctx:
            response = ctrl.generate_inventory_report()
            assert response.status_code == HTTPStatus.ACCEPTED
            body = json.loads(response.data)  # type: ignore
            assert body and body["message"].__contains__("admin@email.com")
            assert not body.__contains__("already")

        # check that when generating a duplicate request a 409 is returned.
        with report_fixture.request_context_with_library_and_admin(
            f"/",
            admin=system_admin,
        ) as ctx:
            response = ctrl.generate_inventory_report()
            body = json.loads(response.data)  # type: ignore
            assert response.status_code == HTTPStatus.CONFLICT
            assert body and body["message"].__contains__("admin@email.com")
            assert body["message"].__contains__("already")
