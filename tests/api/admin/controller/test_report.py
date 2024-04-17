import json
from http import HTTPStatus

import pytest

from core.model import create
from core.model.admin import Admin, AdminRole
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.celery import CeleryFixture


class ReportControllerFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)


@pytest.fixture
def report_fixture(
    controller_fixture: ControllerFixture,
) -> ReportControllerFixture:
    return ReportControllerFixture(controller_fixture)


class TestReportController:
    def test_generate_inventory_and_hold_reports(
        self, report_fixture: ReportControllerFixture, celery_fixture: CeleryFixture
    ):
        ctrl = report_fixture.manager.admin_report_controller
        db = report_fixture.ctrl.db
        report_fixture.ctrl.library = report_fixture.ctrl.db.default_library()
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
