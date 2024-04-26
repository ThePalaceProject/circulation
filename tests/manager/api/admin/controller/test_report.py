from http import HTTPStatus
from unittest.mock import patch

import pytest
from flask import Response

from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.util import create
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
    def test_generate_inventory_and_hold_reports(
        self, report_fixture: ReportControllerFixture
    ):
        email_address = "admin@email.com"
        ctrl = report_fixture.manager.admin_report_controller
        db = report_fixture.ctrl.db
        library = report_fixture.ctrl.db.default_library()
        report_fixture.ctrl.library = library
        library_id = library.id
        system_admin, _ = create(db.session, Admin, email=email_address)
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with (
            report_fixture.request_context_with_library_and_admin(
                f"/",
                admin=system_admin,
            ),
            patch(
                "palace.manager.api.admin.controller.report.generate_inventory_and_hold_reports"
            ) as mock_generate_reports,
        ):
            response = ctrl.generate_inventory_report()
            assert response.status_code == HTTPStatus.ACCEPTED
            assert isinstance(response, Response)
            assert response.json and email_address in response.json["message"]

        mock_generate_reports.delay.assert_called_once_with(
            email_address=email_address, library_id=library_id
        )
