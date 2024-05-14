from http import HTTPStatus
from unittest.mock import patch

import pytest
from flask import Response

from palace.manager.api.admin.controller import ReportController
from palace.manager.api.admin.model.inventory_report import (
    InventoryReportCollectionInfo,
    InventoryReportInfo,
)
from palace.manager.api.admin.problem_details import ADMIN_NOT_AUTHORIZED
from palace.manager.api.overdrive import OverdriveAPI
from palace.manager.core.opds_import import OPDSAPI
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.util import create
from palace.manager.util.problem_detail import ProblemDetailException
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


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

    def test_inventory_report_info(
        self, db: DatabaseTransactionFixture, flask_app_fixture: FlaskAppFixture
    ):
        controller = ReportController(db.session)

        library1 = db.library()
        library2 = db.library()

        sysadmin = flask_app_fixture.admin_user(
            email="sysadmin@example.org", role=AdminRole.SYSTEM_ADMIN
        )
        librarian1 = flask_app_fixture.admin_user(
            email="librarian@example.org", role=AdminRole.LIBRARIAN, library=library1
        )

        collection = db.collection(
            protocol=OPDSAPI.label(),
            settings={"data_source": "test", "external_account_id": "http://url"},
        )
        collection.libraries = [library1, library2]

        success_payload_dict = InventoryReportInfo(
            collections=[
                InventoryReportCollectionInfo(
                    id=collection.id, name=collection.integration_configuration.name
                )
            ]
        ).api_dict()

        # Sysadmin can get info for any library.
        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library1
        ):
            admin_response1 = controller.inventory_report_info()
        assert admin_response1.status_code == 200
        assert admin_response1.get_json() == success_payload_dict

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library2
        ):
            admin_response2 = controller.inventory_report_info()
        assert admin_response2.status_code == 200
        assert admin_response2.get_json() == success_payload_dict

        # The librarian for library 1 can get info only for that library...
        with flask_app_fixture.test_request_context(
            "/", admin=librarian1, library=library1
        ):
            librarian1_response1 = controller.inventory_report_info()
        assert librarian1_response1.status_code == 200
        assert librarian1_response1.get_json() == success_payload_dict
        # ... since it does not have an admin role for library2.
        with flask_app_fixture.test_request_context(
            "/", admin=librarian1, library=library2
        ):
            with pytest.raises(ProblemDetailException) as exc:
                controller.inventory_report_info()
        assert exc.value.problem_detail == ADMIN_NOT_AUTHORIZED

        # A library must be provided.
        with flask_app_fixture.test_request_context("/", admin=sysadmin, library=None):
            admin_response_none = controller.inventory_report_info()
        assert admin_response_none.status_code == 404

    @pytest.mark.parametrize(
        "protocol, settings, expect_collection",
        (
            (
                OPDSAPI.label(),
                {"data_source": "test", "external_account_id": "http://url"},
                True,
            ),
            (
                OPDSAPI.label(),
                {
                    "include_in_inventory_report": False,
                    "data_source": "test",
                    "external_account_id": "http://test.url",
                },
                False,
            ),
            (
                OPDSAPI.label(),
                {
                    "include_in_inventory_report": True,
                    "data_source": "test",
                    "external_account_id": "http://test.url",
                },
                True,
            ),
            (
                OverdriveAPI.label(),
                {
                    "overdrive_website_id": "test",
                    "overdrive_client_key": "test",
                    "overdrive_client_secret": "test",
                },
                False,
            ),
        ),
    )
    def test_inventory_report_info_reportable_collections(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        protocol: str,
        settings: dict,
        expect_collection: bool,
    ):
        controller = ReportController(db.session)
        sysadmin = flask_app_fixture.admin_user(role=AdminRole.SYSTEM_ADMIN)

        library = db.library()
        collection = db.collection(protocol=protocol, settings=settings)
        collection.libraries = [library]

        expected_collections = (
            [InventoryReportCollectionInfo(id=collection.id, name=collection.name)]
            if expect_collection
            else []
        )
        expected_collection_count = 1 if expect_collection else 0
        success_payload_dict = InventoryReportInfo(
            collections=expected_collections
        ).api_dict()
        assert len(expected_collections) == expected_collection_count

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library
        ):
            response = controller.inventory_report_info()
        assert response.status_code == 200
        assert response.get_json() == success_payload_dict