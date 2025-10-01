import logging
from functools import partial
from http import HTTPStatus
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from flask import Response

from palace.manager.api.admin.controller import ReportController
from palace.manager.api.admin.model.inventory_report import (
    InventoryReportCollectionInfo,
    InventoryReportInfo,
)
from palace.manager.api.admin.problem_details import (
    ADMIN_NOT_AUTHORIZED,
    INVALID_REPORT_KEY,
)
from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.problem_details import LIBRARY_NOT_FOUND
from palace.manager.celery.tasks.reports import REPORT_KEY_MAPPING
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.admin import Admin, AdminRole
from palace.manager.sqlalchemy.util import create
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture
from tests.fixtures.services import ServicesFixture


class ReportControllerFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        self.db = db
        self.controller = ReportController(
            db.session,
            services_fixture.services.integration_registry.license_providers(),
        )


@pytest.fixture
def report_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> ReportControllerFixture:
    return ReportControllerFixture(db, services_fixture)


class TestReportController:
    @patch(
        "palace.manager.api.admin.controller.report.generate_inventory_and_hold_reports"
    )
    def test_generate_inventory_and_hold_reports(
        self,
        mock_generate_reports: MagicMock,
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        email_address = "admin@email.com"
        ctrl = report_fixture.controller
        library = db.default_library()
        library_id = library.id
        system_admin, _ = create(db.session, Admin, email=email_address)
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)

        with flask_app_fixture.test_request_context(
            "/", admin=system_admin, library=library
        ):
            response = ctrl.generate_inventory_report()
            assert response.status_code == HTTPStatus.ACCEPTED
            assert isinstance(response, Response)
            assert response.json and email_address in response.json["message"]

        mock_generate_reports.delay.assert_called_once_with(
            email_address=email_address, library_id=library_id
        )

    @patch("palace.manager.api.admin.controller.report.generate_report")
    @patch(
        "palace.manager.api.admin.controller.report.generate_inventory_and_hold_reports"
    )
    def test_generate_report_authorization(
        self,
        mock_generate_inventory_reports: MagicMock,
        mock_generate_report: MagicMock,
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(
            logging.INFO,
            "palace.manager.api.admin.controller.report",
        )

        task_id = 7
        mock_generate_inventory_reports.delay.return_value = SimpleNamespace(id=task_id)
        mock_generate_report.delay.return_value = SimpleNamespace(id=task_id)
        gen_report_success_message = "Upon completion, a notification will be sent to "
        gen_inv_report_success_message = "The completed reports will be sent to "
        gen_report_log_suffix = f"task ID: {task_id}"
        gen_inv_report_log_suffix = f"Task Request Id: {task_id}"

        controller = report_fixture.controller
        generate_inventory_report = controller.generate_inventory_report

        # We'll just use the first key from the report map.
        valid_report_key = next(iter(REPORT_KEY_MAPPING))
        generate_report = partial(
            controller.generate_report, report_key=valid_report_key
        )

        library1 = db.library()
        library2 = db.library()

        sysadmin_email = "sysadmin@example.org"
        librarian_email = "librarian@example.org"

        sysadmin = flask_app_fixture.admin_user(
            email=sysadmin_email, role=AdminRole.SYSTEM_ADMIN
        )
        librarian1 = flask_app_fixture.admin_user(
            email=librarian_email, role=AdminRole.LIBRARIAN, library=library1
        )

        collection = db.collection(
            protocol=OPDSAPI,
        )
        collection.associated_libraries = [library1, library2]

        def assert_and_clear_caplog(
            response: Response | ProblemDetail,
            email: str,
            success_message: str,
            log_suffix: str,
        ) -> None:
            assert isinstance(response, Response)
            assert response.status_code == 202
            assert success_message in response.json.get("message")
            assert email in response.json.get("message")
            assert log_suffix in caplog.text
            caplog.clear()

        # Sysadmin can get info for any library.
        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library1
        ):
            assert_and_clear_caplog(
                generate_inventory_report(),
                sysadmin_email,
                gen_inv_report_success_message,
                gen_inv_report_log_suffix,
            )
            assert_and_clear_caplog(
                generate_report(),
                sysadmin_email,
                gen_report_success_message,
                gen_report_log_suffix,
            )

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library2
        ):
            assert_and_clear_caplog(
                generate_inventory_report(),
                sysadmin_email,
                gen_inv_report_success_message,
                gen_inv_report_log_suffix,
            )
            assert_and_clear_caplog(
                generate_report(),
                sysadmin_email,
                gen_report_success_message,
                gen_report_log_suffix,
            )

        # The librarian for library 1 can get info only for that library...
        with flask_app_fixture.test_request_context(
            "/", admin=librarian1, library=library1
        ):
            assert_and_clear_caplog(
                generate_inventory_report(),
                librarian_email,
                gen_inv_report_success_message,
                gen_inv_report_log_suffix,
            )
            assert_and_clear_caplog(
                generate_report(),
                librarian_email,
                gen_report_success_message,
                gen_report_log_suffix,
            )

        # ... since it does not have an admin role for library2.
        with flask_app_fixture.test_request_context(
            "/", admin=librarian1, library=library2
        ):
            with pytest.raises(ProblemDetailException) as exc1:
                generate_inventory_report()
            with pytest.raises(ProblemDetailException) as exc2:
                generate_report()
        assert exc1.value.problem_detail == ADMIN_NOT_AUTHORIZED
        assert exc2.value.problem_detail == ADMIN_NOT_AUTHORIZED

        # A library must be provided.
        with flask_app_fixture.test_request_context("/", admin=sysadmin, library=None):
            with pytest.raises(ProblemDetailException) as exc1:
                generate_inventory_report()
            with pytest.raises(ProblemDetailException) as exc2:
                generate_report()
        assert exc1.value.problem_detail == LIBRARY_NOT_FOUND
        assert exc2.value.problem_detail == LIBRARY_NOT_FOUND

    def test_inventory_report_info(
        self,
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = report_fixture.controller

        library1 = db.library()
        library2 = db.library()

        sysadmin = flask_app_fixture.admin_user(
            email="sysadmin@example.org", role=AdminRole.SYSTEM_ADMIN
        )
        librarian1 = flask_app_fixture.admin_user(
            email="librarian@example.org", role=AdminRole.LIBRARIAN, library=library1
        )

        collection = db.collection(
            protocol=OPDSAPI,
        )
        collection.associated_libraries = [library1, library2]

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
        assert admin_response1.json == success_payload_dict

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library2
        ):
            admin_response2 = controller.inventory_report_info()
        assert admin_response2.status_code == 200
        assert admin_response2.json == success_payload_dict

        # The librarian for library 1 can get info only for that library...
        with flask_app_fixture.test_request_context(
            "/", admin=librarian1, library=library1
        ):
            librarian1_response1 = controller.inventory_report_info()
        assert librarian1_response1.status_code == 200
        assert librarian1_response1.json == success_payload_dict
        # ... since it does not have an admin role for library2.
        with flask_app_fixture.test_request_context(
            "/", admin=librarian1, library=library2
        ):
            with pytest.raises(ProblemDetailException) as exc:
                controller.inventory_report_info()
        assert exc.value.problem_detail == ADMIN_NOT_AUTHORIZED

        # A library must be provided.
        with flask_app_fixture.test_request_context("/", admin=sysadmin, library=None):
            with pytest.raises(ProblemDetailException) as exc:
                controller.inventory_report_info()
        assert exc.value.problem_detail == LIBRARY_NOT_FOUND

    @pytest.mark.parametrize(
        "protocol, settings, parent_settings, expect_collection",
        (
            (
                OPDSAPI,
                {"data_source": "test", "external_account_id": "http://url"},
                None,
                True,
            ),
            (
                OPDSAPI,
                {
                    "include_in_inventory_report": False,
                    "data_source": "test",
                    "external_account_id": "http://test.url",
                },
                None,
                False,
            ),
            (
                OPDSAPI,
                {
                    "include_in_inventory_report": True,
                    "data_source": "test",
                    "external_account_id": "http://test.url",
                },
                None,
                True,
            ),
            (
                OverdriveAPI,
                {
                    "overdrive_website_id": "test",
                    "overdrive_client_key": "test",
                    "overdrive_client_secret": "test",
                },
                None,
                False,
            ),
            (
                OverdriveAPI,
                {"external_account_id": "test"},
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
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        protocol: type[BaseCirculationAPI[Any, Any]],
        settings: dict,
        parent_settings: dict,
        expect_collection: bool,
    ):
        controller = report_fixture.controller
        sysadmin = flask_app_fixture.admin_user(role=AdminRole.SYSTEM_ADMIN)

        library = db.library()
        collection = db.collection(protocol=protocol, settings=settings)
        collection.associated_libraries = [library]

        if parent_settings:
            parent = db.collection(
                protocol=protocol, settings=parent_settings, library=library
            )
            collection.parent = parent

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
        assert response.json == success_payload_dict

    @patch("palace.manager.api.admin.controller.report.generate_report")
    @patch("palace.manager.api.admin.controller.report.uuid_encode")
    def test_generate_report_celery_task(
        self,
        mock_uuid_encode: MagicMock,
        mock_celery_task: MagicMock,
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        fake_encoded_uuid = "fake-uuid"
        mock_uuid_encode.return_value = fake_encoded_uuid

        email_address = "admin@email.com"
        ctrl = report_fixture.controller
        library = db.default_library()
        library_id = library.id
        system_admin, _ = create(db.session, Admin, email=email_address)
        system_admin.add_role(AdminRole.SYSTEM_ADMIN)
        test_key = next(iter(REPORT_KEY_MAPPING))

        with flask_app_fixture.test_request_context(
            "/", admin=system_admin, library=library
        ):
            response = ctrl.generate_report(report_key=test_key)
            assert response.status_code == HTTPStatus.ACCEPTED
            assert isinstance(response, Response)
            assert response.json and email_address in response.json["message"]

        mock_celery_task.delay.assert_called_once_with(
            key=test_key,
            request_id=fake_encoded_uuid,
            email_address=email_address,
            library_id=library_id,
        )

    @patch("palace.manager.api.admin.controller.report.generate_report")
    @patch("palace.manager.api.admin.controller.report.uuid_encode")
    def test_generate_report(
        self,
        mock_uuid_encode: MagicMock,
        mock_celery_task: MagicMock,
        report_fixture: ReportControllerFixture,
        flask_app_fixture: FlaskAppFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        db = report_fixture.db
        caplog.set_level(
            logging.INFO,
            "palace.manager.api.admin.controller.report",
        )

        fake_encoded_uuid = "fake-uuid"
        mock_uuid_encode.return_value = fake_encoded_uuid

        task_id = 123
        mock_celery_task.delay.return_value = SimpleNamespace(id=task_id)
        log_message_suffix = f"task ID: {task_id})"

        controller = report_fixture.controller
        library = db.default_library()
        sysadmin = flask_app_fixture.admin_user(
            email="sysadmin@example.org", role=AdminRole.SYSTEM_ADMIN
        )

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library
        ):
            for report_key, report_info in REPORT_KEY_MAPPING.items():
                response = controller.generate_report(report_key=report_key)
                assert response.status_code == HTTPStatus.ACCEPTED
                assert isinstance(response, Response)
                assert (
                    "Upon completion, a notification will be sent to "
                    in response.json.get("message")
                )
                assert sysadmin.email in response.json.get("message")
                assert log_message_suffix in caplog.text
                assert report_info.TITLE in caplog.text
                caplog.clear()
                mock_celery_task.delay.assert_called_with(
                    key=report_key,
                    request_id=fake_encoded_uuid,
                    library_id=library.id,
                    email_address=sysadmin.email,
                )
                mock_celery_task.delay.reset_mock()

    def test_generate_report_invalid_key(
        self,
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ):
        controller = report_fixture.controller
        library = db.default_library()
        sysadmin = flask_app_fixture.admin_user(
            email="sysadmin@example.org", role=AdminRole.SYSTEM_ADMIN
        )
        invalid_key = "invalid_key"

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library
        ):
            with pytest.raises(ProblemDetailException) as exc:
                controller.generate_report(report_key=invalid_key)
            assert exc.value.problem_detail.title == INVALID_REPORT_KEY.title
            assert exc.value.problem_detail == INVALID_REPORT_KEY.detailed(
                f"No currently defined report is associated with the specified key (key='{invalid_key}')."
            )

    @patch("palace.manager.api.admin.controller.report.generate_report")
    def test_generate_report_exception(
        self,
        mock_generate_report: MagicMock,
        report_fixture: ReportControllerFixture,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(
            logging.INFO,
            "palace.manager.api.admin.controller.report",
        )
        mock_generate_report.delay.side_effect = Exception("Test Exception")
        controller = report_fixture.controller
        library = db.default_library()
        sysadmin = flask_app_fixture.admin_user(
            email="sysadmin@example.org", role=AdminRole.SYSTEM_ADMIN
        )
        valid_key = next(iter(REPORT_KEY_MAPPING))

        with flask_app_fixture.test_request_context(
            "/", admin=sysadmin, library=library
        ):
            with pytest.raises(ProblemDetailException) as exc:
                controller.generate_report(report_key=valid_key)
            assert exc.value.problem_detail.status_code == 500
            assert exc.value.problem_detail.detail is not None
            assert "Failed to generate report" in exc.value.problem_detail.detail
            assert "Test Exception" in caplog.text
