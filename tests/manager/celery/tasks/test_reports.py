import logging
import unittest
from unittest.mock import MagicMock, patch

import pytest

from palace.manager.celery.tasks.reports import (
    generate_report,
)
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestGenerateReportTask:

    @pytest.mark.parametrize(
        "expected_success",
        (
            pytest.param(True, id="success"),
            pytest.param(False, id="failure"),
        ),
    )
    def test_generate_report_sets_up_and_runs_report(
        self,
        celery_fixture: CeleryFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
        expected_success: bool,
    ):
        caplog.set_level(logging.ERROR)

        test_key = "test_key"
        test_title = "Test Report"

        test_request_id = "test-request-id"
        test_library_id = db.default_library().id
        test_email = "test@example.com"
        kwargs = {
            "request_id": test_request_id,
            "library_id": test_library_id,
            "email_address": test_email,
        }

        mock_report = MagicMock(
            key=test_key,
            title=test_title,
            email_address=test_email,
            request_id=test_request_id,
        )
        mock_report.run.return_value = expected_success
        mock_report_class = MagicMock(return_value=mock_report)
        mock_report_class.from_task.return_value = mock_report

        with patch(
            "palace.manager.celery.tasks.reports.REPORT_KEY_MAPPING",
            {test_key: mock_report_class},
        ):
            success = generate_report.delay(key=test_key, **kwargs).wait()

        assert success == expected_success
        mock_report_class.from_task.assert_called_once_with(unittest.mock.ANY, **kwargs)
        mock_report.run.assert_called_once_with(session=db.session)

        if expected_success:
            assert "Report task failed:" not in caplog.text
        else:
            assert (
                f"Report task failed: '{test_title}' ({test_key}) for <{test_email}>. "
                f"(request ID: {test_request_id})"
            ) in caplog.text

    def test_generate_report_from_task_exception(
        self, celery_fixture: CeleryFixture, db: DatabaseTransactionFixture
    ):
        """If `from_task` throws an exception, that exception is passed on."""
        test_key = "test-report-key"

        report_class = MagicMock()
        report_class.from_task = MagicMock(side_effect=Exception("Test Exception"))

        kwargs = {
            "request_id": "test_request_id",
            "library_id": 1,
            "email_address": "test@example.com",
        }

        with (
            patch(
                "palace.manager.celery.tasks.reports.REPORT_KEY_MAPPING",
                {test_key: report_class},
            ),
            pytest.raises(Exception, match="Test Exception"),
        ):
            generate_report.delay(key=test_key, **kwargs).wait()

        report_class.from_task.assert_called_once_with(unittest.mock.ANY, **kwargs)

    def test_generate_report_run_exception(
        self, celery_fixture: CeleryFixture, db: DatabaseTransactionFixture
    ):
        """If the report run throws an exception, that exception is passed on."""
        test_key = "test-report-key"

        report_class = MagicMock()
        report_instance = MagicMock()
        report_class.from_task = MagicMock(return_value=report_instance)
        report_instance.run.side_effect = Exception("Test Exception")

        kwargs = {
            "request_id": "test_request_id",
            "library_id": 1,
            "email_address": "test@example.com",
        }

        with (
            patch(
                "palace.manager.celery.tasks.reports.REPORT_KEY_MAPPING",
                {test_key: report_class},
            ),
            pytest.raises(Exception, match="Test Exception"),
        ):
            generate_report.delay(key=test_key, **kwargs).wait()

        report_class.from_task.assert_called_once_with(unittest.mock.ANY, **kwargs)
        report_instance.run.assert_called_once_with(session=db.session)

    def test_generate_report_key_not_found(self, celery_fixture: CeleryFixture):
        kwargs = {
            "request_id": "test_request_id",
            "library_id": 1,
            "email_address": "test@example.com",
        }
        invalid_key = "😱 invalid_key 😱"

        with pytest.raises(KeyError, match=invalid_key):
            generate_report.delay(key=invalid_key, **kwargs).wait()
