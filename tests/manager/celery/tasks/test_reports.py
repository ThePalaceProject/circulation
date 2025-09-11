import unittest
from unittest.mock import MagicMock, patch

import pytest

from palace.manager.celery.tasks.reports import (
    generate_report,
)
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestGenerateReportTask:

    def test_generate_report(
        self, celery_fixture: CeleryFixture, db: DatabaseTransactionFixture
    ):
        test_key = "test-report-key"
        test_request_id = "test-request-id"
        test_library_id = db.default_library().id
        test_email_address = "test@example.com"

        report_class = MagicMock()
        report_instance = MagicMock()
        report_class.from_task = MagicMock(return_value=report_instance)
        kwargs = {
            "request_id": test_request_id,
            "library_id": test_library_id,
            "email_address": test_email_address,
        }
        with patch(
            "palace.manager.celery.tasks.reports.REPORT_KEY_MAPPING",
            {test_key: report_class},
        ):
            generate_report(key=test_key, **kwargs).delay().wait()

        report_class.from_task.assert_called_once_with(unittest.mock.ANY, **kwargs)
        report_instance.run.assert_called_once_with(session=db.session)

    def test_generate_report_exception(
        self, celery_fixture: CeleryFixture, db: DatabaseTransactionFixture
    ):
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
            generate_report(key=test_key, **kwargs).delay().wait()

        report_class.from_task.assert_called_once_with(unittest.mock.ANY, **kwargs)
        report_instance.run.assert_called_once_with(session=db.session)

    def test_generate_report_key_not_found(self, celery_fixture: CeleryFixture):
        kwargs = {
            "request_id": "test_request_id",
            "library_id": 1,
            "email_address": "test@example.com",
        }
        invalid_key = "invalid_key"

        with pytest.raises(KeyError, match=invalid_key):
            generate_report(key=invalid_key, **kwargs).delay().wait()
