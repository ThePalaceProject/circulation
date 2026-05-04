from unittest.mock import patch

import pytest

from palace.util.log import LogLevel

from palace.manager.scripts.license_expiration import UpdateExpiredLicensesScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestUpdateExpiredLicensesScript:
    def test_queues_task(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.info)
        with patch(
            "palace.manager.scripts.license_expiration.update_expired_licenses"
        ) as task_mock:
            UpdateExpiredLicensesScript(db.session).run()
            assert task_mock.delay.call_count == 1
            assert (
                'The "update_expired_licenses" task has been queued for execution.'
                in caplog.text
            )
