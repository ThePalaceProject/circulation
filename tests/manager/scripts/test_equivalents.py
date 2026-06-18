from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.util.log import LogLevel

from palace.manager.scripts.equivalents import EquivalentIdentifiersRefreshScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestEquivalentIdentifiersRefreshScript:
    def test_delta_run_by_default(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.info)
        with patch(
            "palace.manager.scripts.equivalents.equivalent_identifiers_refresh"
        ) as task_mock:
            EquivalentIdentifiersRefreshScript(db.session).do_run()
            task_mock.delay.assert_called_once_with(full_refresh=False)
            assert "delta" in caplog.text

    def test_full_refresh_flag(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.info)
        with patch(
            "palace.manager.scripts.equivalents.equivalent_identifiers_refresh"
        ) as task_mock:
            EquivalentIdentifiersRefreshScript(db.session).do_run(
                cmd_args=["--full-refresh"]
            )
            task_mock.delay.assert_called_once_with(full_refresh=True)
            assert "full refresh" in caplog.text
