"""Tests for the Lexile DB update script."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from palace.manager.scripts.lexile_db import LexileDBUpdateScript
from palace.manager.service.logging.configuration import LogLevel
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestLexileDBUpdateScript:
    """Tests for LexileDBUpdateScript."""

    @pytest.mark.parametrize(
        "force",
        [
            pytest.param(True, id="force"),
            pytest.param(False, id="no force"),
        ],
    )
    def test_do_run(
        self,
        force: bool,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """do_run queues lexile_db_update_task with correct force flag and logs."""
        caplog.set_level(LogLevel.info)
        with patch(
            "palace.manager.scripts.lexile_db.lexile_db_update_task"
        ) as mock_task:
            command_args = ["--force"] if force else []
            LexileDBUpdateScript(
                _db=db.session,
                services=services_fixture.services,
            ).do_run(command_args)
            mock_task.delay.assert_called_once_with(force=force)
            assert "Successfully queued lexile_db_update_task" in caplog.text
            assert f"force={force}" in caplog.text
