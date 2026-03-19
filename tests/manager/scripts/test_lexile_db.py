"""Tests for the Lexile DB update script."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from palace.manager.scripts.lexile_db import LexileDBUpdateScript
from palace.manager.service.logging.configuration import LogLevel
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestLexileDBUpdateScript:
    """Tests for LexileDBUpdateScript."""

    def test_script_name(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ) -> None:
        """Script has correct name."""
        script = LexileDBUpdateScript(
            _db=db.session,
            services=services_fixture.services,
        )
        assert script.script_name == "Lexile DB Update"

    def test_arg_parser(
        self,
        db: DatabaseTransactionFixture,
    ) -> None:
        """arg_parser returns parser with --force and description."""
        parser = LexileDBUpdateScript.arg_parser(db.session)
        args = parser.parse_args([])
        assert args.force is False

        args = parser.parse_args(["--force"])
        assert args.force is True

        assert "Lexile" in parser.description
        assert "augment" in parser.description.lower()

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

    def test_do_run_uses_constructor_force_when_parsed_missing_force(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """do_run uses self._force when parsed namespace has no force attribute."""
        caplog.set_level(LogLevel.info)
        with (
            patch(
                "palace.manager.scripts.lexile_db.lexile_db_update_task"
            ) as mock_task,
            patch.object(
                LexileDBUpdateScript,
                "parse_command_line",
                return_value=SimpleNamespace(),
            ),
        ):
            script = LexileDBUpdateScript(
                _db=db.session,
                services=services_fixture.services,
                force=True,
            )
            script.do_run([])
            mock_task.delay.assert_called_once_with(force=True)
