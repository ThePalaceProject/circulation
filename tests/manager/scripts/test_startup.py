"""Tests for the one-time startup task system."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from celery.canvas import Signature
from sqlalchemy import select
from sqlalchemy.engine import Engine

from palace.manager.scripts.startup import (
    StartupTaskRunner,
    _slugify,
    create_startup_task,
    discover_startup_tasks,
)
from palace.manager.sqlalchemy.model.startup_task import StartupTask
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestDiscoverStartupTasks:
    def test_discover_startup_tasks(self, tmp_path: Path) -> None:
        """Task modules are discovered and returned as a dict sorted by key."""
        (tmp_path / "b_second.py").write_text("def startup_task_signature(): pass\n")
        (tmp_path / "a_first.py").write_text("def startup_task_signature(): pass\n")

        result = discover_startup_tasks(tmp_path)

        assert list(result.keys()) == ["a_first", "b_second"]
        assert callable(result["a_first"])
        assert callable(result["b_second"])

    def test_discover_skips_invalid_modules(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Modules without a 'startup_task_signature' callable are skipped with a warning."""
        (tmp_path / "no_task.py").write_text("x = 1\n")

        caplog.set_level(logging.WARNING)
        result = discover_startup_tasks(tmp_path)

        assert len(result) == 0
        assert "does not define 'startup_task_signature'" in caplog.text

    def test_discover_skips_import_errors(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Modules that fail to import are skipped with an error log."""
        (tmp_path / "broken.py").write_text("raise RuntimeError('boom')\n")

        caplog.set_level(logging.ERROR)
        result = discover_startup_tasks(tmp_path)

        assert len(result) == 0
        assert "Failed to import startup task module" in caplog.text

    def test_discover_skips_underscore_files(self, tmp_path: Path) -> None:
        """Files starting with _ are skipped."""
        (tmp_path / "__init__.py").write_text("")
        (tmp_path / "_create.py").write_text("def startup_task_signature(): pass\n")
        (tmp_path / "_helper.py").write_text("def startup_task_signature(): pass\n")

        result = discover_startup_tasks(tmp_path)

        assert len(result) == 0

    def test_discover_nonexistent_directory(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A missing directory returns an empty dict."""
        caplog.set_level(logging.INFO)
        result = discover_startup_tasks(tmp_path / "does_not_exist")

        assert result == {}
        assert "does not exist" in caplog.text


class TestStartupTaskRunner:
    def _mock_session(self, db: DatabaseTransactionFixture) -> MagicMock:
        """Create a mock Session class that returns the test fixture's session."""
        mock_session_cls = MagicMock()
        mock_session_cls.return_value.__enter__ = MagicMock(return_value=db.session)
        mock_session_cls.return_value.__exit__ = MagicMock(return_value=False)
        return mock_session_cls

    def test_run_no_tasks_discovered(self, caplog: pytest.LogCaptureFixture) -> None:
        """When no tasks are discovered, nothing is queued."""
        engine = create_autospec(Engine)
        with patch(
            "palace.manager.scripts.startup.discover_startup_tasks",
            return_value={},
        ):
            caplog.set_level(logging.INFO)
            StartupTaskRunner().run(engine)

        assert "No startup tasks discovered" in caplog.text

    def test_run_queues_new_task(self, db: DatabaseTransactionFixture) -> None:
        """A new task is dispatched and recorded in the database."""
        mock_sig = create_autospec(Signature)

        engine = db.session.get_bind()
        assert isinstance(engine, Engine)

        with (
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"test_task": lambda: mock_sig},
            ),
            patch("palace.manager.scripts.startup.Session", self._mock_session(db)),
        ):
            StartupTaskRunner().run(engine)

        mock_sig.apply_async.assert_called_once()

        row = db.session.execute(
            select(StartupTask).where(StartupTask.key == "test_task")
        ).scalar_one()
        assert row.queued_at is not None
        assert row.run is True

    def test_run_skips_already_queued_task(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Tasks already recorded in the database are not re-queued."""
        existing = StartupTask(
            key="already_done",
            queued_at=utc_now(),
            run=True,
        )
        db.session.add(existing)
        db.session.flush()

        mock_create = MagicMock()

        engine = db.session.get_bind()
        assert isinstance(engine, Engine)

        with (
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"already_done": mock_create},
            ),
            patch("palace.manager.scripts.startup.Session", self._mock_session(db)),
        ):
            StartupTaskRunner().run(engine)

        mock_create.assert_not_called()

    def test_run_handles_queue_failure_gracefully(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If one task fails to queue, others still proceed."""
        good_sig = create_autospec(Signature)
        bad_sig = create_autospec(Signature)
        bad_sig.apply_async.side_effect = RuntimeError("Celery is down")

        engine = db.session.get_bind()
        assert isinstance(engine, Engine)

        with (
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={
                    "a_failing": lambda: bad_sig,
                    "b_succeeding": lambda: good_sig,
                },
            ),
            patch("palace.manager.scripts.startup.Session", self._mock_session(db)),
        ):
            caplog.set_level(logging.ERROR)
            StartupTaskRunner().run(engine)

        # The failing task should not be recorded
        assert (
            db.session.execute(
                select(StartupTask).where(StartupTask.key == "a_failing")
            ).scalar_one_or_none()
            is None
        )

        # The succeeding task should be recorded
        row = db.session.execute(
            select(StartupTask).where(StartupTask.key == "b_succeeding")
        ).scalar_one()
        assert row is not None
        good_sig.apply_async.assert_called_once()

        assert "Failed to queue startup task" in caplog.text

    def test_run_idempotent_on_second_call(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Running the same tasks twice only queues them once."""
        mock_sig = create_autospec(Signature)
        call_count = 0

        def make_sig() -> Signature:
            nonlocal call_count
            call_count += 1
            return mock_sig

        engine = db.session.get_bind()
        assert isinstance(engine, Engine)

        with (
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"idempotent_task": make_sig},
            ),
            patch("palace.manager.scripts.startup.Session", self._mock_session(db)),
        ):
            StartupTaskRunner().run(engine)
            StartupTaskRunner().run(engine)

        assert call_count == 1
        mock_sig.apply_async.assert_called_once()

    def test_run_startup_task_signature_exception(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If startup_task_signature itself raises, the task is skipped."""

        def bad_create() -> Signature:
            raise ValueError("Cannot build signature")

        engine = db.session.get_bind()
        assert isinstance(engine, Engine)

        with (
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"bad_signature": bad_create},
            ),
            patch("palace.manager.scripts.startup.Session", self._mock_session(db)),
        ):
            caplog.set_level(logging.ERROR)
            StartupTaskRunner().run(engine)

        assert "Failed to queue startup task" in caplog.text
        assert (
            db.session.execute(
                select(StartupTask).where(StartupTask.key == "bad_signature")
            ).scalar_one_or_none()
            is None
        )

    def test_run_stamp_only_records_without_queuing(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """stamp_only=True records tasks without calling startup_task_signature."""
        mock_create = MagicMock()

        engine = db.session.get_bind()
        assert isinstance(engine, Engine)

        with (
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"stamp_me": mock_create},
            ),
            patch("palace.manager.scripts.startup.Session", self._mock_session(db)),
        ):
            caplog.set_level(logging.INFO)
            StartupTaskRunner().run(engine, stamp_only=True)

        # startup_task_signature should never be called
        mock_create.assert_not_called()

        # But the row should still be recorded
        row = db.session.execute(
            select(StartupTask).where(StartupTask.key == "stamp_me")
        ).scalar_one()
        assert row.queued_at is not None
        assert row.run is False

        assert "Stamped startup task" in caplog.text
        assert "Fresh database install" in caplog.text


class TestCreateStartupTask:
    def test_slugify(self) -> None:
        assert _slugify("Force Harvest OPDS") == "force_harvest_opds"
        assert _slugify("  hello--world!!  ") == "hello_world"
        assert _slugify("simple") == "simple"
        assert _slugify("!!!") == ""

    def test_main_creates_file(self, tmp_path: Path) -> None:
        """The create command generates a valid task file."""
        with patch("palace.manager.scripts.startup.STARTUP_TASKS_DIR", tmp_path):
            with patch(
                "sys.argv",
                [
                    "create_startup_task",
                    "reindex everything",
                    "--date-prefix",
                    "2026_03_15_1430",
                ],
            ):
                create_startup_task()

        filepath = tmp_path / "2026_03_15_1430_reindex_everything.py"
        assert filepath.exists()
        content = filepath.read_text()
        assert "reindex everything" in content
        assert "def startup_task_signature" in content

    def test_main_refuses_duplicate(self, tmp_path: Path) -> None:
        """The create command refuses to overwrite an existing file."""
        existing = tmp_path / "2026_03_15_1430_duplicate.py"
        existing.write_text("# existing")

        with (
            patch("palace.manager.scripts.startup.STARTUP_TASKS_DIR", tmp_path),
            patch(
                "sys.argv",
                [
                    "create_startup_task",
                    "duplicate",
                    "--date-prefix",
                    "2026_03_15_1430",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            create_startup_task()

        assert exc_info.value.code == 1

    def test_main_refuses_empty_slug(self) -> None:
        """The create command refuses a description that produces an empty slug."""
        with (
            patch(
                "sys.argv",
                ["create_startup_task", "!!!"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            create_startup_task()

        assert exc_info.value.code == 1
