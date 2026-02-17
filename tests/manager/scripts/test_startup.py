"""Tests for the one-time startup task system."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from celery.canvas import Signature
from sqlalchemy import select
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

from palace.manager.scripts.startup import (
    _slugify,
    create_startup_task,
    discover_startup_tasks,
    run_startup_tasks,
)
from palace.manager.service.container import Services
from palace.manager.sqlalchemy.model.startup_task import StartupTask, StartupTaskState
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestDiscoverStartupTasks:
    def test_discover_startup_tasks(self, tmp_path: Path) -> None:
        """Task modules are discovered and returned as a dict sorted by key."""
        (tmp_path / "b_second.py").write_text("def run(services, session, log): pass\n")
        (tmp_path / "a_first.py").write_text("def run(services, session, log): pass\n")

        result = discover_startup_tasks(tmp_path)

        assert list(result.keys()) == ["a_first", "b_second"]
        assert callable(result["a_first"])
        assert callable(result["b_second"])

    def test_discover_skips_invalid_modules(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Modules without a 'run' callable are skipped with a warning."""
        (tmp_path / "no_task.py").write_text("x = 1\n")

        caplog.set_level(logging.WARNING)
        result = discover_startup_tasks(tmp_path)

        assert len(result) == 0
        assert "does not define 'run'" in caplog.text

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
        (tmp_path / "_create.py").write_text("def run(services, session, log): pass\n")
        (tmp_path / "_helper.py").write_text("def run(services, session, log): pass\n")

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
    @staticmethod
    @contextmanager
    def _engine(db: DatabaseTransactionFixture) -> Iterator[Engine]:
        """Yield a mock engine and patch ``Session`` in the startup module.

        Replaces ``Session`` so that every ``Session(engine)`` call inside
        the startup functions creates a session bound to the test fixture's
        connection, preserving rollback-based test isolation.
        """
        connection = db.session.connection()
        assert isinstance(connection, Connection)

        engine = MagicMock(spec=Engine)
        with patch(
            "palace.manager.scripts.startup.Session",
            lambda *_args, **_kwargs: Session(bind=connection),
        ):
            yield engine

    def test_run_no_tasks_discovered(self, caplog: pytest.LogCaptureFixture) -> None:
        """When no tasks are discovered, nothing is executed."""
        engine = create_autospec(Engine)
        services = create_autospec(Services)
        with patch(
            "palace.manager.scripts.startup.discover_startup_tasks",
            return_value={},
        ):
            caplog.set_level(logging.INFO)
            run_startup_tasks(engine, services, already_initialized=True)

        assert "No startup tasks discovered" in caplog.text

    def test_run_executes_new_task(self, db: DatabaseTransactionFixture) -> None:
        """A new task is executed and recorded in the database."""
        mock_task = MagicMock(return_value=None)
        services = create_autospec(Services)

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"test_task": mock_task},
            ),
        ):
            run_startup_tasks(engine, services, already_initialized=True)

        mock_task.assert_called_once()
        # Verify the task received the services, a Session, and a logger
        call_args = mock_task.call_args
        assert call_args[0][0] is services
        assert isinstance(call_args[0][1], Session)
        assert isinstance(call_args[0][2], logging.Logger)

        row = db.session.execute(
            select(StartupTask).where(StartupTask.key == "test_task")
        ).scalar_one()
        assert row.recorded_at is not None
        assert row.state == StartupTaskState.RUN

    def test_run_dispatches_celery_signature(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When run() returns a Signature, it is dispatched via apply_async."""
        mock_signature = create_autospec(Signature, instance=True)
        mock_signature.apply_async.return_value.id = "fake-task-id"

        def task_returning_signature(
            svc: Services, sess: Session, log: logging.Logger
        ) -> Signature:
            return mock_signature

        services = create_autospec(Services)

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"celery_task": task_returning_signature},
            ),
        ):
            caplog.set_level(logging.INFO)
            run_startup_tasks(engine, services, already_initialized=True)

        mock_signature.apply_async.assert_called_once()
        assert "fake-task-id" in caplog.text
        assert "dispatched Celery task" in caplog.text

    def test_run_retries_task_when_celery_dispatch_fails(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A dispatch failure does not record the task, so it runs again later."""
        mock_signature = create_autospec(Signature, instance=True)
        mock_signature.apply_async.side_effect = RuntimeError("broker unavailable")
        mock_task = MagicMock(return_value=mock_signature)
        services = create_autospec(Services)

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"celery_task": mock_task},
            ),
        ):
            caplog.set_level(logging.INFO)
            run_startup_tasks(engine, services, already_initialized=True)

            # Simulate broker recovery and rerun startup tasks.
            mock_signature.apply_async.side_effect = None
            mock_signature.apply_async.return_value.id = "recovered-task-id"
            run_startup_tasks(engine, services, already_initialized=True)

        row = db.session.execute(
            select(StartupTask).where(StartupTask.key == "celery_task")
        ).scalar_one()
        assert row.state == StartupTaskState.RUN
        assert mock_task.call_count == 2
        assert "Failed to execute startup task" in caplog.text
        assert "recovered-task-id" in caplog.text

    def test_run_skips_already_executed_task(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Tasks already recorded in the database are not re-executed."""
        existing = StartupTask(
            key="already_done",
            recorded_at=utc_now(),
            state=StartupTaskState.RUN,
        )
        db.session.add(existing)
        db.session.flush()

        mock_task = MagicMock()
        services = create_autospec(Services)

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"already_done": mock_task},
            ),
        ):
            run_startup_tasks(engine, services, already_initialized=True)

        mock_task.assert_not_called()

    def test_run_handles_failure_gracefully(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If one task fails, others still proceed."""
        services = create_autospec(Services)

        def bad_task(svc: Services, sess: Session, log: logging.Logger) -> None:
            raise RuntimeError("Something broke")

        good_task = MagicMock(return_value=None)

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={
                    "a_failing": bad_task,
                    "b_succeeding": good_task,
                },
            ),
        ):
            caplog.set_level(logging.ERROR)
            run_startup_tasks(engine, services, already_initialized=True)

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
        good_task.assert_called_once()

        assert "Failed to execute startup task" in caplog.text

    def test_run_idempotent_on_second_call(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """Running the same tasks twice only executes them once."""
        call_count = 0
        services = create_autospec(Services)

        def counting_task(svc: Services, sess: Session, log: logging.Logger) -> None:
            nonlocal call_count
            call_count += 1

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"idempotent_task": counting_task},
            ),
        ):
            run_startup_tasks(engine, services, already_initialized=True)
            run_startup_tasks(engine, services, already_initialized=True)

        assert call_count == 1

    def test_run_task_exception(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """If a task's run function raises, the task is skipped."""
        services = create_autospec(Services)

        def bad_task(svc: Services, sess: Session, log: logging.Logger) -> None:
            raise ValueError("Cannot run task")

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"bad_task": bad_task},
            ),
        ):
            caplog.set_level(logging.ERROR)
            run_startup_tasks(engine, services, already_initialized=True)

        assert "Failed to execute startup task" in caplog.text
        assert (
            db.session.execute(
                select(StartupTask).where(StartupTask.key == "bad_task")
            ).scalar_one_or_none()
            is None
        )

    def test_stamp_records_without_executing(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """already_initialized=False records tasks without calling run."""
        mock_task = MagicMock()
        services = create_autospec(Services)

        with (
            self._engine(db) as engine,
            patch(
                "palace.manager.scripts.startup.discover_startup_tasks",
                return_value={"stamp_me": mock_task},
            ),
        ):
            caplog.set_level(logging.INFO)
            run_startup_tasks(engine, services, already_initialized=False)

        # run should never be called
        mock_task.assert_not_called()

        # But the row should still be recorded
        row = db.session.execute(
            select(StartupTask).where(StartupTask.key == "stamp_me")
        ).scalar_one()
        assert row.recorded_at is not None
        assert row.state == StartupTaskState.MARKED

        assert "Stamped startup task" in caplog.text
        assert "Fresh database install" in caplog.text


class TestCreateStartupTask:
    def test_slugify(self) -> None:
        assert _slugify("Force Harvest OPDS") == "force_harvest_opds"
        assert _slugify("  hello--world!!  ") == "hello_world"
        assert _slugify("simple") == "simple"
        assert _slugify("!!!") == ""

    def test_slugify_truncates_long_descriptions(self) -> None:
        """Long descriptions are truncated at a word boundary."""
        long_desc = "word " * 30  # 150 chars
        slug = _slugify(long_desc)
        assert len(slug) <= 60
        assert not slug.endswith("_")

    def test_creates_file(self, tmp_path: Path) -> None:
        """The create command generates a valid task file."""
        with patch("palace.manager.scripts.startup.STARTUP_TASKS_DIR", tmp_path):
            with patch(
                "sys.argv",
                [
                    "palace-startup-task",
                    "reindex everything",
                    "--date-prefix",
                    "2026_03_15",
                ],
            ):
                create_startup_task()

        filepath = tmp_path / "2026_03_15_reindex_everything.py"
        assert filepath.exists()
        content = filepath.read_text()
        assert "reindex everything" in content
        assert "def run" in content

    def test_refuses_duplicate(self, tmp_path: Path) -> None:
        """The create command refuses to overwrite an existing file."""
        existing = tmp_path / "2026_03_15_duplicate.py"
        existing.write_text("# existing")

        with (
            patch("palace.manager.scripts.startup.STARTUP_TASKS_DIR", tmp_path),
            patch(
                "sys.argv",
                [
                    "palace-startup-task",
                    "duplicate",
                    "--date-prefix",
                    "2026_03_15",
                ],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            create_startup_task()

        assert exc_info.value.code == 1

    def test_refuses_empty_slug(self) -> None:
        """The create command refuses a description that produces an empty slug."""
        with (
            patch(
                "sys.argv",
                ["palace-startup-task", "!!!"],
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            create_startup_task()

        assert exc_info.value.code == 1
