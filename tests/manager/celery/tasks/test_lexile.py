"""Tests for the Lexile DB Celery tasks."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from celery.exceptions import Ignore
from pytest import LogCaptureFixture

from palace.manager.celery.tasks import lexile
from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.lexile.service import LexileDBService
from palace.manager.integration.metadata.lexile.settings import LexileDBSettings
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.sqlalchemy.constants import DataSourceConstants
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.util import get_one
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.http import MockHttpClientFixture
from tests.fixtures.redis import RedisFixture


class TestLexileDBUpdate:
    """Tests for the Lexile DB update tasks."""

    def test_run_lexile_db_update_skipped_when_not_configured(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Orchestrator skips when no Lexile DB integration exists."""
        caplog.set_level(LogLevel.info)
        lexile.run_lexile_db_update.delay().wait()
        assert "Lexile DB update skipped" in caplog.text

    def test_run_lexile_db_update_queues_worker_when_configured(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Orchestrator queues worker when integration exists and lock is free."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        caplog.set_level(LogLevel.info)
        with patch.object(
            lexile.lexile_db_update_task,
            "delay",
            wraps=lexile.lexile_db_update_task.delay,
        ) as mock_delay:
            lexile.run_lexile_db_update.delay().wait()
        mock_delay.assert_called_once_with(force=False)
        assert "Lexile DB update task queued" in caplog.text

    def test_run_lexile_db_update_skipped_when_lock_already_held(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Orchestrator skips when lock is already held (worker in progress)."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        caplog.set_level(LogLevel.info)
        with patch.object(RedisLock, "locked", return_value=True):
            lexile.run_lexile_db_update.delay().wait()
        assert "Lexile DB update already in progress, skipping." in caplog.text
        assert "Lexile DB update task queued" not in caplog.text

    def test_lexile_db_update_task_skipped_when_not_configured(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Worker skips when no Lexile DB integration exists."""
        caplog.set_level(LogLevel.info)
        lexile.lexile_db_update_task.delay(force=False).wait()
        assert "Lexile DB update skipped" in caplog.text

    def test_lexile_db_update_task_errors_when_timestamp_id_missing_with_offset(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Worker logs error and returns when offset > 0 but timestamp_id is None."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        caplog.set_level(LogLevel.error)
        lexile.lexile_db_update_task.delay(
            force=False, offset=5, timestamp_id=None
        ).wait()
        assert "timestamp_id required when offset > 0" in caplog.text

    def test_lexile_db_update_task_errors_when_lock_value_missing_with_offset(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Worker logs error and returns when offset > 0 but lock_value is None."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        caplog.set_level(LogLevel.error)
        lexile.lexile_db_update_task.delay(
            force=False, offset=5, timestamp_id=42, lock_value=None
        ).wait()
        assert "lock_value required when offset > 0" in caplog.text

    def test_lexile_db_update_task_skipped_when_lock_already_held_first_batch(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """First batch skips when workflow lock is already held by another run."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        caplog.set_level(LogLevel.info)
        with patch.object(RedisLock, "acquire", return_value=False):
            lexile.lexile_db_update_task.delay(force=False).wait()
        assert "could not acquire lock, skipping" in caplog.text

    def test_lexile_db_update_task_adds_classification(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Worker fetches Lexile from API and adds classification."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        identifier = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9780123456789"
        )

        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [{"lexile": 650}],
            },
        )

        lexile.lexile_db_update_task.delay(force=False).wait()

        db.session.refresh(identifier)
        classifications = [
            c
            for c in identifier.classifications
            if c.subject.type == Subject.LEXILE_SCORE
        ]
        assert len(classifications) == 1
        assert classifications[0].subject.identifier == "650"
        assert classifications[0].data_source.name == DataSourceConstants.LEXILE_DB

    def test_lexile_db_update_task_force_mode_replaces_existing(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Force mode replaces existing Lexile DB classification with new value."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        data_source = DataSource.lookup(
            db.session, DataSourceConstants.LEXILE_DB, autocreate=True
        )
        identifier = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9780123456789"
        )
        identifier.classify(
            data_source,
            Subject.LEXILE_SCORE,
            "500",
            None,
            weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
        )
        db.session.commit()

        http_client.queue_response(
            200,
            content={
                "meta": {"total_count": 1},
                "objects": [{"lexile": 720}],
            },
        )

        lexile.lexile_db_update_task.delay(force=True).wait()

        db.session.refresh(identifier)
        classifications = [
            c
            for c in identifier.classifications
            if c.subject.type == Subject.LEXILE_SCORE
        ]
        assert len(classifications) == 1
        assert classifications[0].subject.identifier == "720"

    def test_lexile_db_update_task_creates_timestamp(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Worker creates Timestamp for run status."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )

        lexile.lexile_db_update_task.delay(force=False).wait()

        stamp = get_one(
            db.session,
            Timestamp,
            service="Lexile DB Update",
            service_type=Timestamp.TASK_TYPE,
            collection=None,
        )
        assert stamp is not None
        assert stamp.finish is not None
        assert "Processed" in (stamp.achievements or "")

    def test_lexile_db_update_task_continues_to_next_batch(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Worker calls task.replace() with correct args when full batch returned."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        # Create exactly BATCH_SIZE (10) ISBNs so the task replaces to continue.
        for i in range(lexile.BATCH_SIZE):
            db.identifier(
                identifier_type=Identifier.ISBN,
                foreign_id=f"9780123456{i:03d}",
            )

        for i in range(lexile.BATCH_SIZE):
            http_client.queue_response(
                200,
                content={
                    "meta": {"total_count": 1},
                    "objects": [{"lexile": 600 + i}],
                },
            )

        replace_calls: list[tuple] = []

        def capture_replace(*args: object, **kwargs: object) -> None:
            replace_calls.append((args, kwargs))
            raise RuntimeError("Replace captured (avoid actual replacement)")

        with patch.object(
            lexile.lexile_db_update_task, "replace", side_effect=capture_replace
        ):
            with pytest.raises(RuntimeError, match="Replace captured"):
                lexile.lexile_db_update_task.delay(force=False).wait()

        assert len(replace_calls) == 1
        (args, kwargs) = replace_calls[0]
        assert len(args) == 1
        sig = args[0]
        assert sig.kwargs.get("force") is False
        assert sig.kwargs.get("offset") == lexile.BATCH_SIZE
        assert "timestamp_id" in sig.kwargs
        assert sig.kwargs["timestamp_id"] is not None
        assert "lock_value" in sig.kwargs
        assert isinstance(sig.kwargs["lock_value"], str)

    def test_lexile_db_update_task_workflow_lock_configured_to_survive_replace(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Workflow lock is configured with ignored_exceptions=(Ignore,) so it is
        NOT released when task.replace() raises Ignore to chain the next batch."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )

        captured_ignored_exceptions: list[tuple[type[BaseException], ...]] = []
        original_lock = RedisLock.lock

        def spy_lock(
            self_lock: RedisLock,
            raise_when_not_acquired: bool = True,
            release_on_error: bool = True,
            release_on_exit: bool = True,
            ignored_exceptions: tuple[type[BaseException], ...] = (),
        ) -> object:
            captured_ignored_exceptions.append(ignored_exceptions)
            return original_lock(
                self_lock,
                raise_when_not_acquired=raise_when_not_acquired,
                release_on_error=release_on_error,
                release_on_exit=release_on_exit,
                ignored_exceptions=ignored_exceptions,
            )

        with patch.object(RedisLock, "lock", spy_lock):
            lexile.lexile_db_update_task.delay(force=False).wait()

        assert len(captured_ignored_exceptions) == 1
        assert Ignore in captured_ignored_exceptions[0]

    def test_lexile_db_update_task_excludes_overdrive_only_lexile_default_mode(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Default mode does not process ISBNs that have Lexile only from Overdrive."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        overdrive_source = DataSource.lookup(
            db.session, DataSourceConstants.OVERDRIVE, autocreate=True
        )
        identifier = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9780123456789"
        )
        identifier.classify(
            overdrive_source,
            Subject.LEXILE_SCORE,
            "600",
            None,
            weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
        )
        db.session.commit()

        lexile.lexile_db_update_task.delay(force=False).wait()

        db.session.refresh(identifier)
        lexile_classifications = [
            c
            for c in identifier.classifications
            if c.subject.type == Subject.LEXILE_SCORE
        ]
        assert len(lexile_classifications) == 1
        assert (
            lexile_classifications[0].data_source.name == DataSourceConstants.OVERDRIVE
        )

    def test_lexile_db_update_task_excludes_overdrive_only_lexile_force_mode(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        http_client: MockHttpClientFixture,
    ) -> None:
        """Force mode does not process ISBNs that have Lexile only from Overdrive."""
        db.integration_configuration(
            protocol=LexileDBService,
            goal=Goals.METADATA_GOAL,
            settings=LexileDBSettings(
                username="user",
                password="pass",
                base_url="https://api.example.com",
            ),
        )
        overdrive_source = DataSource.lookup(
            db.session, DataSourceConstants.OVERDRIVE, autocreate=True
        )
        identifier = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id="9780123456789"
        )
        identifier.classify(
            overdrive_source,
            Subject.LEXILE_SCORE,
            "600",
            None,
            weight=Classification.TRUSTED_DISTRIBUTOR_WEIGHT,
        )
        db.session.commit()

        lexile.lexile_db_update_task.delay(force=True).wait()

        db.session.refresh(identifier)
        lexile_classifications = [
            c
            for c in identifier.classifications
            if c.subject.type == Subject.LEXILE_SCORE
        ]
        assert len(lexile_classifications) == 1
        assert (
            lexile_classifications[0].data_source.name == DataSourceConstants.OVERDRIVE
        )
