"""Tests for the Lexile DB Celery tasks."""

from __future__ import annotations

from pytest import LogCaptureFixture

from palace.manager.celery.tasks import lexile
from palace.manager.integration.goals import Goals
from palace.manager.integration.metadata.lexile.service import LexileDBService
from palace.manager.integration.metadata.lexile.settings import LexileDBSettings
from palace.manager.service.logging.configuration import LogLevel
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
        lexile.run_lexile_db_update.delay().wait()
        assert "Lexile DB update task queued" in caplog.text

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
