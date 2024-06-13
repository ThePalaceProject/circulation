from unittest.mock import MagicMock, create_autospec, patch

import pytest

from palace.manager.celery.task import Task
from palace.manager.celery.tasks.patron_activity import sync_patron_activity
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.patron_activity import (
    PatronActivity,
    PatronActivityStatus,
)
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.circulation import MockPatronActivityCirculationAPI


class SyncTaskFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        self.db = db
        self.redis_fixture = redis_fixture
        self.celery_fixture = celery_fixture
        self.services_fixture = services_fixture

        self.library = db.library()
        self.patron = db.patron(library=self.library)
        self.collection = db.collection(library=self.library)

        self.redis_record = PatronActivity(
            self.redis_fixture.client,
            self.collection.id,
            self.patron.id,
            "test-fixture-task-id",
        )
        self.mock_registry = create_autospec(LicenseProvidersRegistry)
        self.services_fixture.services.integration_registry.license_providers.override(
            self.mock_registry
        )
        self.mock_collection_api = MockPatronActivityCirculationAPI(
            self.db.session, self.collection
        )
        self.mock_registry.from_collection.return_value = self.mock_collection_api


@pytest.fixture
def sync_task_fixture(
    db: DatabaseTransactionFixture,
    redis_fixture: RedisFixture,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
):
    return SyncTaskFixture(db, redis_fixture, celery_fixture, services_fixture)


class TestSyncPatronActivity:
    def test_unable_to_lock(
        self, sync_task_fixture: SyncTaskFixture, caplog: pytest.LogCaptureFixture
    ):
        caplog.set_level(LogLevel.info)

        # We lock the patron activity record in redis, so the task cannot acquire it.
        sync_task_fixture.redis_record.lock()

        # We patch the task to raise an exception if the db is accessed. If we don't acquire the lock
        # we should never go out to the database.
        with patch.object(
            Task, "_session_maker", side_effect=Exception()
        ) as mock_session:
            sync_patron_activity.apply_async(
                (sync_task_fixture.collection.id, sync_task_fixture.patron.id, "pin")
            ).wait()

        assert (
            "Patron activity sync task could not acquire lock. "
            "Task will not perform sync. Lock state (LOCKED)"
        ) in caplog.text
        assert mock_session.call_count == 0

    def test_patron_not_found(
        self,
        sync_task_fixture: SyncTaskFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        patron_id = sync_task_fixture.patron.id
        db.session.delete(sync_task_fixture.patron)
        task = sync_patron_activity.apply_async(
            (sync_task_fixture.collection.id, patron_id, "pin")
        )
        task.wait()

        assert f"Patron (id: {patron_id}) not found." in caplog.text

        task_status = sync_task_fixture.redis_record.status()
        assert task_status is not None
        assert task_status.state == PatronActivityStatus.State.FAILED
        assert task_status.task_id == task.id

    def test_collection_not_found(
        self,
        sync_task_fixture: SyncTaskFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        collection_id = sync_task_fixture.collection.id
        db.session.delete(sync_task_fixture.collection)
        task = sync_patron_activity.apply_async(
            (collection_id, sync_task_fixture.patron.id, "pin")
        )
        task.wait()

        assert f"Collection (id: {collection_id}) not found." in caplog.text

        task_status = sync_task_fixture.redis_record.status()
        assert task_status is not None
        assert task_status.state == PatronActivityStatus.State.FAILED
        assert task_status.task_id == task.id

    def test_exception(
        self, sync_task_fixture: SyncTaskFixture, caplog: pytest.LogCaptureFixture
    ):
        sync_task_fixture.mock_registry.from_collection.side_effect = Exception("Boom!")

        with pytest.raises(Exception, match="Boom!"):
            sync_patron_activity.apply_async(
                (sync_task_fixture.collection.id, sync_task_fixture.patron.id, "pin")
            ).wait()

        task_status = sync_task_fixture.redis_record.status()
        assert task_status is not None
        assert task_status.state == PatronActivityStatus.State.FAILED

        assert "An exception occurred during the patron activity sync" in caplog.text
        assert "Boom!" in caplog.text

    def test_not_supported(
        self, sync_task_fixture: SyncTaskFixture, caplog: pytest.LogCaptureFixture
    ):
        caplog.set_level(LogLevel.info)
        sync_task_fixture.mock_registry.from_collection.return_value = MagicMock()

        sync_patron_activity.apply_async(
            (sync_task_fixture.collection.id, sync_task_fixture.patron.id, "pin")
        ).wait()

        task_status = sync_task_fixture.redis_record.status()
        assert task_status is not None
        assert task_status.state == PatronActivityStatus.State.NOT_SUPPORTED

        assert "does not support patron activity sync" in caplog.text
        sync_task_fixture.mock_registry.from_collection.assert_called_once_with(
            sync_task_fixture.db.session, sync_task_fixture.collection
        )

    def test_success(self, sync_task_fixture: SyncTaskFixture):
        sync_patron_activity.apply_async(
            (sync_task_fixture.collection.id, sync_task_fixture.patron.id, "pin")
        ).wait()

        task_status = sync_task_fixture.redis_record.status()
        assert task_status is not None
        assert task_status.state == PatronActivityStatus.State.SUCCESS

        sync_task_fixture.mock_registry.from_collection.assert_called_once_with(
            sync_task_fixture.db.session, sync_task_fixture.collection
        )
        assert len(sync_task_fixture.mock_collection_api.patron_activity_calls) == 1
        assert sync_task_fixture.mock_collection_api.patron_activity_calls[0] == (
            sync_task_fixture.patron,
            "pin",
        )

    def test_force(
        self, sync_task_fixture: SyncTaskFixture, caplog: pytest.LogCaptureFixture
    ):
        # The task has been marked as failed. Normally, this means we don't need to run it again
        # until the status expires.
        caplog.set_level(LogLevel.info)
        sync_task_fixture.redis_record.lock()
        sync_task_fixture.redis_record.fail()

        sync_patron_activity.apply_async(
            (sync_task_fixture.collection.id, sync_task_fixture.patron.id, "pin")
        ).wait()

        assert (
            "Patron activity sync task could not acquire lock. "
            "Task will not perform sync. Lock state (FAILED)"
        ) in caplog.text
        sync_task_fixture.mock_registry.from_collection.assert_not_called()

        # But if we force it, we should run it again.
        caplog.clear()
        sync_patron_activity.apply_async(
            (sync_task_fixture.collection.id, sync_task_fixture.patron.id, "pin"),
            {"force": True},
        ).wait()
        assert "Patron activity sync task could not acquire lock" not in caplog.text
        sync_task_fixture.mock_registry.from_collection.assert_called_once_with(
            sync_task_fixture.db.session, sync_task_fixture.collection
        )

        # And it will update the state when it's done.
        task_status = sync_task_fixture.redis_record.status()
        assert task_status is not None
        assert task_status.state == PatronActivityStatus.State.SUCCESS
