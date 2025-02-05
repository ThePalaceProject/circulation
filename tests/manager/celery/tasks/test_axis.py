import logging
from unittest.mock import MagicMock, patch

import pytest
from fixtures.celery import CeleryFixture
from fixtures.database import DatabaseTransactionFixture
from fixtures.redis import RedisFixture

from palace.manager.api.axis import Axis360API
from palace.manager.celery.tasks import axis
from palace.manager.celery.tasks.axis import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_START_TIME,
    _redis_lock_queue_collection_import,
    import_all_collections,
    import_items,
    queue_collection_import_batches,
    timestamp,
)
from palace.manager.util.datetime_helpers import utc_now

TEST_COLLECTION_ID = 1


class QueueCollectionImportLockFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture
        self.redis_client = redis_fixture.client
        self.task = MagicMock()
        self.task.request.root_id = "fake"
        self.task_lock = _redis_lock_queue_collection_import(
            self.redis_client, collection_id=TEST_COLLECTION_ID
        )


@pytest.fixture
def queue_collection_import_lock_fixture(redis_fixture: RedisFixture):
    return QueueCollectionImportLockFixture(redis_fixture)


def test_queue_collection_import_lock(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    queue_collection_import_lock_fixture.task_lock.acquire()
    collection_id = TEST_COLLECTION_ID
    queue_collection_import_batches.delay(collection_id).wait()
    assert "another task holds its lock" in caplog.text


def set_caplog_level_to_info(caplog):
    caplog.set_level(
        logging.INFO,
        "palace.manager.celery.tasks.axis",
    )


def test_import_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection1 = db.default_collection()
    collection2 = db.collection(name="test_collection", protocol=Axis360API.label())
    with patch.object(
        axis, "queue_collection_import_batches"
    ) as mock_queue_collection_import_batches:
        import_all_collections.delay().wait()

        assert mock_queue_collection_import_batches.delay.call_count == 1
        assert mock_queue_collection_import_batches.delay.call_args_list[0].kwargs == {
            "collection_id": collection2.id,
            "batch_size": DEFAULT_BATCH_SIZE,
        }
        assert "Finished queuing 1 collection." in caplog.text


def test_queue_collection_import_batches(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())
    mock_api = MagicMock()
    current_time = utc_now()
    mock_api.recent_activity.return_value = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
        (5, "e"),
    ]
    with (
        patch.object(axis, "create_api") as mock_create_api,
        patch.object(axis, "import_items") as mock_import_items,
    ):
        mock_create_api.return_value = mock_api
        queue_collection_import_batches.delay(collection.id, batch_size=3).wait()
        assert mock_import_items.delay.call_count == 2
    ts = timestamp(
        _db=db.session,
        collection=collection,
        service_name="palace.manager.celery.tasks.axis.queue_collection_import_batches",
        default_start_time=DEFAULT_START_TIME,
    )
    assert ts.start > current_time
    assert not queue_collection_import_lock_fixture.task_lock.locked()
    assert mock_api.recent_activity.call_count == 1
    assert mock_api.recent_activity.call_args[0][0] == axis.DEFAULT_START_TIME
    assert mock_import_items.delay.call_count == 2
    assert mock_import_items.delay.call_args_list[0].kwargs == {
        "items": [(1, "a"), (2, "b"), (3, "c")],
        "collection_id": collection.id,
    }
    assert mock_import_items.delay.call_args_list[1].kwargs == {
        "items": [(4, "d"), (5, "e")],
        "collection_id": collection.id,
    }

    assert "Finished queuing items in collection" in caplog.text


def test_import_items(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())
    mock_api = MagicMock()
    with patch.object(axis, "create_api") as mock_create_api:
        mock_create_api.return_value = mock_api
        edition, lp = db.edition(with_license_pool=True)

        mock_api.update_book.return_value = (edition, False, lp, False)
        import_items.delay(collection.id, items=[(1, "a"), (2, "b")]).wait()

    assert mock_api.update_book.call_count == 2
    mock_api.update_book.call_args_list[0].kwargs == {
        "bibiographic": 1,
        "availability": "a",
    }
    mock_api.update_book.call_args_list[0].kwargs == {
        "bibiographic": 2,
        "availability": "b",
    }
    assert f"Edition (id={edition.id}" in caplog.text
