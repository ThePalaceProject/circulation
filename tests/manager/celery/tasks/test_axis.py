import logging
from unittest.mock import MagicMock

import pytest
from fixtures.celery import CeleryFixture
from fixtures.database import DatabaseTransactionFixture
from fixtures.redis import RedisFixture

from palace.manager.celery.tasks.axis import (
    _redis_lock_queue_collection_import,
    queue_collection_import_batches,
)


class QueueCollectionImportLockFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture
        self.redis_client = redis_fixture.client
        self.task = MagicMock()
        self.task.request.root_id = "fake"
        self.task_lock = _redis_lock_queue_collection_import(
            self.redis_client, collection_id=1
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
    caplog.set_level(
        logging.INFO,
        "palace.manager.celery.tasks.axis",
    )
    queue_collection_import_lock_fixture.task_lock.acquire()
    collection_id = 1
    queue_collection_import_batches.delay(collection_id).wait()
    assert "another task holds its lock" in caplog.text
