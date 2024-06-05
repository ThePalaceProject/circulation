import datetime

import pytest
from freezegun import freeze_time

from palace.manager.service.redis.models.patron_activity import (
    PatronActivityError,
    PatronActivitySync,
)
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class PatronActivitySyncFixture:
    def __init__(self, db: DatabaseTransactionFixture, redis_fixture: RedisFixture):
        self._db = db
        self._redis = redis_fixture

        self.patron = db.patron()
        self.collection = db.collection()
        self.patron_activity = PatronActivitySync(
            redis_fixture.client, self.patron, self.collection
        )
        self.timeout_slop = 5


@pytest.fixture
def patron_activity_sync_fixture(
    db: DatabaseTransactionFixture, redis_fixture: RedisFixture
):
    return PatronActivitySyncFixture(db, redis_fixture)


class TestPatronActivitySync:
    def test_key(
        self,
        patron_activity_sync_fixture: PatronActivitySyncFixture,
        redis_fixture: RedisFixture,
    ):
        key = patron_activity_sync_fixture.patron_activity.key
        assert key.startswith(redis_fixture.key_prefix)
        patron_key = patron_activity_sync_fixture.patron.__redis_key__()
        collection_key = patron_activity_sync_fixture.collection.__redis_key__()
        assert key.endswith(f"::PatronActivity::{patron_key}::{collection_key}")

    def test_status(self, patron_activity_sync_fixture: PatronActivitySyncFixture):
        patron_activity = patron_activity_sync_fixture.patron_activity

        # If no status is set, we should return None
        assert patron_activity.status() is None

        # If we set a status, we should be able to retrieve it
        assert patron_activity.acquire() is True
        assert patron_activity.status() == patron_activity.Status.IN_PROGRESS

        # If we complete the status, we should be able to retrieve the timestamp
        timestamp = datetime.datetime(1995, 1, 1, 0, 0, 0)
        assert patron_activity.complete(timestamp) is True
        assert patron_activity.status() == timestamp

        # If the status gets cleared we will return None
        assert patron_activity.clear() is True
        assert patron_activity.status() is None

    def test_acquire(
        self,
        patron_activity_sync_fixture: PatronActivitySyncFixture,
        redis_fixture: RedisFixture,
    ):
        patron_activity = patron_activity_sync_fixture.patron_activity

        # If we acquire the status, we should return True
        assert patron_activity.acquire() is True

        # If we try to acquire the status again, we should return False
        assert patron_activity.acquire() is False

        # If we clear the status, we should be able to acquire it again
        assert patron_activity.clear() is True
        assert patron_activity.acquire() is True

        # We set an expiry time for the status, so it will automatically clear.
        timeout = patron_activity.IN_PROGRESS_TIMEOUT
        min_timeout = timeout - patron_activity_sync_fixture.timeout_slop
        max_timeout = timeout
        assert (
            min_timeout < redis_fixture.client.ttl(patron_activity.key) <= max_timeout
        )

    def test_clear(self, patron_activity_sync_fixture: PatronActivitySyncFixture):
        patron_activity = patron_activity_sync_fixture.patron_activity

        # If we clear a status that doesn't exist, we should return False
        assert patron_activity.clear() is False

        # If we acquire a status, we should be able to clear it
        assert patron_activity.acquire() is True
        assert patron_activity.clear() is True

        # If we try to clear it again, we should return False
        assert patron_activity.clear() is False

    def test_complete(
        self,
        patron_activity_sync_fixture: PatronActivitySyncFixture,
        redis_fixture: RedisFixture,
    ):
        patron_activity = patron_activity_sync_fixture.patron_activity

        # If we try to complete a status that doesn't exist, we should return False
        assert patron_activity.complete() is False

        # If we acquire a status, we should be able to complete it. If no timestamp is provided, we should
        # use the current time.
        assert patron_activity.acquire() is True
        test_time = datetime.datetime(1999, 9, 9, 9, 9, 9, tzinfo=datetime.timezone.utc)
        with freeze_time(test_time):
            assert patron_activity.complete() is True
        assert patron_activity.status() == test_time

        # Trying to complete the status again should return False
        assert patron_activity.complete() is False

        # Trying to fail a complete status should return False
        assert patron_activity.fail() is False

        # We can also provide a timestamp to complete the status
        assert patron_activity.clear() is True
        assert patron_activity.acquire() is True
        assert patron_activity.complete(test_time) is True
        assert patron_activity.status() == test_time

        # We set a timeout for the status, so it will automatically clear
        timeout = patron_activity.SUCCESS_TIMEOUT
        min_timeout = timeout - patron_activity_sync_fixture.timeout_slop
        max_timeout = timeout
        assert (
            min_timeout < redis_fixture.client.ttl(patron_activity.key) <= max_timeout
        )

    def test_fail(
        self,
        patron_activity_sync_fixture: PatronActivitySyncFixture,
        redis_fixture: RedisFixture,
    ):
        patron_activity = patron_activity_sync_fixture.patron_activity

        # If we try to fail a status that doesn't exist, we should return False
        assert patron_activity.fail() is False

        # If we acquire a status, we should be able to fail it.
        assert patron_activity.acquire() is True
        assert patron_activity.fail() is True
        assert patron_activity.status() == patron_activity.Status.FAILED

        # Trying to fail the status again should return False
        assert patron_activity.fail() is False

        # Trying to complete a failed status should return False
        assert patron_activity.complete() is False

        # We set a timeout for the status, so it will automatically clear
        timeout = patron_activity.FAILED_TIMEOUT
        min_timeout = timeout - patron_activity_sync_fixture.timeout_slop
        max_timeout = timeout
        assert (
            min_timeout < redis_fixture.client.ttl(patron_activity.key) <= max_timeout
        )

    def test_context_manager(
        self, patron_activity_sync_fixture: PatronActivitySyncFixture
    ):
        patron_activity = patron_activity_sync_fixture.patron_activity

        # The context manager should acquire the status when entered and complete it when exited
        test_time = utc_now()
        with freeze_time(test_time):
            with patron_activity as acquired:
                assert acquired is True
                assert patron_activity.status() == patron_activity.Status.IN_PROGRESS
        assert patron_activity.status() == test_time

        # If there is an exception, the status should be failed
        patron_activity.clear()
        with pytest.raises(Exception):
            with patron_activity as acquired:
                assert acquired is True
                raise Exception()
        assert patron_activity.status() == patron_activity.Status.FAILED

        # If the status is already acquired, we should not be able to acquire it again
        patron_activity.clear()
        patron_activity.acquire()
        with patron_activity as acquired:
            assert acquired is False
        # The context manager should not have changed the status
        assert patron_activity.status() == patron_activity.Status.IN_PROGRESS

        # Nesting the context manager should raise an error
        patron_activity.clear()
        with pytest.raises(PatronActivityError):
            with patron_activity as acquired1:
                assert acquired1 is True
                with patron_activity:
                    # We should never get here
                    assert False
        assert patron_activity.status() == patron_activity.Status.FAILED
