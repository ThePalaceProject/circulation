import datetime
from unittest.mock import patch

import pytest
from freezegun import freeze_time

from palace.manager.service.redis.models.patron_activity import (
    PatronActivity,
    PatronActivityError,
    PatronActivityStatus,
)
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture


class TestPatronActivityStatus:
    def test_init(self):
        state = PatronActivityStatus.State.FAILED
        task_id = "abc"

        # If timestamp is provided, it must be timezone aware
        with pytest.raises(ValueError):
            PatronActivityStatus(
                state=state, task_id=task_id, timestamp=datetime.datetime.now()
            )

        timestamp = datetime_utc(year=1995, month=1, day=1)
        status = PatronActivityStatus(state=state, task_id=task_id, timestamp=timestamp)
        assert status.state == state
        assert status.task_id == task_id
        assert status.timestamp == timestamp

        with freeze_time():
            status = PatronActivityStatus(state=state, task_id=task_id)
            assert status.state == state
            assert status.task_id == task_id
            assert status.timestamp == utc_now()

    def test_offsets(self):
        time = datetime_utc(2020, 1, 1, 1, 1)
        status = PatronActivityStatus(
            state=PatronActivityStatus.State.LOCKED, task_id="abc", timestamp=time
        )

        data_str = status.to_redis()

        state = data_str[PatronActivityStatus.STATE_OFFSET.slice]
        timestamp = data_str[PatronActivityStatus.TIMESTAMP_OFFSET.slice]
        task_id = data_str[PatronActivityStatus.TASK_ID_OFFSET.slice]

        assert state == str(PatronActivityStatus.State.LOCKED)
        assert timestamp == "2020-01-01T01:01:00"
        assert task_id == "abc"

    def test_offsets_redis(self, redis_fixture: RedisFixture):
        time = datetime_utc(1919, 1, 2, 3, 4)
        status = PatronActivityStatus(
            state=PatronActivityStatus.State.NOT_SUPPORTED,
            task_id="abc",
            timestamp=time,
        )
        client = redis_fixture.client

        key = client.get_key("test")
        redis_fixture.client.set(key, status.to_redis())

        assert redis_fixture.client.getrange(
            key,
            PatronActivityStatus.STATE_OFFSET.start,
            PatronActivityStatus.STATE_OFFSET.redis_end,
        ) == str(PatronActivityStatus.State.NOT_SUPPORTED)
        assert (
            redis_fixture.client.getrange(
                key,
                PatronActivityStatus.TIMESTAMP_OFFSET.start,
                PatronActivityStatus.TIMESTAMP_OFFSET.redis_end,
            )
            == "1919-01-02T03:04:00"
        )
        assert (
            redis_fixture.client.getrange(
                key,
                PatronActivityStatus.TASK_ID_OFFSET.start,
                PatronActivityStatus.TASK_ID_OFFSET.redis_end,
            )
            == "abc"
        )

    def test_round_trip(self):
        status = PatronActivityStatus(
            state=PatronActivityStatus.State.FAILED, task_id="test::123456"
        )
        status_from_redis = PatronActivityStatus.from_redis(status.to_redis())
        assert status_from_redis == status

    def test_to_redis(self):
        # If the state cannot be converted into a 2 character string, we should raise an error
        time = datetime_utc(2024, 6, 11, 1, 24)
        with freeze_time(time):
            status = PatronActivityStatus(
                state=PatronActivityStatus.State.FAILED, task_id="test-123"
            )
        with patch.object(status, "state", "foo bar baz"):
            with pytest.raises(ValueError):
                status.to_redis()

        # If the timestamp cannot be converted into a 19 character string, we should raise an error
        with patch.object(status, "timestamp") as mock_timestamp:
            mock_timestamp.isoformat.return_value = "foo bar baz"
            with pytest.raises(ValueError):
                status.to_redis()

        assert status.to_redis() == "F::2024-06-11T01:24:00::test-123"

    def test___eq__(self):
        timestamp = utc_now()
        task_id = "test::123456"

        status = PatronActivityStatus(
            state=PatronActivityStatus.State.FAILED,
            task_id=task_id,
            timestamp=timestamp,
        )

        # We cannot compare with a different type
        assert not (status == "foo")

        # We can compare with the same instance
        assert status == status

        # Or a different instance, with same data
        assert status == PatronActivityStatus(
            state=PatronActivityStatus.State.FAILED,
            task_id=task_id,
            timestamp=timestamp,
        )

        # But a different instance with different data will not be equal
        assert status != PatronActivityStatus(
            state=PatronActivityStatus.State.FAILED,
            task_id="different",
            timestamp=timestamp,
        )
        assert status != PatronActivityStatus(
            state=PatronActivityStatus.State.SUCCESS,
            task_id=task_id,
            timestamp=timestamp,
        )
        assert status != PatronActivityStatus(
            state=PatronActivityStatus.State.SUCCESS,
            task_id=task_id,
            timestamp=datetime_utc(2010, 1, 1),
        )


class PatronActivityFixture:
    def __init__(self, db: DatabaseTransactionFixture, redis_fixture: RedisFixture):
        self._db = db
        self._redis = redis_fixture

        self.patron = db.patron()
        self.collection = db.collection()
        self.task_id = "abc"
        self.patron_activity = PatronActivity(
            redis_fixture.client, self.collection.id, self.patron.id, self.task_id
        )
        self.other_patron_activity = PatronActivity(
            redis_fixture.client, self.collection.id, self.patron.id, "123"
        )
        self.timeout_slop = 5

    def assert_ttl(self, expected_ttl: int | None) -> None:
        if expected_ttl is None:
            # No ttl set
            assert self._redis.client.ttl(self.patron_activity.key) == -1
        else:
            min_timeout = expected_ttl - self.timeout_slop
            max_timeout = expected_ttl
            assert (
                min_timeout
                < self._redis.client.ttl(self.patron_activity.key)
                <= max_timeout
            )

    def assert_state(self, state: PatronActivityStatus.State):
        status = self.patron_activity.status()
        assert status is not None
        assert status.state == state


@pytest.fixture
def patron_activity_fixture(
    db: DatabaseTransactionFixture, redis_fixture: RedisFixture
):
    return PatronActivityFixture(db, redis_fixture)


class TestPatronActivity:
    def test_key(
        self,
        patron_activity_fixture: PatronActivityFixture,
        redis_fixture: RedisFixture,
    ):
        key = patron_activity_fixture.patron_activity.key
        assert key.startswith(redis_fixture.key_prefix)
        patron_key = patron_activity_fixture.patron.redis_key()
        collection_key = patron_activity_fixture.collection.redis_key()
        assert key.endswith(f"::PatronActivity::{patron_key}::{collection_key}")

    def test_status(self, patron_activity_fixture: PatronActivityFixture):
        patron_activity = patron_activity_fixture.patron_activity

        # If there is no record, we should return None
        assert patron_activity.status() is None

        # If acquire the lock, we should be able to retrieve the status
        assert patron_activity.lock() is True
        status = patron_activity.status()
        assert status is not None
        assert status.state == PatronActivityStatus.State.LOCKED
        assert status.task_id == patron_activity_fixture.task_id

        # If we complete the sync, we should be able to retrieve the timestamp
        timestamp = datetime_utc(1995, 1, 1)
        with freeze_time(timestamp):
            assert patron_activity.success() is True
        status = patron_activity.status()
        assert status is not None
        assert status.state == PatronActivityStatus.State.SUCCESS
        assert status.task_id == patron_activity_fixture.task_id
        assert status.timestamp == timestamp

        # If the status gets cleared we will return None
        assert patron_activity.clear() is True
        assert patron_activity.status() is None

    def test_lock(
        self,
        patron_activity_fixture: PatronActivityFixture,
    ):
        patron_activity = patron_activity_fixture.patron_activity

        # If we acquire the lock, we should return True
        assert patron_activity.lock() is True

        # We cannot acquire the lock again, so we should return False
        assert patron_activity.lock() is False

        # We set an expiry time for the LOCKED_TIMEOUT status, so it will automatically clear
        # if anything goes wrong and the lock is never released.
        patron_activity_fixture.assert_ttl(patron_activity.LOCKED_TIMEOUT)

        # A different patron activity object cannot acquire the lock because we already have it
        assert patron_activity_fixture.other_patron_activity.lock() is False

    def test_not_supported(
        self,
        patron_activity_fixture: PatronActivityFixture,
    ):
        patron_activity = patron_activity_fixture.patron_activity

        # If we don't have the lock acquired, we are unable to set the state to NOT_SUPPORTED
        assert patron_activity.not_supported() is False

        # Once we acquire the lock, no other task can set the state to NOT_SUPPORTED, but we can
        assert patron_activity.lock() is True
        assert patron_activity_fixture.other_patron_activity.not_supported() is False
        assert patron_activity.not_supported() is True

        patron_activity_fixture.assert_state(PatronActivityStatus.State.NOT_SUPPORTED)

        # And the key has the expected TTL set
        patron_activity_fixture.assert_ttl(patron_activity.NOT_SUPPORTED_TIMEOUT)

        # We are able to clear the NOT_SUPPORTED status
        assert patron_activity.clear() is True
        assert patron_activity.status() is None

    def test_clear(self, patron_activity_fixture: PatronActivityFixture):
        patron_activity = patron_activity_fixture.patron_activity
        other_patron_activity = patron_activity_fixture.other_patron_activity

        # Clearing a status that doesn't exist will return True since there
        # is nothing to clear.
        assert patron_activity.status() is None
        assert patron_activity.clear() is True

        # Once we acquire the lock, no one else can clear it except for us
        assert patron_activity.lock() is True
        assert other_patron_activity.clear() is False
        assert patron_activity.clear() is True

        # If we clear the status, we cannot complete or fail it
        assert patron_activity.fail() is False
        assert patron_activity.success() is False

        # If we complete or fail the status, any one can clear it
        for clear_func in [patron_activity.clear, other_patron_activity.clear]:
            for complete_func in [patron_activity.fail, patron_activity.success]:
                assert patron_activity.lock() is True
                assert complete_func() is True
                assert patron_activity.status() is not None
                assert clear_func() is True
                assert patron_activity.status() is None

    def test_success(
        self,
        patron_activity_fixture: PatronActivityFixture,
    ):
        patron_activity = patron_activity_fixture.patron_activity

        # If we to update the status to success for a status that doesn't exist, we should return False
        assert patron_activity.success() is False

        # Once we acquire the lock, a PatronActivity with a different task_id
        # cannot update the status to success.
        assert patron_activity.lock() is True
        assert patron_activity_fixture.other_patron_activity.success() is False

        # But we can, and the status should be updated
        test_time = datetime_utc(1999, 9, 9, 9, 9, 9)
        with freeze_time(test_time):
            assert patron_activity.success() is True
        assert patron_activity.status() == PatronActivityStatus(
            state=PatronActivityStatus.State.SUCCESS,
            task_id=patron_activity_fixture.task_id,
            timestamp=test_time,
        )

        # Calling success again should return False
        assert patron_activity.success() is False

        # Trying to fail a successful status should return False
        assert patron_activity.fail() is False

        # We set a timeout for the status, so it will automatically clear, indicating that
        # we want to sync the patron activity again.
        patron_activity_fixture.assert_ttl(patron_activity.SUCCESS_TIMEOUT)

    def test_fail(
        self,
        patron_activity_fixture: PatronActivityFixture,
    ):
        patron_activity = patron_activity_fixture.patron_activity

        # If we try to fail a status that doesn't exist, we should return False
        assert patron_activity.fail() is False

        # If we acquire the lock, a different PatronActivity object cannot update the status to failed
        assert patron_activity.lock() is True
        assert patron_activity_fixture.other_patron_activity.fail() is False

        # But we can, and the status should be updated
        with freeze_time():
            assert patron_activity.fail() is True
            assert patron_activity.status() == PatronActivityStatus(
                state=PatronActivityStatus.State.FAILED,
                task_id=patron_activity_fixture.task_id,
            )

        # Trying to fail the status again should return False
        assert patron_activity.fail() is False

        # Trying to complete a failed status should return False
        assert patron_activity.success() is False

        # We set a timeout for the status, so it will automatically clear after some time
        # indicating that we want to try to sync the patron activity again.
        patron_activity_fixture.assert_ttl(patron_activity.FAILED_TIMEOUT)

    def test_context_manager(
        self,
        patron_activity_fixture: PatronActivityFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        patron_activity = patron_activity_fixture.patron_activity
        other_patron_activity = patron_activity_fixture.other_patron_activity

        # The context manager should set the status to LOCKED when entered and complete it when exited
        with freeze_time():
            with patron_activity as acquired:
                assert acquired is True
                patron_activity_fixture.assert_state(PatronActivityStatus.State.LOCKED)
        patron_activity_fixture.assert_state(PatronActivityStatus.State.SUCCESS)

        # If there is an exception, the status should be failed instead of completed, and we should
        # log the exception.
        patron_activity.clear()
        with pytest.raises(Exception):
            with patron_activity as acquired:
                assert acquired is True
                raise Exception()
        patron_activity_fixture.assert_state(PatronActivityStatus.State.FAILED)
        assert "An exception occurred during the patron activity sync" in caplog.text

        # If the status is already LOCKED, the context manager returns False to indicate that
        # it could not acquire the lock
        patron_activity.clear()
        other_patron_activity.lock()
        with patron_activity as acquired:
            assert acquired is False
        # In this case the context manager does not update the status when it exits
        patron_activity_fixture.assert_state(PatronActivityStatus.State.LOCKED)
        assert other_patron_activity.clear() is True

        # Nesting the context manager will raise an error
        with pytest.raises(PatronActivityError):
            with patron_activity as acquired:
                assert acquired is True
                with patron_activity:
                    ...

    def test_collections_ready_for_sync(
        self, redis_fixture: RedisFixture, db: DatabaseTransactionFixture
    ):
        library = db.library()
        patron = db.patron(library=library)

        collection_success = db.collection(library=library)
        activity_success = PatronActivity(
            redis_fixture.client, collection_success, patron, "abc"
        )
        with activity_success:
            activity_success.success()

        collection_fail = db.collection(library=library)
        activity_fail = PatronActivity(
            redis_fixture.client, collection_fail, patron, "def"
        )
        with activity_fail:
            activity_fail.fail()

        collection_not_supported = db.collection(library=library)
        activity_not_supported = PatronActivity(
            redis_fixture.client, collection_not_supported, patron, "ghi"
        )
        with activity_not_supported:
            activity_not_supported.not_supported()

        collection_locked = db.collection(library=library)
        activity_locked = PatronActivity(
            redis_fixture.client, collection_locked, patron, "jkl"
        )
        activity_locked.lock()

        collection_ready_1 = db.collection(library=library)
        collection_ready_2 = db.collection(library=library)

        assert PatronActivity.collections_ready_for_sync(
            redis_fixture.client, patron
        ) == {collection_ready_1, collection_ready_2}
