from datetime import timedelta
from unittest.mock import create_autospec

import pytest

from palace.manager.celery.task import Task
from palace.manager.service.redis.models.lock import LockError, RedisLock, TaskLock
from tests.fixtures.redis import RedisFixture


class RedisLockFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture

        self.lock = RedisLock(self.redis_fixture.client, "test_lock")
        self.other_lock = RedisLock(
            self.redis_fixture.client, "test_lock", lock_timeout=timedelta(seconds=1)
        )
        self.no_timeout_lock = RedisLock(
            self.redis_fixture.client, "test_lock", lock_timeout=None
        )


@pytest.fixture
def redis_lock_fixture(redis_fixture: RedisFixture):
    return RedisLockFixture(redis_fixture)


class TestRedisLock:
    def test_acquire(
        self, redis_lock_fixture: RedisLockFixture, redis_fixture: RedisFixture
    ):
        # We can acquire the lock. And acquiring the lock sets a timeout on the key, so the lock
        # will expire eventually if something goes wrong.
        assert redis_lock_fixture.lock.acquire()
        assert redis_fixture.client.ttl(redis_lock_fixture.lock.key) > 0

        # Acquiring the lock again with the same random value should return True
        # and extend the timeout for the lock
        redis_fixture.client.expire(redis_lock_fixture.lock.key, 5)
        timeout = redis_fixture.client.ttl(redis_lock_fixture.lock.key)
        assert redis_lock_fixture.lock.acquire()
        assert redis_fixture.client.ttl(redis_lock_fixture.lock.key) > timeout

        # Acquiring the lock again with a different random value should return False
        assert not redis_lock_fixture.other_lock.acquire()

    def test_acquire_blocking(self, redis_lock_fixture: RedisLockFixture):
        # If you specify a negative timeout, you should get an error
        with pytest.raises(LockError):
            redis_lock_fixture.lock.acquire_blocking(timeout=-5)

        # If you acquire the lock with blocking, it will block until the lock is available or times out.
        # Because the lock timeout on other_lock is 1 second, the first call should fail because its
        # blocking timeout is 0.1 seconds, but the second call should succeed, since its blocking timeout
        # is 2 seconds. It will block and wait, then acquire the lock.
        assert redis_lock_fixture.other_lock.acquire()
        assert not redis_lock_fixture.lock.acquire_blocking(timeout=0.1)
        assert redis_lock_fixture.lock.acquire_blocking(timeout=2)

    def test_release(
        self, redis_lock_fixture: RedisLockFixture, redis_fixture: RedisFixture
    ):
        # If you acquire a lock another client cannot release it
        assert redis_lock_fixture.lock.acquire()
        assert redis_lock_fixture.other_lock.release() is False

        # Make sure the key is set in redis
        assert redis_fixture.client.get(redis_lock_fixture.lock.key) is not None

        # But the client that acquired the lock can release it
        assert redis_lock_fixture.lock.release() is True

        # And the key should be removed from redis
        assert redis_fixture.client.get(redis_lock_fixture.lock.key) is None

    def test_extend_timeout(
        self, redis_lock_fixture: RedisLockFixture, redis_fixture: RedisFixture
    ):
        # If the lock has no timeout, we can't extend it
        assert redis_lock_fixture.no_timeout_lock.acquire()
        assert redis_lock_fixture.no_timeout_lock.extend_timeout() is False
        assert redis_lock_fixture.no_timeout_lock.release() is True

        # If the lock has a timeout, the acquiring client can extend it, but another client cannot
        assert redis_lock_fixture.lock.acquire()
        redis_fixture.client.expire(redis_lock_fixture.lock.key, 5)
        assert redis_lock_fixture.other_lock.extend_timeout() is False
        assert redis_lock_fixture.lock.extend_timeout() is True

        # The key should have a new timeout
        assert redis_fixture.client.ttl(redis_lock_fixture.other_lock.key) > 5

    def test_locked(self, redis_lock_fixture: RedisLockFixture):
        # If the lock is not acquired, it should not be locked
        assert redis_lock_fixture.lock.locked() is False

        # If the lock is acquired, it should be locked
        assert redis_lock_fixture.lock.acquire()
        assert redis_lock_fixture.lock.locked() is True
        assert redis_lock_fixture.other_lock.locked() is True
        assert redis_lock_fixture.lock.locked(by_us=True) is True
        assert redis_lock_fixture.other_lock.locked(by_us=True) is False

        # If the lock is released, it should not be locked
        assert redis_lock_fixture.lock.release() is True
        assert redis_lock_fixture.lock.locked() is False

    def test_lock(self, redis_lock_fixture: RedisLockFixture):
        # The lock can be used as a context manager
        assert redis_lock_fixture.lock.locked() is False
        with redis_lock_fixture.lock.lock() as acquired:
            assert acquired
            assert redis_lock_fixture.lock.locked() is True
        assert redis_lock_fixture.lock.locked() is False

        # The context manager returns LockReturn.acquired if the lock is acquired
        with redis_lock_fixture.no_timeout_lock.lock():
            with redis_lock_fixture.lock.lock() as acquired:
                assert not acquired

        # If the lock is extended, the context manager returns True
        redis_lock_fixture.lock.acquire()
        with redis_lock_fixture.lock.lock() as acquired:
            assert acquired
            assert redis_lock_fixture.lock.locked() is True
        # Exiting the inner context manager should release the lock
        assert redis_lock_fixture.lock.locked() is False

    @pytest.mark.parametrize(
        "release_on_error, release_on_exit",
        (
            (True, True),
            (True, False),
            (False, True),
            (False, False),
        ),
    )
    def test_lock_release_options(
        self,
        release_on_error: bool,
        release_on_exit: bool,
        redis_lock_fixture: RedisLockFixture,
    ):
        # The lock can be used as a context manager with options to control when the lock is released
        assert redis_lock_fixture.lock.locked() is False
        try:
            with redis_lock_fixture.lock.lock(
                release_on_error=release_on_error, release_on_exit=release_on_exit
            ) as acquired:
                assert acquired
                assert redis_lock_fixture.lock.locked() is True
                raise ValueError("Test error")
        except ValueError:
            ...
        assert redis_lock_fixture.lock.locked() is not release_on_error
        redis_lock_fixture.lock.release()

        assert redis_lock_fixture.lock.locked() is False
        with redis_lock_fixture.lock.lock(
            release_on_error=release_on_error, release_on_exit=release_on_exit
        ) as acquired:
            assert acquired
            assert redis_lock_fixture.lock.locked() is True
        assert redis_lock_fixture.lock.locked() is not release_on_exit


class TestTaskLock:
    def test___init__(self, redis_fixture: RedisFixture):
        mock_task = create_autospec(Task)
        mock_task.name = None

        # If we don't provide a lock_name, and the task name is None, we should get an error
        with pytest.raises(LockError):
            TaskLock(redis_fixture.client, mock_task)

        # If we don't provide a lock_name, we should use the task name
        mock_task.name = "test_task"
        task_lock = TaskLock(redis_fixture.client, mock_task)
        assert task_lock.key.endswith("::TaskLock::Task::test_task")

        # If we provide a lock_name, we should use that instead
        task_lock = TaskLock(redis_fixture.client, mock_task, lock_name="test_lock")
        assert task_lock.key.endswith("::TaskLock::test_lock")
