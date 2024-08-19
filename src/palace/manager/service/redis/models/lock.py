import random
import time
from abc import ABC, abstractmethod
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import timedelta
from functools import cached_property
from typing import cast
from uuid import uuid4

from palace.manager.celery.task import Task
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.redis.redis import Redis


class LockError(BasePalaceException):
    pass


class BaseRedisLock(ABC):
    def __init__(
        self,
        redis_client: Redis,
        random_value: str | None = None,
    ):
        self._redis_client = redis_client
        self._random_value = random_value if random_value else str(uuid4())

    @abstractmethod
    def acquire(self) -> bool:
        """
        Acquire the lock. Always non-blocking.

        :return: True if the lock was acquired, False otherwise.
        """

    @abstractmethod
    def release(self) -> bool:
        """
        Release the lock.

        :return: True if the lock was released, False if there was some error releasing the lock.
        """

    @abstractmethod
    def locked(self, by_us: bool = False) -> bool:
        """
        Check if the lock is currently held, by us or anyone else.

        :param by_us: If True, check if the lock is held by us. If False, check if the lock is held by anyone.

        :return: True if the lock is held, False otherwise.
        """

    @abstractmethod
    def extend_timeout(self) -> bool:
        """
        Extend the timeout of the lock.

        :return: True if the timeout was extended, False otherwise.
        """

    @property
    @abstractmethod
    def lock_key(self) -> str:
        """
        Return the key used to store the lock in Redis.

        :return: The key used to store the lock in Redis.
        """

    @contextmanager
    def lock(
        self,
        release_on_error: bool = True,
        release_on_exit: bool = True,
        ignored_exceptions: tuple[type[BaseException], ...] = (),
    ) -> Generator[bool, None, None]:
        """
        Context manager for acquiring and releasing the lock.

        :param release_on_error: If True, release the lock if an exception occurs.
        :param release_on_exit: If True, release the lock when the context manager exits.
        :param ignored_exceptions: Exceptions that should not cause the lock to be released.

        :return: The result of the lock acquisition. You must check the return value to see if the lock was acquired.
        """
        locked = self.acquire()
        exception_occurred = False
        try:
            yield locked
        except Exception as exc:
            if not issubclass(exc.__class__, ignored_exceptions):
                exception_occurred = True
            raise
        finally:
            if (release_on_error and exception_occurred) or (
                release_on_exit and not exception_occurred
            ):
                self.release()


class RedisLock(BaseRedisLock):
    """
    A simple distributed lock implementation using Redis.

    See https://redis.io/docs/latest/develop/use/patterns/distributed-locks/
    This is based on "Correct Implementation with a Single Instance" from that page.
    """

    _UNLOCK_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    _EXTEND_SCRIPT = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("pexpire", KEYS[1], ARGV[2])
    else
        return 0
    end
    """

    def __init__(
        self,
        redis_client: Redis,
        lock_name: str | Sequence[str],
        random_value: str | None = None,
        lock_timeout: timedelta | None = timedelta(minutes=5),
        retry_delay: float = 0.2,
    ):
        super().__init__(redis_client, random_value)
        if isinstance(lock_name, str):
            lock_name = [lock_name]
        self._lock_timeout = lock_timeout
        self._retry_delay = retry_delay
        self._lock_name = lock_name
        self.unlock_script = self._redis_client.register_script(self._UNLOCK_SCRIPT)
        self.extend_script = self._redis_client.register_script(self._EXTEND_SCRIPT)

    @cached_property
    def lock_key(self) -> str:
        return self._redis_client.get_key(self._lock_type, *self._lock_name)

    @property
    def _lock_type(self) -> str:
        return self.__class__.__name__

    def acquire(self) -> bool:
        previous_value = cast(
            str | None,
            self._redis_client.set(
                self.lock_key,
                self._random_value,
                nx=True,
                px=self._lock_timeout,
                get=True,
            ),
        )

        if (
            previous_value is not None
            and previous_value == self._random_value
            and self._lock_timeout is not None
        ):
            return self.extend_timeout()

        return previous_value is None or previous_value == self._random_value

    def acquire_blocking(self, timeout: float | int = -1) -> bool:
        """
        Acquire the lock. Blocks until the lock is acquired or the timeout is reached.

        This is a light wrapper around acquire that adds blocking and timeout functionality.

        :param timeout: The maximum time to wait for the lock to be acquired. If 0, wait indefinitely.

        :return: The result of the lock acquisition. You must check the return value to see if the lock was acquired.
        """
        if timeout < 0:
            raise LockError("Cannot specify a negative timeout")

        start_time = time.time()
        while timeout == 0 or (time.time() - start_time) < timeout:
            acquired = self.acquire()
            if acquired:
                return acquired
            delay = random.uniform(0, self._retry_delay)
            time.sleep(delay)
        return False

    def release(self) -> bool:
        ret_val: int = self.unlock_script(
            keys=(self.lock_key,), args=(self._random_value,)
        )
        return ret_val == 1

    def extend_timeout(self) -> bool:
        if self._lock_timeout is None:
            # If the lock has no timeout, we can't extend it
            return False

        timout_ms = int(self._lock_timeout.total_seconds() * 1000)
        ret_val: int = self.extend_script(
            keys=(self.lock_key,), args=(self._random_value, timout_ms)
        )
        return ret_val == 1

    def locked(self, by_us: bool = False) -> bool:
        key_value = self._redis_client.get(self.lock_key)
        if by_us:
            return key_value == self._random_value
        return key_value is not None


class TaskLock(RedisLock):
    def __init__(
        self,
        redis_client: Redis,
        task: Task,
        lock_name: str | None = None,
        lock_timeout: timedelta | None = timedelta(minutes=5),
        retry_delay: float = 0.2,
    ):
        random_value = task.request.root_id or task.request.id
        if lock_name is None:
            if task.name is None:
                raise LockError(
                    "Task.name must not be None if lock_name is not provided."
                )
            name = ["Task", task.name]
        else:
            name = [lock_name]
        super().__init__(redis_client, name, random_value, lock_timeout, retry_delay)
