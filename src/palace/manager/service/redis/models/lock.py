import random
import time
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import timedelta
from enum import Enum, auto
from typing import cast
from uuid import uuid4

from palace.manager.celery.task import Task
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.redis.redis import Redis


class LockError(BasePalaceException):
    pass


class LockReturn(Enum):
    failed = auto()
    timeout = auto()
    acquired = auto()
    extended = auto()

    def __bool__(self) -> bool:
        return self in (LockReturn.acquired, LockReturn.extended)


class RedisLock:
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
        timeout: timedelta | None = timedelta(minutes=5),
        retry_delay: float = 0.2,
    ):
        self._redis_client = redis_client
        if isinstance(lock_name, str):
            lock_name = [lock_name]
        self.lock_key = self._redis_client.get_key(self._lock_type, *lock_name)
        self.random_value = random_value if random_value else str(uuid4())
        self.timeout = timeout
        self.unlock_script = self._redis_client.register_script(self._UNLOCK_SCRIPT)
        self.extend_script = self._redis_client.register_script(self._EXTEND_SCRIPT)
        self._retry_delay = retry_delay

    @property
    def _lock_type(self) -> str:
        return self.__class__.__name__

    def _acquire(self) -> LockReturn:
        previous_value = cast(
            str | None,
            self._redis_client.set(
                self.lock_key, self.random_value, nx=True, px=self.timeout, get=True
            ),
        )

        if (
            previous_value is not None
            and previous_value == self.random_value
            and self.timeout is not None
        ):
            return LockReturn.extended if self.extend_timeout() else LockReturn.failed

        return (
            LockReturn.acquired
            if previous_value is None or previous_value == self.random_value
            else LockReturn.failed
        )

    def acquire(self, blocking: bool = False, timeout: float | int = -1) -> LockReturn:
        if not blocking and timeout != -1:
            raise LockError("Cannot specify a timeout without blocking")

        if not blocking:
            return self._acquire()

        start_time = time.time()
        while timeout == -1 or (time.time() - start_time) < timeout:
            acquired = self._acquire()
            if acquired:
                return acquired
            delay = random.uniform(0, self._retry_delay)
            time.sleep(delay)
        return LockReturn.timeout

    def release(self) -> bool:
        ret_val: int = self.unlock_script(
            keys=(self.lock_key,), args=(self.random_value,)
        )
        return ret_val == 1

    def extend_timeout(self) -> bool:
        if self.timeout is None:
            # If the lock has no timeout, we can't extend it
            return False

        timout_ms = int(self.timeout.total_seconds() * 1000)
        ret_val: int = self.extend_script(
            keys=(self.lock_key,), args=(self.random_value, timout_ms)
        )
        return ret_val == 1

    def locked(self, by_us: bool = False) -> bool:
        key_value = self._redis_client.get(self.lock_key)
        if by_us:
            return key_value == self.random_value
        return key_value is not None

    @contextmanager
    def lock(
        self,
        blocking: bool = False,
        timeout: float | int = -1,
        release_on_error: bool = True,
        release_on_exit: bool = True,
        ignored_exceptions: tuple[type[BaseException], ...] = (),
    ) -> Generator[LockReturn, None, None]:
        locked = self.acquire(blocking=blocking, timeout=timeout)
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


class TaskLock(RedisLock):
    def __init__(
        self,
        redis_client: Redis,
        task: Task,
        lock_name: str | None = None,
        timeout: timedelta | None = timedelta(minutes=5),
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
        super().__init__(redis_client, name, random_value, timeout, retry_delay)
