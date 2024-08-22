import json
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from datetime import timedelta
from functools import cached_property
from typing import Any, TypeVar, cast
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
    def key(self) -> str:
        """
        Return the key used to store the lock in Redis.

        :return: The key used to store the lock in Redis.
        """

    def _exception_exit(self) -> None:
        """
        Clean up before exiting the context manager, if an exception occurs.
        """
        self.release()

    def _normal_exit(self) -> None:
        """
        Clean up before exiting the context manager, if no exception occurs.
        """
        self.release()

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
            if release_on_error and exception_occurred:
                self._exception_exit()
            elif release_on_exit and not exception_occurred:
                self._normal_exit()


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
    def key(self) -> str:
        return self._redis_client.get_key(self._lock_type, *self._lock_name)

    @property
    def _lock_type(self) -> str:
        return self.__class__.__name__

    def acquire(self) -> bool:
        previous_value = cast(
            str | None,
            self._redis_client.set(
                self.key,
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
        ret_val: int = self.unlock_script(keys=(self.key,), args=(self._random_value,))
        return ret_val == 1

    def extend_timeout(self) -> bool:
        if self._lock_timeout is None:
            # If the lock has no timeout, we can't extend it
            return False

        timout_ms = int(self._lock_timeout.total_seconds() * 1000)
        ret_val: int = self.extend_script(
            keys=(self.key,), args=(self._random_value, timout_ms)
        )
        return ret_val == 1

    def locked(self, by_us: bool = False) -> bool:
        key_value = self._redis_client.get(self.key)
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


class RedisJsonLock(BaseRedisLock, ABC):
    _ACQUIRE_SCRIPT = """
        -- If the locks json object doesn't exist, create it with the initial value
        redis.call("json.set", KEYS[1], "$", ARGV[4], "nx")

        -- Get the current lock value
        local lock_value = cjson.decode(redis.call("json.get", KEYS[1], ARGV[1]))[1]
        if not lock_value then
            -- The lock isn't currently locked, so we lock it and set the timeout
            redis.call("json.set", KEYS[1], ARGV[1], cjson.encode(ARGV[2]))
            redis.call("pexpire", KEYS[1], ARGV[3])
            return 1
        elseif lock_value == ARGV[2] then
            -- The lock is already held by us, so we extend the timeout
            redis.call("pexpire", KEYS[1], ARGV[3])
            return 2
        else
            -- The lock is held by someone else, we do nothing
            return nil
        end
    """

    _RELEASE_SCRIPT = """
        if cjson.decode(redis.call("json.get", KEYS[1], ARGV[1]))[1] == ARGV[2] then
            redis.call("json.del", KEYS[1], ARGV[1])
            return 1
        else
            return nil
        end
    """

    _EXTEND_SCRIPT = """
        if cjson.decode(redis.call("json.get", KEYS[1], ARGV[1]))[1] == ARGV[2] then
            redis.call("pexpire", KEYS[1], ARGV[3])
            return 1
        else
            return nil
        end
    """

    _DELETE_SCRIPT = """
        if cjson.decode(redis.call("json.get", KEYS[1], ARGV[1]))[1] == ARGV[2] then
            redis.call("del", KEYS[1])
            return 1
        else
            return nil
        end
    """

    def __init__(
        self,
        redis_client: Redis,
        random_value: str | None = None,
    ):
        super().__init__(redis_client, random_value)

        # Register our scripts
        self._acquire_script = self._redis_client.register_script(self._ACQUIRE_SCRIPT)
        self._release_script = self._redis_client.register_script(self._RELEASE_SCRIPT)
        self._extend_script = self._redis_client.register_script(self._EXTEND_SCRIPT)
        self._delete_script = self._redis_client.register_script(self._DELETE_SCRIPT)

    @property
    @abstractmethod
    def _lock_timeout_ms(self) -> int:
        """
        The lock timeout in milliseconds.
        """
        ...

    @property
    def _lock_json_key(self) -> str:
        """
        The key to use for the lock value in the JSON object.

        This can be overridden if you need to store the lock value in a different key. It should
        be a Redis JSONPath.
        See: https://redis.io/docs/latest/develop/data-types/json/path/
        """
        return "$.lock"

    @property
    def _initial_value(self) -> str:
        """
        The initial value to use for the locks JSON object.
        """
        return json.dumps({})

    T = TypeVar("T")

    @classmethod
    def _parse_multi(
        cls, value: Mapping[str, Sequence[T]] | None
    ) -> dict[str, T | None]:
        if value is None:
            return {}
        return {k: cls._parse_value(v) for k, v in value.items()}

    @staticmethod
    def _parse_value(value: Sequence[T] | None) -> T | None:
        if value is None:
            return None
        try:
            return value[0]
        except IndexError:
            return None

    @classmethod
    def _parse_value_or_raise(cls, value: Sequence[T] | None) -> T:
        parsed_value = cls._parse_value(value)
        if parsed_value is None:
            raise LockError(f"Could not parse value ({json.dumps(value)})")
        return parsed_value

    def _get_value(self, json_key: str) -> Any | None:
        value = self._redis_client.json().get(self.key, json_key)
        if value is None or len(value) != 1:
            return None
        return value[0]

    def acquire(self) -> bool:
        return (
            self._acquire_script(
                keys=(self.key,),
                args=(
                    self._lock_json_key,
                    self._random_value,
                    self._lock_timeout_ms,
                    self._initial_value,
                ),
            )
            is not None
        )

    def release(self) -> bool:
        """
        Release the lock.

        You must have the lock to release it. This will unset the lock value in the JSON object, but importantly
        it will not delete the JSON object itself. If you want to delete the JSON object, use the delete method.
        """
        return (
            self._release_script(
                keys=(self.key,),
                args=(self._lock_json_key, self._random_value),
            )
            is not None
        )

    def locked(self, by_us: bool = False) -> bool:
        lock_value: str | None = self._parse_value(
            self._redis_client.json().get(self.key, self._lock_json_key)
        )
        if by_us:
            return lock_value == self._random_value
        return lock_value is not None

    def extend_timeout(self) -> bool:
        return (
            self._extend_script(
                keys=(self.key,),
                args=(self._lock_json_key, self._random_value, self._lock_timeout_ms),
            )
            is not None
        )

    def delete(self) -> bool:
        """
        Delete the whole json object, including the lock. Must have the lock to delete the object.
        """
        return (
            self._delete_script(
                keys=(self.key,),
                args=(self._lock_json_key, self._random_value),
            )
            is not None
        )
