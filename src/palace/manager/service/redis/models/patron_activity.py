from datetime import datetime
from enum import auto
from functools import cached_property
from types import TracebackType
from typing import Literal

from backports.strenum import StrEnum

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.datetime_helpers import utc_now


class PatronActivityError(BasePalaceException, RuntimeError):
    ...


class PatronActivitySync:
    class Status(StrEnum):
        """The status of a patron activity sync task."""

        # The task is currently running.
        IN_PROGRESS = auto()

        # The task failed to complete.
        FAILED = auto()

    IN_PROGRESS_TIMEOUT = 60 * 15  # 15 minutes
    FAILED_TIMEOUT = 60 * 60 * 4  # 4 hours
    SUCCESS_TIMEOUT = 60 * 60 * 12  # 12 hours

    UPDATE_SCRIPT = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('SET', KEYS[1], ARGV[2], 'XX', 'EX', ARGV[3])
    else
        return nil
    end
    """

    def __init__(self, redis_client: Redis, patron: Patron, collection: Collection):
        self._redis_client = redis_client
        self._patron = patron
        self._collection = collection
        self._update_script = self._redis_client.register_script(self.UPDATE_SCRIPT)
        self._in_context_manager = False
        self._context_manager_acquired = False

    @cached_property
    def key(self) -> str:
        return self._redis_client.get_key(
            "PatronActivity", self._patron, self._collection
        )

    def status(self) -> datetime | Status | None:
        status = self._redis_client.get(self.key)
        if status is None:
            return None

        if status in list(self.Status):
            return self.Status(status)

        return datetime.fromisoformat(status)

    def acquire(self) -> bool:
        acquired = self._redis_client.set(
            self.key,
            self.Status.IN_PROGRESS,
            nx=True,
            ex=self.IN_PROGRESS_TIMEOUT,
        )
        return acquired is not None

    def clear(self) -> bool:
        return self._redis_client.delete(self.key) == 1

    def _update(self, status: str, timeout: int) -> bool:
        value_set = self._update_script(
            keys=[self.key],
            args=[self.Status.IN_PROGRESS, status, timeout],
        )
        return value_set is not None

    def complete(self, timestamp: datetime | None = None) -> bool:
        timestamp = timestamp or utc_now()
        return self._update(timestamp.isoformat(), self.SUCCESS_TIMEOUT)

    def fail(self) -> bool:
        return self._update(self.Status.FAILED, self.FAILED_TIMEOUT)

    def __enter__(self) -> bool:
        if self._in_context_manager:
            raise PatronActivityError(f"Cannot nest {self.__class__.__name__}.")
        self._in_context_manager = True
        acquired = self.acquire()
        self._context_manager_acquired = acquired
        return acquired

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None,
    ) -> Literal[False]:
        if self._context_manager_acquired:
            if exctype is not None:
                self.fail()
            else:
                self.complete()

        self._in_context_manager = False
        self._context_manager_acquired = False
        return False
