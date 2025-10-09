import datetime
from enum import StrEnum
from functools import cached_property
from types import TracebackType
from typing import Any, Literal, NamedTuple, Self

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin


class PatronActivityError(BasePalaceException, RuntimeError): ...


class PatronActivityStatus:
    """
    Serialize and deserialize the status of a patron activity sync task from a string
    stored in Redis. The string consists of three fields separated by cls.SEPERATOR:
    - The state of the task.
    - The timestamp of the task.
    - The task id.

    The state is a one character string representing the state of the task. The timestamp
    is a 19 character string representing the time the task was last updated in UTC. It is
    stored in ISO 8601 format with second precision without a timezone. The task id is a
    string that uniquely identifies the task.
    """

    class State(StrEnum):
        """
        The state of a patron activity sync.

        The value of the enum is what gets stored in redis for the state.
        """

        # The task is currently running.
        LOCKED = "L"
        # The task failed to complete.
        FAILED = "F"
        # The task completed successfully.
        SUCCESS = "S"
        # The api does not support patron activity sync.
        NOT_SUPPORTED = "N"

    class _FieldOffset(NamedTuple):
        """
        A class to manage the offsets of fields in the redis string representation.

        This helper is here because when we slice a string in Python, the end index is
        exclusive. However, when we use the redis GETRANGE command, the end index is
        inclusive. This class helps us manage the difference between the two.

        The start index is inclusive and the end index is exclusive, as in Python. The
        redis_end property returns the end index in the format expected by the GETRANGE
        command.
        """

        start: int
        end: int | None

        @property
        def slice(self) -> slice:
            return slice(self.start, self.end)

        @property
        def redis_end(self) -> int:
            """
            Get the end index in the format expected by redis GETRANGE.
            """
            return self.end - 1 if self.end is not None else -1

        @property
        def redis(self) -> str:
            """
            Get the start and end index as a string, that can be directly used in a redis
            GETRANGE command.
            """
            return f"{self.start}, {self.redis_end}"

    STATE_FIELD_LEN = 1
    TIMESTAMP_FIELD_LEN = 19

    SEPERATOR = "::"
    SEPERATOR_LEN = len(SEPERATOR)

    STATE_OFFSET = _FieldOffset(0, STATE_FIELD_LEN)
    TIMESTAMP_OFFSET = _FieldOffset(
        STATE_FIELD_LEN + SEPERATOR_LEN,
        STATE_FIELD_LEN + TIMESTAMP_FIELD_LEN + SEPERATOR_LEN,
    )
    TASK_ID_OFFSET = _FieldOffset(
        STATE_FIELD_LEN + TIMESTAMP_FIELD_LEN + 2 * SEPERATOR_LEN, None
    )

    def __init__(
        self,
        *,
        state: State,
        task_id: str,
        timestamp: datetime.datetime | None = None,
    ):
        self.state = state
        self.task_id = task_id

        self.timestamp = timestamp or utc_now()
        if self.timestamp.tzinfo is None:
            raise ValueError("Timestamp must be timezone aware.")

    @classmethod
    def from_redis(cls, data: str) -> Self:
        state = data[cls.STATE_OFFSET.slice]
        timestamp = data[cls.TIMESTAMP_OFFSET.slice]
        aware_timestamp = datetime.datetime.fromisoformat(timestamp).replace(
            tzinfo=datetime.UTC
        )
        task_id = data[cls.TASK_ID_OFFSET.slice]

        return cls(
            state=cls.State(state),
            task_id=task_id,
            timestamp=aware_timestamp,
        )

    def to_redis(self) -> str:
        state_str = str(self.state)
        if len(state_str) != self.STATE_FIELD_LEN:
            raise ValueError(
                f"State field is not the correct length: {state_str}. Expected {self.STATE_FIELD_LEN} characters."
            )

        # Convert the timestamp to UTC before converting to a string.
        utc_local = self.timestamp.astimezone(datetime.UTC).replace(tzinfo=None)
        timestamp_str = utc_local.isoformat(timespec="seconds")
        if len(timestamp_str) != self.TIMESTAMP_FIELD_LEN:
            raise ValueError(
                f"Timestamp field is not the correct length: {timestamp_str}. Expected {self.TIMESTAMP_FIELD_LEN} characters."
            )

        return self.SEPERATOR.join([state_str, timestamp_str, self.task_id])

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, PatronActivityStatus):
            return NotImplemented

        return (
            self.state == other.state
            and self.task_id == other.task_id
            # Since we truncate the timestamp to the second when converting to a string,
            # we need to compare the timestamps with a second of tolerance.
            and self.timestamp - other.timestamp < datetime.timedelta(seconds=1)
        )


class PatronActivity(LoggerMixin):
    """
    A class to manage the status of a patron activity sync in Redis.

    It provides a locking mechanism, so only one task updates the patron activity at a time,
    it also provides a state, so we know if the task is running, failed, succeeded, or not supported.
    Each state change stores a timestamp, so we know when the task was last updated.

    Each status is stored in redis with a timeout, so that eventually, the status will be cleared.

    The design of the locking mechanism is inspired by the Redis documentation on distributed locks:
    https://redis.io/docs/latest/develop/use/patterns/distributed-locks/#correct-implementation-with-a-single-instance
    """

    LOCKED_TIMEOUT = 60 * 15  # 15 minutes
    FAILED_TIMEOUT = 60 * 60 * 4  # 4 hours
    SUCCESS_TIMEOUT = 60 * 60 * 12  # 12 hours
    NOT_SUPPORTED_TIMEOUT = 60 * 60 * 24 * 14  # 2 week

    # We use a lua script so that we can atomically check the status and then update it
    # without worrying about race conditions. The script checks that the state is
    # LOCKED and the task_id matches before updating the status.
    UPDATE_SCRIPT = f"""
    if
        redis.call('GETRANGE', KEYS[1], {PatronActivityStatus.STATE_OFFSET.redis}) == '{PatronActivityStatus.State.LOCKED}'
        and redis.call('GETRANGE', KEYS[1], {PatronActivityStatus.TASK_ID_OFFSET.redis}) == ARGV[1]
    then
        return redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
    else
        return nil
    end
    """

    # Again, we use a lua script so we can atomically check and update the status.
    # This script will delete the key if the status is not locked, or if the task_id
    # matches the task_id that owns the lock.
    CLEAR_SCRIPT = f"""
    if
        redis.call('GETRANGE', KEYS[1], {PatronActivityStatus.STATE_OFFSET.redis}) == '{PatronActivityStatus.State.LOCKED}'
        and redis.call('GETRANGE', KEYS[1], {PatronActivityStatus.TASK_ID_OFFSET.redis}) ~= ARGV[1]
    then
        return nil
    else
        redis.call('DEL', KEYS[1])
        return 1
    end
    """

    def __init__(
        self,
        redis_client: Redis,
        collection: Collection | int | None,
        patron: Patron | int | None,
        task_id: str,
    ):
        self._redis_client = redis_client
        self._patron_id = patron.id if isinstance(patron, Patron) else patron
        self._collection_id = (
            collection.id if isinstance(collection, Collection) else collection
        )
        self._task_id = task_id
        self._update_script = self._redis_client.register_script(self.UPDATE_SCRIPT)
        self._clear_script = self._redis_client.register_script(self.CLEAR_SCRIPT)
        self._in_context_manager = False
        self._context_manager_acquired = False

    @cached_property
    def key(self) -> str:
        return self._get_key(self._redis_client, self._patron_id, self._collection_id)

    def status(self) -> PatronActivityStatus | None:
        """
        Get the current status of the patron activity sync task.

        :return: If the return value is `None` there is no record of the task in
        Redis. Otherwise, the return value is a `PatronActivityStatus` object.
        """
        status = self._redis_client.get(self.key)
        if status is None:
            return None

        return PatronActivityStatus.from_redis(status)

    def lock(self) -> bool:
        """
        Attempt to acquire the lock for the patron activity sync task.

        The lock can only be acquired if the task currently has no data in
        redis. The lock will expire after `LOCKED_TIMEOUT` seconds.

        :return: True if the lock was acquired, False otherwise.
        """
        acquired = self._redis_client.set(
            self.key,
            PatronActivityStatus(
                state=PatronActivityStatus.State.LOCKED, task_id=self._task_id
            ).to_redis(),
            nx=True,
            ex=self.LOCKED_TIMEOUT,
        )
        return acquired is not None

    def clear(self) -> bool:
        """
        Clear the status of the patron activity sync task.

        If the state is not LOCKED, any task can clear the status. If the state is
        LOCKED, only the task that acquired the lock can clear the status.

        :return: True if the status was cleared, False otherwise.
        """
        return (
            self._clear_script(
                keys=[self.key],
                args=[self._task_id],
            )
            is not None
        )

    def _update(self, status: PatronActivityStatus, timeout: int) -> bool:
        value_set = self._update_script(
            keys=[self.key],
            args=[self._task_id, status.to_redis(), timeout],
        )
        return value_set is not None

    def success(self) -> bool:
        """
        Mark the patron activity sync task as successful. This can only be done by
        the task that acquired the lock.

        :return: True if the status was updated, False otherwise.
        """
        return self._update(
            PatronActivityStatus(
                state=PatronActivityStatus.State.SUCCESS, task_id=self._task_id
            ),
            self.SUCCESS_TIMEOUT,
        )

    def fail(self) -> bool:
        """
        Mark the patron activity sync task as failed. This can only be done by
        the task that acquired the lock.

        :return: True if the status was updated, False otherwise.
        """
        return self._update(
            PatronActivityStatus(
                state=PatronActivityStatus.State.FAILED, task_id=self._task_id
            ),
            self.FAILED_TIMEOUT,
        )

    def not_supported(self) -> bool:
        """
        Mark the patron activity sync task as not supported. This can only be done by
        the task that acquired the lock.

        :return: True if the status was set, False otherwise.
        """
        return self._update(
            PatronActivityStatus(
                state=PatronActivityStatus.State.NOT_SUPPORTED, task_id=self._task_id
            ),
            self.NOT_SUPPORTED_TIMEOUT,
        )

    def __enter__(self) -> bool:
        if self._in_context_manager:
            raise PatronActivityError(f"Cannot nest {self.__class__.__name__}.")
        self._in_context_manager = True
        acquired = self.lock()
        self._context_manager_acquired = acquired
        return acquired

    def __exit__(
        self,
        exctype: type[BaseException] | None,
        excinst: BaseException | None,
        exctb: TracebackType | None,
    ) -> Literal[False]:
        if self._context_manager_acquired:
            if excinst is not None:
                self.log.error(
                    "An exception occurred during the patron activity sync. Marking the task as failed.",
                    exc_info=excinst,
                )
                self.fail()
            else:
                self.success()

        self._in_context_manager = False
        self._context_manager_acquired = False
        return False

    @classmethod
    def _get_key(
        cls, redis_client: Redis, patron_id: int | None, collection_id: int | None
    ) -> str:
        return redis_client.get_key(
            "PatronActivity",
            Patron.redis_key_from_id(patron_id),
            Collection.redis_key_from_id(collection_id),
        )

    @classmethod
    def collections_ready_for_sync(
        cls, redis_client: Redis, patron: Patron
    ) -> set[Collection]:
        """
        Find the collections for a patron that have no records in redis for
        patron activity sync. This indicates that the collection is ready to be
        synced.
        """
        # TODO: What should happen to loans that are in a collection that is not active?
        #  For now, we'll handle loans only for active collections.
        collections = patron.library.active_collections
        keys = [
            cls._get_key(redis_client, patron.id, collection.id)
            for collection in collections
        ]
        statuses = redis_client.mget(keys)
        return {
            collection
            for collection, status in zip(collections, statuses)
            if status is None
        }
