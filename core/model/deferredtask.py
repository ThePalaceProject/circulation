import datetime
import json
import uuid
from enum import Enum, auto
from typing import Any

from pydantic.dataclasses import dataclass
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Session
from sqlalchemy.types import Enum as SqlAlchemyEnum

from core.exceptions import BasePalaceException
from core.model import (
    LOCK_ID_DEFERRED_TASK_CREATE,
    LOCK_ID_DEFERRED_TASK_START_NEXT,
    Base,
    create,
    flush,
    pg_advisory_lock,
)
from core.util.datetime_helpers import utc_now


class DeferredTaskStatus(Enum):
    READY = auto()
    PROCESSING = auto()
    SUCCESS = auto()
    FAILURE = auto()


class DeferredTaskType(Enum):
    INVENTORY_REPORT = auto()


class DeferredTaskInvalidStateTransition(BasePalaceException):
    """Indicates an unexpected/invalid task state transition"""


class DeferredTask(Base):
    """A task to be processed at some point in the future."""

    __tablename__ = "deferredtasks"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created = Column(
        DateTime(timezone=True), index=True, nullable=False, default=utc_now
    )
    task_type = Column(SqlAlchemyEnum(DeferredTaskType), index=True, nullable=False)
    status = Column(SqlAlchemyEnum(DeferredTaskStatus), index=True, nullable=False)
    processing_start_time = Column(DateTime(timezone=True), nullable=True)
    processing_end_time = Column(DateTime(timezone=True), nullable=True)
    status_details = Column(String, nullable=True)
    data: dict[str, Any] = Column(MutableDict.as_mutable(JSONB), default={})

    def _check_status(self, status: DeferredTaskStatus) -> None:
        if self.status != status:
            raise DeferredTaskInvalidStateTransition(
                message=f"The task ({self.id}, currently in the {self.status} state must be in "
                f"the {status} state in order to transition to a completion state"
            )

    def complete(self) -> None:
        self._check_status(DeferredTaskStatus.PROCESSING)
        self.status = DeferredTaskStatus.SUCCESS
        self.processing_end_time = datetime.datetime.now()

    def fail(self, failure_details: str) -> None:
        self._check_status(DeferredTaskStatus.PROCESSING)
        self.status = DeferredTaskStatus.FAILURE
        self.processing_end_time = datetime.datetime.now()
        self.status_details = failure_details


def start_next_task(_db: Session, task_type: DeferredTaskType) -> DeferredTask | None:
    """
    Start the next ready task in the queue of the specified type.
    The next task will be the oldest task in the READY state.
    Once retrieved the status is set to the "PROCESSING" status and the
    start time property is set to the current time.
    """
    t: DeferredTask | None = (
        _db.query(DeferredTask)
        .filter(DeferredTask.task_type == task_type)
        .filter(DeferredTask.status == DeferredTaskStatus.READY)
        .order_by(DeferredTask.created)
        .first()
    )

    if t:
        with pg_advisory_lock(_db, lock_id=LOCK_ID_DEFERRED_TASK_START_NEXT):
            t.status = DeferredTaskStatus.PROCESSING
            t.processing_start_time = datetime.datetime.now()
            flush(_db)

    return t


def queue_task(
    _db: Session, task_type: DeferredTaskType, data: dict[str, str]
) -> tuple[DeferredTask, bool]:
    """
    Add a new task of the specified task type to the task queue.
    If the task is a duplicate - ie the task data and task_type match an existing task in the
    READY state - the task returned will be the existing task.
    :param _db:
    :param task_type: The type of task
    :param data: The data associated with the task
    :return: Tuple containing a task and a boolean flag indicating whether a new task was created.
    """

    # ensure that simultaneous identical calls in separate processes do not result in duplicate tasks.
    with pg_advisory_lock(_db, lock_id=LOCK_ID_DEFERRED_TASK_CREATE):
        # does an unprocessed task like this already exist?
        t = (
            _db.query(DeferredTask)
            .filter(DeferredTask.task_type == task_type)
            .filter(DeferredTask.status == DeferredTaskStatus.READY)
            .filter(DeferredTask.data.cast(String) == json.dumps(data))
            .first()
        )

        if t:
            return t, False
        else:
            t = create(
                _db,
                DeferredTask,
                task_type=task_type,
                status=DeferredTaskStatus.READY,
                data=data,
            )

            return t


@dataclass
class InventoryReportTaskData:
    admin_id: int
    library_id: int
    admin_email: str
