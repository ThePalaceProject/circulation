# Async
import datetime
import json
import uuid
from enum import Enum
from typing import Any

from pydantic.dataclasses import dataclass
from sqlalchemy import Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.types import Enum as SqlAlchemyEnum

from core.model import Base, create
from core.util.datetime_helpers import utc_now


class AsyncTaskStatus(str, Enum):
    READY = "READY"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class AsyncTaskType(Enum):
    INVENTORY_REPORT = "INVENTORY_REPORT"


class AsyncTask(Base):
    """An asynchronous task."""

    __tablename__ = "asynctasks"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created = Column(
        DateTime(timezone=True), index=True, nullable=False, default=utc_now
    )
    task_type = Column(SqlAlchemyEnum(AsyncTaskType), index=False, nullable=False)
    status = Column(SqlAlchemyEnum(AsyncTaskStatus), index=False, nullable=False)
    processing_start_time = Column(DateTime(timezone=True), nullable=True)
    processing_end_time = Column(DateTime(timezone=True), nullable=True)
    status_details = Column(String, nullable=True)
    data: dict[str, Any] = Column(MutableDict.as_mutable(JSONB), default={})

    def __repr__(self):
        return f"<{self.__class__.__name__}({repr(self.__dict__)})>"

    def complete(self):
        if self.status != AsyncTaskStatus.PROCESSING:
            raise Exception(
                "The task must be in the PROCESSING state in order to transition to a completion state"
            )
        self.status = AsyncTaskStatus.SUCCESS
        self.processing_end_time = datetime.datetime.now()

    def fail(self, failure_details: str):
        if self.status != AsyncTaskStatus.PROCESSING:
            raise Exception(
                "The task must be in the PROCESSING state in order to transition to a completion state"
            )
        self.status = AsyncTaskStatus.FAILURE
        self.processing_end_time = datetime.datetime.now()
        self.status_details = failure_details


def start_next_task(_db, task_type: AsyncTaskType) -> AsyncTask | None:
    """
    Start the next ready task in the queue of the specified type.
    The next task will be the oldest task in the READY state.
    Once retrieved the status is set to the "PROCESSING" status and the
    start time property is set to the current time.
    """
    t: AsyncTask = (
        _db.query(AsyncTask)
        .filter(AsyncTask.task_type == task_type)
        .filter(AsyncTask.status == AsyncTaskStatus.READY)
        .order_by(AsyncTask.created)
        .first()
    )

    if t:
        t.status = AsyncTaskStatus.PROCESSING
        t.processing_start_time = datetime.datetime.now()

    return t


def queue_task(_db, task_type, data: dict[str, str]) -> tuple[AsyncTask, bool]:
    """
    Add a new task of the specified task type to the task queue.
    If the task is a duplicate - ie the task data and task_type match an existing task in the
    READY state - the task returned will be the existing task.
    :param _db:
    :param task_type: The type of task
    :param data: The data associated with the task
    :return: Tuple containing a task and a boolean flag indicating whether a new task was created.
    """
    # does an unprocessed task like this already exist?
    t = (
        _db.query(AsyncTask)
        .filter(AsyncTask.task_type == task_type)
        .filter(AsyncTask.status == AsyncTaskStatus.READY)
        .filter(AsyncTask.data.cast(String) == json.dumps(data))
        .first()
    )

    if t:
        return t, False
    else:
        return create(
            _db, AsyncTask, task_type=task_type, status=AsyncTaskStatus.READY, data=data
        )


@dataclass
class InventoryReportTaskData:
    admin_id: int
    library_id: int
    admin_email: str
