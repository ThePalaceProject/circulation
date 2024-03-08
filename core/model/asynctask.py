# Async
import datetime
import json
from enum import Enum

from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.sql.functions import now
from sqlalchemy.types import Enum as SqlAlchemyEnum

from core.model import Base, create


class AsyncTaskStatus(str, Enum):
    READY = "ready"
    PROCESSING = "processing"
    SUCCESSFUL = "successful"
    FAILED = "failed"


class AsyncTaskType(Enum):
    INVENTORY_REPORT = "inventory-report"


class AsyncTask(Base):
    """An asynchronous task."""

    __tablename__ = "asynctasks"
    id = Column(Integer, primary_key=True)
    task_type = Column(SqlAlchemyEnum(AsyncTaskType), index=True, nullable=False)
    status = Column(SqlAlchemyEnum(AsyncTaskStatus), index=True, nullable=False)
    created = Column(DateTime, default=now(), nullable=False)
    processing_start_time = Column(DateTime, nullable=True)
    processing_end_time = Column(DateTime, nullable=True)
    failure_details = Column(String, nullable=True)
    data = Column(MutableDict.as_mutable(JSON), default={})

    def __repr__(self):
        return f"<{self.__class__.__name__}({repr(self.__dict__)})>"


def start_next_task(_db, task_type: str) -> AsyncTask | None:
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
