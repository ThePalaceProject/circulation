from __future__ import annotations

from abc import ABC, abstractmethod

from sqlalchemy.orm import Session, sessionmaker

from core.celery.session import SessionMixin
from core.util.log import LoggerMixin


class Job(LoggerMixin, SessionMixin, ABC):
    """
    Base class for all our Celery jobs.

    This class provides a few helper methods for our jobs to use, such as
    a logger and a session context manager. This class  is and should remain
    runable outside the context of a Celery worker. That way we are able to
    test our jobs fully outside the Celery worker.

    This class purposefully does not open a SqlAlchemy session for the job,
    preferring to let the job open and close the session as needed. This
    allows a long-running job to open and close the session as needed rather
    than keeping the session open for the entire duration of the job.

    Because our default Celery configuration is setup to ack tasks after they
    are completed, if a worker dies while processing a task, the task will be
    requeued and run again. We need to keep this in mind when writing our jobs
    to ensure that they are idempotent and can be run multiple times without
    causing any issues.
    """

    def __init__(self, session_maker: sessionmaker[Session]):
        """
        Initialize the job with a session maker, when running in the context
        of a Task, this will come directly from the Task.
        """
        self._session_maker = session_maker

    @property
    def session_maker(self) -> sessionmaker[Session]:
        """
        A session maker for the job to use when creating sessions.

        This should generally be accessed via the `session` or `transaction`
        context managers defined in `SessionMixin`.
        """
        return self._session_maker

    @abstractmethod
    def run(self) -> None:
        """
        Implement this method in your subclass to define the work that the job should do.
        """

        ...
