from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy.orm import Session, sessionmaker

from core.util.log import LoggerMixin


class Job(LoggerMixin, ABC):
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

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Starts a session and yields it to the caller. The session is closed
        when the context manager exits.

        See: https://docs.sqlalchemy.org/en/20/orm/session_basics.html#opening-and-closing-a-session
        """
        with self._session_maker() as session:
            yield session

    @contextmanager
    def transaction(self) -> Generator[Session, None, None]:
        """
        Start a new transaction and yield a session to the caller. The transaction will be
        committed when the context manager exits. If an exception is raised, the transaction
        will be rolled back.

        See: https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.sessionmaker.begin
        """
        with self._session_maker.begin() as session:
            yield session

    @abstractmethod
    def run(self) -> None:
        """
        Implement this method in your subclass to define the work that the job should do.
        """

        ...
