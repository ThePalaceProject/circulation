from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy.orm import Session


class SessionMixin(ABC):
    @property
    @abstractmethod
    def session_maker(self):
        """
        The session maker to use when creating sessions.
        """

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """
        Starts a session and yields it to the caller. The session is closed
        when the context manager exits.

        See: https://docs.sqlalchemy.org/en/20/orm/session_basics.html#opening-and-closing-a-session
        """
        with self.session_maker() as session:
            yield session

    @contextmanager
    def transaction(self) -> Generator[Session, None, None]:
        """
        Start a new transaction and yield a session to the caller. The transaction will be
        committed when the context manager exits. If an exception is raised, the transaction
        will be rolled back.

        See: https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.sessionmaker.begin
        """
        with self.session_maker.begin() as session:
            yield session
