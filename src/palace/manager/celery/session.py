from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy.orm import Session, sessionmaker


class SessionMixin(ABC):
    @property
    @abstractmethod
    def session_maker(self) -> sessionmaker[Session]:
        """
        The session maker to use when creating sessions. Generally this should be accessed
        via the `session` or `transaction` context managers defined below.
        """

    @contextmanager
    def session(self) -> Generator[Session]:
        """
        Starts a session and yields it to the caller. The session is closed
        when the context manager exits.

        See: https://docs.sqlalchemy.org/en/20/orm/session_basics.html#opening-and-closing-a-session
        """
        with self.session_maker() as session:
            yield session

    @contextmanager
    def transaction(self) -> Generator[Session]:
        """
        Start a new transaction and yield a session to the caller. The transaction will be
        committed when the context manager exits. If an exception is raised, the transaction
        will be rolled back.

        See: https://docs.sqlalchemy.org/en/20/orm/session_api.html#sqlalchemy.orm.sessionmaker.begin
        """
        with self.session_maker.begin() as session:
            yield session
