from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy.orm import Session, sessionmaker

from core.util.log import LoggerMixin


class Job(LoggerMixin, ABC):
    def __init__(self, session_maker: sessionmaker[Session]):
        self._session_maker = session_maker

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        with self._session_maker() as session:
            yield session

    @contextmanager
    def transaction(self) -> Generator[Session, None, None]:
        with self._session_maker.begin() as session:
            yield session

    @abstractmethod
    def run(self) -> None:
        ...
