from __future__ import annotations

import celery
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from core.model import SessionManager
from core.service.container import Services, container_instance
from core.util.log import LoggerMixin


class Task(celery.Task, LoggerMixin):
    _session_maker = None

    @property
    def session_maker(self) -> sessionmaker[Session]:
        if self._session_maker is None:
            engine = SessionManager.engine(poolclass=NullPool)
            maker = sessionmaker(bind=engine)
            SessionManager.setup_event_listener(maker)
            self._session_maker = maker
        return self._session_maker

    @property
    def services(self) -> Services:
        return container_instance()
