"""SQLAlchemy model for tracking one-time startup tasks."""

from __future__ import annotations

import datetime
from enum import StrEnum, auto

from sqlalchemy import Column, DateTime, Enum as SaEnum, Unicode
from sqlalchemy.orm import Mapped

from palace.manager.sqlalchemy.model.base import Base


class StartupTaskState(StrEnum):
    """Possible states for a startup task record."""

    RUN = auto()
    MARKED = auto()


class StartupTask(Base):
    """Track which one-time startup tasks have been recorded.

    Each row represents a startup task that has been recorded during
    application initialization.  The ``key`` serves as both the primary
    key and the unique identifier that prevents the same task from being
    processed more than once.
    """

    __tablename__ = "startup_tasks"

    key: Mapped[str] = Column(Unicode, primary_key=True)
    recorded_at: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), nullable=False
    )
    state: Mapped[StartupTaskState] = Column(
        SaEnum(StartupTaskState),
        nullable=False,
    )
