"""SQLAlchemy model for tracking one-time startup tasks."""

from __future__ import annotations

import datetime

from sqlalchemy import Boolean, Column, DateTime, Unicode
from sqlalchemy.orm import Mapped

from palace.manager.sqlalchemy.model.base import Base


class StartupTask(Base):
    """Track which one-time startup tasks have been queued.

    Each row represents a startup task that has been recorded during
    application initialization.  The ``key`` serves as both the primary
    key and the unique identifier that prevents the same task from being
    processed more than once.
    """

    __tablename__ = "startup_tasks"

    key: Mapped[str] = Column(Unicode, primary_key=True)
    queued_at: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), nullable=False
    )
    run: Mapped[bool] = Column(Boolean, nullable=False)
