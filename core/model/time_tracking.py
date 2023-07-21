from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Session, relationship

from core.model import Base, get_one_or_create

if TYPE_CHECKING:
    from sqlalchemy.orm import Mapped

    from core.model import Identifier


class IdentifierPlaytimeEntry(Base):
    __tablename__ = "identifier_playtime_entries"

    id = Column(Integer, autoincrement=True, primary_key=True)

    identifier_id = Column(
        Integer,
        ForeignKey("identifiers.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[DateTime] = Column(DateTime(timezone=True), nullable=False)
    total_seconds_played = Column(
        Integer,
        CheckConstraint(
            "total_seconds_played <= 60", name="max_total_seconds_played_constraint"
        ),
        nullable=False,
    )
    tracking_id = Column(String(64), nullable=False)
    processed = Column(Boolean, default=False)

    identifier: Mapped[Identifier] = relationship("Identifier", uselist=False)

    __table_args__ = (UniqueConstraint("identifier_id", "tracking_id"),)


class IdentifierPlaytime(Base):
    __tablename__ = "identifier_playtimes"

    id = Column(Integer, autoincrement=True, primary_key=True)

    identifier_id = Column(
        Integer,
        ForeignKey("identifiers.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    # In case an identifier is deleted, we should not delete "analytics" on it
    # so we store the identifier "string" as well. This should be an identifier.urn string
    identifier_str = Column(String, nullable=False)

    # This should be a per-minute datetime
    timestamp: Mapped[DateTime] = Column(
        DateTime(timezone=True),
        CheckConstraint(
            "extract(second from timestamp)::integer = 0",
            name="timestamp_minute_boundary_constraint",
        ),
        nullable=False,
    )

    total_seconds_played = Column(Integer, default=0)

    identifier: Mapped[Identifier] = relationship("Identifier", uselist=False)

    __table_args__ = (UniqueConstraint("identifier_str", "timestamp"),)

    @classmethod
    def add(
        cls, identifier: Identifier, ts: datetime.datetime, seconds: int
    ) -> IdentifierPlaytime:
        """Add playtime in seconds to an identifier for a minute-timestamp"""
        _db = Session.object_session(identifier)
        # Sanitize the timestamp to a minute boundary
        timestamp = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute)

        # Ensure the row exists
        playtime, _ = get_one_or_create(
            _db,
            cls,
            timestamp=timestamp,
            identifier_id=identifier.id,
            create_method_kwargs={
                "identifier_str": identifier.urn,
            },
        )

        # Race condition safe update
        _db.query(cls).filter(cls.id == playtime.id).update(
            {"total_seconds_played": cls.total_seconds_played + seconds}
        )
        _db.refresh(playtime)
        return playtime
