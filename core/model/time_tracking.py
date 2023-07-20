from __future__ import annotations

import datetime
import logging
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
    timestamp: Mapped[DateTime] = Column(DateTime, nullable=False)
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
    timestamp = Column(
        DateTime,
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
    def add(cls, playtime_entry: IdentifierPlaytimeEntry) -> None:
        if playtime_entry.processed is True:
            logging.getLogger("IdentifierPlaytime").info(
                f"PlaytimeEntry is already processed {playtime_entry.identifier.urn} | {playtime_entry.tracking_id}"
            )
            return

        db = Session.object_session(playtime_entry)
        identifier = playtime_entry.identifier
        ts = playtime_entry.timestamp
        timestamp = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute)
        playtime, created = get_one_or_create(
            db,
            cls,
            timestamp=timestamp,
            identifier_id=identifier.id,
        )

        if created:
            playtime.identifier_str = identifier.urn

        playtime.total_seconds_played.op("+")(playtime_entry.total_seconds_played)
        playtime_entry.processed = True
