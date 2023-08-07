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
    from core.model.collection import Collection
    from core.model.library import Library


class PlaytimeEntry(Base):
    __tablename__ = "playtime_entries"

    id = Column(Integer, autoincrement=True, primary_key=True)

    identifier_id = Column(
        Integer,
        ForeignKey("identifiers.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    collection_id = Column(
        Integer,
        ForeignKey("collections.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), nullable=False
    )
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
    collection: Mapped[Collection] = relationship("Collection", uselist=False)
    library: Mapped[Library] = relationship("Library", uselist=False)

    __table_args__ = (
        UniqueConstraint("identifier_id", "collection_id", "library_id", "tracking_id"),
    )


class PlaytimeSummary(Base):
    __tablename__ = "playtime_summaries"

    id = Column(Integer, autoincrement=True, primary_key=True)

    identifier_id = Column(
        Integer,
        ForeignKey("identifiers.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    collection_id = Column(
        Integer,
        ForeignKey("collections.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=False,
        index=True,
    )
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=False,
        index=True,
    )
    # In case an identifier is deleted, we should not delete "analytics" on it
    # so we store the identifier "string" as well. This should be an identifier.urn string
    # The same logic applies to collection, and library
    identifier_str = Column(String, nullable=False)
    collection_name = Column(String, nullable=False)
    library_name = Column(String, nullable=False)

    # This should be a per-minute datetime
    timestamp: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True),
        CheckConstraint(
            "extract(second from timestamp)::integer = 0",
            name="timestamp_minute_boundary_constraint",
        ),
        nullable=False,
    )

    total_seconds_played = Column(Integer, default=0)

    identifier: Mapped[Identifier] = relationship("Identifier", uselist=False)
    collection: Mapped[Collection] = relationship("Collection", uselist=False)
    library: Mapped[Library] = relationship("Library", uselist=False)

    __table_args__ = (
        UniqueConstraint(
            "identifier_str", "collection_name", "library_name", "timestamp"
        ),
    )

    @classmethod
    def add(
        cls,
        identifier: Identifier,
        collection: Collection,
        library: Library,
        ts: datetime.datetime,
        seconds: int,
    ) -> PlaytimeSummary:
        """Add playtime in seconds to a summary record for a minute-timestamp"""
        _db = Session.object_session(identifier)
        # Sanitize the timestamp to a minute boundary
        timestamp = datetime.datetime(ts.year, ts.month, ts.day, ts.hour, ts.minute)

        # Ensure the row exists
        playtime, _ = get_one_or_create(
            _db,
            cls,
            timestamp=timestamp,
            identifier_id=identifier.id,
            collection_id=collection.id,
            library_id=library.id,
            create_method_kwargs={
                "identifier_str": identifier.urn,
                "collection_name": collection.name,
                "library_name": library.name,
            },
        )

        # Race condition safe update
        _db.query(cls).filter(cls.id == playtime.id).update(
            {"total_seconds_played": cls.total_seconds_played + seconds}
        )
        _db.refresh(playtime)
        return playtime
