from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

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

from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier, isbn_for_identifier
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import minute_timestamp

if TYPE_CHECKING:
    from sqlalchemy.orm import Mapped

    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.library import Library


class PlaytimeEntry(Base):
    __tablename__ = "playtime_entries"

    id: Mapped[int] = Column(Integer, autoincrement=True, primary_key=True)

    # Even if related objects are deleted, we keep our row.
    identifier_id = Column(
        Integer,
        ForeignKey("identifiers.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
    )
    identifier: Mapped[Identifier | None] = relationship("Identifier", uselist=False)
    collection_id = Column(
        Integer,
        ForeignKey("collections.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
    )
    collection: Mapped[Collection | None] = relationship("Collection", uselist=False)
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
    )
    library: Mapped[Library | None] = relationship("Library", uselist=False)
    # Related objects can be deleted, so we keep string representation.
    identifier_str: Mapped[str] = Column(String, nullable=False)
    collection_name: Mapped[str] = Column(String, nullable=False)
    data_source_name: Mapped[str] = Column(String, nullable=False)
    library_name: Mapped[str] = Column(String, nullable=False)

    timestamp: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), nullable=False
    )
    total_seconds_played: Mapped[int] = Column(
        Integer,
        CheckConstraint(
            "total_seconds_played <= 60", name="max_total_seconds_played_constraint"
        ),
        nullable=False,
    )
    tracking_id: Mapped[str] = Column(String(64), nullable=False)
    processed: Mapped[bool] = Column(Boolean, default=False, nullable=False)

    loan_identifier: Mapped[str] = Column(String(40), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "tracking_id",
            "identifier_str",
            "collection_name",
            "library_name",
            name="unique_playtime_entry",
        ),
    )


class PlaytimeSummary(Base):
    __tablename__ = "playtime_summaries"

    id: Mapped[int] = Column(Integer, autoincrement=True, primary_key=True)

    # Even if related objects are deleted, we keep our row.
    identifier_id = Column(
        Integer,
        ForeignKey("identifiers.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    identifier: Mapped[Identifier | None] = relationship("Identifier", uselist=False)
    collection_id = Column(
        Integer,
        ForeignKey("collections.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    collection: Mapped[Collection | None] = relationship("Collection", uselist=False)
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", onupdate="CASCADE", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    library: Mapped[Library | None] = relationship("Library", uselist=False)
    # Related objects can be deleted, so we keep string representation.
    identifier_str: Mapped[str] = Column(String, nullable=False)
    collection_name: Mapped[str] = Column(String, nullable=False)
    data_source_name: Mapped[str] = Column(String, nullable=False)
    library_name: Mapped[str] = Column(String, nullable=False)

    # This should be a per-minute datetime
    timestamp: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True),
        CheckConstraint(
            "extract(second from timestamp)::integer = 0",
            name="timestamp_minute_boundary_constraint",
        ),
        nullable=False,
    )

    total_seconds_played: Mapped[int] = Column(Integer, default=0, nullable=False)

    title = Column(String)
    isbn = Column(String)
    loan_identifier: Mapped[str] = Column(String(40), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "timestamp",
            "identifier_str",
            "collection_name",
            "library_name",
            "loan_identifier",
            name="unique_playtime_summary",
        ),
    )

    @classmethod
    def add(
        cls,
        _db: Session,
        ts: datetime.datetime,
        seconds: int,
        identifier: Identifier | None,
        collection: Collection | None,
        library: Library | None,
        identifier_str: str,
        collection_name: str,
        library_name: str | None,
        loan_identifier: str,
        data_source_name: str,
    ) -> PlaytimeSummary:
        """Add playtime (in seconds) to it's associated minute-level summary record."""
        # Update each label with its current value, if its foreign key is present.
        # Because a collection is sometimes renamed when marked for deletion, we
        # won't update the name in that case.
        if identifier:
            identifier_str = identifier.urn
        if collection and collection.name and not collection.marked_for_deletion:
            collection_name = collection.name
        if library and library.name:
            library_name = library.name

        # When the related identifier, collection, and/or library rows are available,
        # we'll use those to look up or create the summary row. If not, we'll use their
        # string labels to do so. The minute-level timestamp is always part of the key.
        _potential_lookup_keys = {
            "timestamp": minute_timestamp(ts),
            "identifier_id": identifier.id if identifier else None,
            "identifier_str": None if identifier else identifier_str,
            "collection_id": collection.id if collection else None,
            "collection_name": None if collection else collection_name,
            "library_id": library.id if library else None,
            "library_name": None if library else library_name,
            "loan_identifier": loan_identifier,
        }
        lookup_keys: dict[str, Any] = {
            k: v for k, v in _potential_lookup_keys.items() if v is not None
        }
        additional_columns = {
            k: v
            for k, v in {
                "identifier_str": identifier_str,
                "collection_name": collection_name,
                "library_name": library_name,
                "data_source_name": data_source_name,
            }.items()
            if k not in lookup_keys
        }

        # Ensure the row exists
        playtime, _ = get_one_or_create(
            _db,
            cls,
            create_method_kwargs=additional_columns,
            **lookup_keys,
        )

        # Set the label values, in case they weren't used to create the summary row.
        playtime.identifier_str = identifier_str
        playtime.collection_name = collection_name
        playtime.library_name = library_name

        # Set ISBN and title, if needed and possible.
        if (not playtime.isbn or not playtime.title) and not identifier:
            identifier, _ = Identifier.parse_urn(_db, identifier_str, autocreate=False)
        if not playtime.isbn and identifier:
            playtime.isbn = isbn_for_identifier(identifier)
        if not playtime.title and identifier:
            playtime.title = _title_for_identifier(identifier)

        # Race condition safe update
        _db.query(cls).filter(cls.id == playtime.id).update(
            {"total_seconds_played": cls.total_seconds_played + seconds}
        )
        _db.refresh(playtime)
        return playtime


def _title_for_identifier(identifier: Identifier | None) -> str | None:
    """Find the strongest title match for the given identifier.

    :param identifier: The identifier to match.
    :return: The title string associated with the identifier or None, if no match is found.
    """
    if identifier is None:
        return None
    db = Session.object_session(identifier)
    if (
        edition := db.query(Edition)
        .filter(Edition.primary_identifier == identifier)
        .first()
    ):
        return edition.title  # type: ignore[no-any-return]
    return None
