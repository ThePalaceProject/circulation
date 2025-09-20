from __future__ import annotations

import datetime
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime, ForeignKey, Integer, Unicode
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, relationship

from palace.manager.sqlalchemy.model.base import Base

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.library import Library


class MarcFile(Base):
    """A record that a MARC file has been created and cached for a particular library and collection."""

    __tablename__ = "marcfiles"
    id: Mapped[uuid.UUID] = Column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )

    # The library should never be null in normal operation, but if a library is deleted, we don't want to lose the
    # record of the MARC file, so we set the library to null.
    library_id = Column(
        Integer,
        ForeignKey("libraries.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    library: Mapped[Library | None] = relationship("Library")

    # The collection should never be null in normal operation, but similar to the library, if a collection is deleted,
    # we don't want to lose the record of the MARC file, so we set the collection to null.
    collection_id = Column(
        Integer,
        ForeignKey("collections.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    collection: Mapped[Collection | None] = relationship("Collection")

    # The key in s3 used to store the file.
    key: Mapped[str] = Column(Unicode, nullable=False)

    # The creation date of the file.
    created: Mapped[datetime.datetime] = Column(
        DateTime(timezone=True), nullable=False, index=True
    )

    # If the file is a delta, the date of the previous file. If the file is a full file, null.
    since = Column(DateTime(timezone=True), nullable=True)
