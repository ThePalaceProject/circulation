# Cached Marc Files
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, DateTime, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from core.model import Base

if TYPE_CHECKING:
    from core.model import Representation


class CachedMARCFile(Base):
    """A record that a MARC file has been created and cached for a particular lane."""

    __tablename__ = "cachedmarcfiles"
    id = Column(Integer, primary_key=True)

    # Every MARC file is associated with a library and a lane. If the
    # lane is null, the file is for the top-level WorkList.
    library_id = Column(Integer, ForeignKey("libraries.id"), nullable=False, index=True)

    lane_id = Column(Integer, ForeignKey("lanes.id"), nullable=True, index=True)

    # The representation for this file stores the URL where it was mirrored.
    representation_id = Column(
        Integer, ForeignKey("representations.id"), nullable=False
    )
    representation: Mapped[Representation] = relationship(
        "Representation", back_populates="marc_file"
    )

    start_time = Column(DateTime(timezone=True), nullable=True, index=True)
    end_time = Column(DateTime(timezone=True), nullable=True, index=True)
