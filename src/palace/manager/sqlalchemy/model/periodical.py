# Periodical, PeriodicalVolume
from __future__ import annotations

from enum import StrEnum, auto
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    DateTime,
    Enum as AlchemyEnum,
    ForeignKey,
    Integer,
    Unicode,
)
from sqlalchemy.orm import Mapped, relationship

from palace.util.datetime_helpers import utc_now

from palace.manager.sqlalchemy.model.base import Base

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.datasource import DataSource
    from palace.manager.sqlalchemy.model.edition import Edition
    from palace.manager.sqlalchemy.model.identifier import Identifier
    from palace.manager.sqlalchemy.model.resource import Resource


class SerialType(StrEnum):
    """What kind of serial publication a Periodical is."""

    MAGAZINE = auto()
    JOURNAL = auto()
    NEWSPAPER = auto()

    PERIODICAL = auto()
    # We know it comes out in issues, but not what kind of thing it is.


class Periodical(Base):
    """A serial publication: the magazine itself, not one of its issues.

    A Periodical is deliberately not a Work. There is nothing to lend here --
    no LicensePool, no delivery mechanism, no loan. Those belong to the
    individual issues, which are Editions. A Periodical holds only what stays
    the same from one issue to the next.

    A Periodical is identified by its Identifier, normally an ISSN. When it
    has no Identifier, fall back to its data source and its normalized
    sort title. The data source is only ever a fallback: when an Identifier is
    present it plays no part in deciding whether two rows are the same
    magazine, so the same ISSN from two sources is one Periodical.
    """

    __tablename__ = "periodicals"

    id: Mapped[int] = Column(Integer, primary_key=True)

    # The identifier this periodical is known by, normally an ISSN. Nullable,
    # because plenty of feeds hand us a magazine with no ISSN at all.
    identifier_id = Column(
        Integer, ForeignKey("identifiers.id"), index=True, nullable=True
    )
    identifier: Mapped[Identifier | None] = relationship(
        "Identifier", back_populates="periodicals"
    )

    serial_type = Column(
        AlchemyEnum(SerialType, values_callable=lambda obj: [e.value for e in obj]),
        nullable=True,
    )

    title = Column(Unicode)
    # Indexed because a periodical with no identifier is looked up by its
    # data source and its sort title.
    sort_title = Column(Unicode, index=True)

    publisher = Column(Unicode)
    language = Column(Unicode)
    description = Column(Unicode)

    cover_id = Column(
        Integer,
        ForeignKey("resources.id", use_alter=True, name="fk_periodicals_cover_id"),
        index=True,
    )
    cover: Mapped[Resource | None] = relationship(
        "Resource", back_populates="cover_periodicals", foreign_keys=[cover_id]
    )

    # Where this periodical's metadata came from. This records provenance and
    # nothing more: see the class docstring on how identity works.
    data_source_id = Column(
        Integer, ForeignKey("datasources.id"), index=True, nullable=True
    )
    data_source: Mapped[DataSource | None] = relationship(
        "DataSource", back_populates="periodicals"
    )

    created = Column(DateTime(timezone=True), default=utc_now)
    updated = Column(DateTime(timezone=True), default=None)

    volumes: Mapped[list[PeriodicalVolume]] = relationship(
        "PeriodicalVolume", back_populates="periodical", uselist=True
    )

    # The issues of this periodical.
    editions: Mapped[list[Edition]] = relationship(
        "Edition", back_populates="periodical", uselist=True
    )

    def __repr__(self) -> str:
        return (
            f"<Periodical id={self.id} title={self.title!r} "
            f"identifier_id={self.identifier_id}>"
        )


class PeriodicalVolume(Base):
    """A group of issues within a Periodical: a volume, a year, a season."""

    __tablename__ = "periodical_volumes"

    id: Mapped[int] = Column(Integer, primary_key=True)

    periodical_id: Mapped[int] = Column(
        Integer, ForeignKey("periodicals.id"), index=True, nullable=False
    )
    periodical: Mapped[Periodical] = relationship(
        "Periodical", back_populates="volumes"
    )

    # What to call this volume on screen, for example "Volume 12" or "2024".
    label = Column(Unicode)

    # Where this volume sorts within its periodical. Lower comes first.
    position = Column(Integer)

    created = Column(DateTime(timezone=True), default=utc_now)
    updated = Column(DateTime(timezone=True), default=None)

    # The issues that make up this volume.
    editions: Mapped[list[Edition]] = relationship(
        "Edition", back_populates="periodical_volume", uselist=True
    )

    def __repr__(self) -> str:
        return (
            f"<PeriodicalVolume id={self.id} periodical_id={self.periodical_id} "
            f"label={self.label!r}>"
        )
