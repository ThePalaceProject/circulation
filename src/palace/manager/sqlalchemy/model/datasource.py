# DataSource
from __future__ import annotations

from collections import defaultdict
from collections.abc import Generator
from typing import TYPE_CHECKING, Literal, overload

from sqlalchemy import Boolean, Column, Integer, String, or_
from sqlalchemy.orm import Mapped, Query, Session, relationship

from palace.manager.sqlalchemy.constants import DataSourceConstants, IdentifierConstants
from palace.manager.sqlalchemy.hassessioncache import HasSessionCache
from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.util import get_one, get_one_or_create

if TYPE_CHECKING:
    # This is needed during type checking so we have the
    # types of related models.
    from palace.manager.sqlalchemy.model.classification import Classification
    from palace.manager.sqlalchemy.model.coverage import CoverageRecord
    from palace.manager.sqlalchemy.model.credential import Credential
    from palace.manager.sqlalchemy.model.customlist import CustomList
    from palace.manager.sqlalchemy.model.edition import Edition
    from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
    from palace.manager.sqlalchemy.model.lane import Lane
    from palace.manager.sqlalchemy.model.licensing import (
        LicensePool,
        LicensePoolDeliveryMechanism,
    )
    from palace.manager.sqlalchemy.model.measurement import Measurement
    from palace.manager.sqlalchemy.model.resource import Hyperlink, Resource


class DataSource(Base, HasSessionCache, DataSourceConstants):
    """A source for information about books, and possibly the books themselves."""

    __tablename__ = "datasources"
    id: Mapped[int] = Column(Integer, primary_key=True)
    name: Mapped[str] = Column(String, unique=True, index=True, nullable=False)

    @property
    def active_name(self) -> str:
        """
        The name of this DataSource, normalized to replace any
        deprecated names with their current equivalents.
        """
        return self.DEPRECATED_NAMES.get(self.name, self.name)

    offers_licenses: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    primary_identifier_type = Column(String, index=True)

    # One DataSource can generate many Editions.
    editions: Mapped[list[Edition]] = relationship(
        "Edition", back_populates="data_source", uselist=True
    )

    # One DataSource can generate many CoverageRecords.
    coverage_records: Mapped[list[CoverageRecord]] = relationship(
        "CoverageRecord", back_populates="data_source"
    )

    # One DataSource can generate many IDEquivalencies.
    id_equivalencies: Mapped[list[Equivalency]] = relationship(
        "Equivalency", back_populates="data_source"
    )

    # One DataSource can grant access to many LicensePools.
    license_pools: Mapped[list[LicensePool]] = relationship(
        "LicensePool", back_populates="data_source", overlaps="delivery_mechanisms"
    )

    # One DataSource can provide many Hyperlinks.
    links: Mapped[list[Hyperlink]] = relationship(
        "Hyperlink", back_populates="data_source"
    )

    # One DataSource can provide many Resources.
    resources: Mapped[list[Resource]] = relationship(
        "Resource", back_populates="data_source"
    )

    # One DataSource can generate many Measurements.
    measurements: Mapped[list[Measurement]] = relationship(
        "Measurement", back_populates="data_source"
    )

    # One DataSource can provide many Classifications.
    classifications: Mapped[list[Classification]] = relationship(
        "Classification", back_populates="data_source"
    )

    # One DataSource can have many associated Credentials.
    credentials: Mapped[list[Credential]] = relationship(
        "Credential", back_populates="data_source"
    )

    # One DataSource can generate many CustomLists.
    custom_lists: Mapped[list[CustomList]] = relationship(
        "CustomList", back_populates="data_source"
    )

    # One DataSource can provide many LicensePoolDeliveryMechanisms.
    delivery_mechanisms: Mapped[list[LicensePoolDeliveryMechanism]] = relationship(
        "LicensePoolDeliveryMechanism",
        back_populates="data_source",
        foreign_keys="LicensePoolDeliveryMechanism.data_source_id",
    )

    license_lanes: Mapped[list[Lane]] = relationship(
        "Lane",
        back_populates="license_datasource",
        foreign_keys="Lane.license_datasource_id",
    )

    list_lanes: Mapped[list[Lane]] = relationship(
        "Lane",
        back_populates="_list_datasource",
        foreign_keys="Lane._list_datasource_id",
    )

    metadata_lookups_by_identifier_type: defaultdict[str | None, list[str]]

    def __repr__(self) -> str:
        return '<DataSource: name="%s">' % (self.name)

    def cache_key(self) -> str:
        return self.name

    @classmethod
    @overload
    def lookup(
        cls,
        _db: Session,
        name: str,
        autocreate: Literal[True],
        offers_licenses: bool = ...,
        primary_identifier_type: str | None = ...,
    ) -> DataSource: ...

    @classmethod
    @overload
    def lookup(
        cls,
        _db: Session,
        name: str,
        autocreate: bool = ...,
        offers_licenses: bool = ...,
        primary_identifier_type: str | None = ...,
    ) -> DataSource | None: ...

    @classmethod
    def lookup(
        cls,
        _db: Session,
        name: str,
        autocreate: bool = False,
        offers_licenses: bool = False,
        primary_identifier_type: str | None = None,
    ) -> DataSource | None:
        # Turn a deprecated name (e.g. "3M" into the current name
        # (e.g. "Bibliotheca").

        if name in cls.DEPRECATED_NAMES:
            primary_name = cls.DEPRECATED_NAMES[name]
            secondary_name = name
        elif name in cls.DEPRECATED_NAMES.inverse:
            primary_name = name
            secondary_name = cls.DEPRECATED_NAMES.inverse[name]
        else:
            primary_name = name
            secondary_name = None

        def lookup_hook() -> tuple[DataSource | None, bool]:
            """There was no such DataSource in the cache. Look one up or
            create one.
            """
            constraint = (
                DataSource.name == primary_name
                if secondary_name is None
                else or_(
                    DataSource.name == primary_name, DataSource.name == secondary_name
                )
            )
            data_source: DataSource | None
            if autocreate:
                data_source, is_new = get_one_or_create(
                    _db,
                    DataSource,
                    constraint=constraint,
                    create_method_kwargs=dict(
                        name=primary_name,
                        offers_licenses=offers_licenses,
                        primary_identifier_type=primary_identifier_type,
                    ),
                )
            else:
                data_source = get_one(_db, DataSource, constraint=constraint)
                is_new = False
            return data_source, is_new

        # Look up the DataSource in the full-table cache, falling back
        # to the database if necessary.
        obj, is_new = cls.by_cache_key(_db, primary_name, lookup_hook)
        return obj

    URI_PREFIX = "http://librarysimplified.org/terms/sources/"

    @classmethod
    def license_source_for(
        cls, _db: Session, identifier: Identifier | str
    ) -> DataSource | None:
        """Find the one DataSource that provides licenses for books identified
        by the given identifier.
        If there is no such DataSource, or there is more than one,
        raises an exception.
        """
        sources = cls.license_sources_for(_db, identifier)
        return sources.one()

    @classmethod
    def license_sources_for(
        cls, _db: Session, identifier: Identifier | str
    ) -> Query[DataSource]:
        """A query that locates all DataSources that provide licenses for
        books identified by the given identifier.
        """
        type = identifier if isinstance(identifier, str) else identifier.type
        q = (
            _db.query(DataSource)
            .filter(DataSource.offers_licenses == True)
            .filter(DataSource.primary_identifier_type == type)
        )
        return q

    @classmethod
    def metadata_sources_for(
        cls, _db: Session, identifier: Identifier | str
    ) -> list[DataSource]:
        """Finds the DataSources that provide metadata for books
        identified by the given identifier.
        """
        type = identifier if isinstance(identifier, str) else identifier.type
        if not hasattr(cls, "metadata_lookups_by_identifier_type"):
            # This should only happen during testing.
            list(DataSource.well_known_sources(_db))

        names = cls.metadata_lookups_by_identifier_type[type]
        return _db.query(DataSource).filter(DataSource.name.in_(names)).all()

    @classmethod
    def well_known_sources(cls, _db: Session) -> Generator[DataSource]:
        """Make sure all the well-known sources exist in the database."""

        cls.metadata_lookups_by_identifier_type = defaultdict(list)

        for (
            name,
            offers_licenses,
            offers_metadata_lookup,
            primary_identifier_type,
            refresh_rate,
        ) in (
            (cls.GUTENBERG, True, False, IdentifierConstants.GUTENBERG_ID, None),
            (cls.OVERDRIVE, True, False, IdentifierConstants.OVERDRIVE_ID, 0),
            (
                cls.BIBLIOTHECA,
                True,
                False,
                IdentifierConstants.BIBLIOTHECA_ID,
                60 * 60 * 6,
            ),
            (cls.BOUNDLESS, True, False, IdentifierConstants.AXIS_360_ID, 0),
            (cls.OCLC, False, False, None, None),
            (cls.OCLC_LINKED_DATA, False, False, None, None),
            (cls.AMAZON, False, False, None, None),
            (cls.OPEN_LIBRARY, False, False, IdentifierConstants.OPEN_LIBRARY_ID, None),
            (
                cls.GUTENBERG_COVER_GENERATOR,
                False,
                False,
                IdentifierConstants.GUTENBERG_ID,
                None,
            ),
            (
                cls.GUTENBERG_EPUB_GENERATOR,
                False,
                False,
                IdentifierConstants.GUTENBERG_ID,
                None,
            ),
            (cls.WEB, True, False, IdentifierConstants.URI, None),
            (cls.VIAF, False, False, None, None),
            (cls.CONTENT_CAFE, True, True, IdentifierConstants.ISBN, None),
            (cls.MANUAL, False, False, None, None),
            (cls.NYT, False, False, IdentifierConstants.ISBN, None),
            (cls.LIBRARY_STAFF, False, False, None, None),
            (cls.METADATA_WRANGLER, False, False, None, None),
            (
                cls.PROJECT_GITENBERG,
                True,
                False,
                IdentifierConstants.GUTENBERG_ID,
                None,
            ),
            (cls.STANDARD_EBOOKS, True, False, IdentifierConstants.URI, None),
            (cls.UNGLUE_IT, True, False, IdentifierConstants.URI, None),
            (cls.ADOBE, False, False, None, None),
            (cls.PLYMPTON, True, False, IdentifierConstants.ISBN, None),
            (cls.ELIB, True, False, IdentifierConstants.ELIB_ID, None),
            (cls.OA_CONTENT_SERVER, True, False, None, None),
            (cls.NOVELIST, False, True, IdentifierConstants.NOVELIST_ID, None),
            (cls.PRESENTATION_EDITION, False, False, None, None),
            (cls.INTERNAL_PROCESSING, False, False, None, None),
            (cls.FEEDBOOKS, True, False, IdentifierConstants.URI, None),
            (
                cls.BIBBLIO,
                False,
                True,
                IdentifierConstants.BIBBLIO_CONTENT_ITEM_ID,
                None,
            ),
            (cls.PROQUEST, True, False, IdentifierConstants.PROQUEST_ID, None),
        ):
            obj = DataSource.lookup(
                _db,
                name,
                autocreate=True,
                offers_licenses=offers_licenses,
                primary_identifier_type=primary_identifier_type,
            )

            if offers_metadata_lookup:
                l = cls.metadata_lookups_by_identifier_type[primary_identifier_type]
                l.append(obj.name)

            yield obj
