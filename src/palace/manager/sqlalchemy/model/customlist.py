# CustomList, CustomListEntry
from __future__ import annotations

import logging
from functools import total_ordering
from typing import TYPE_CHECKING

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Table,
    Unicode,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.expression import or_, select

from palace.manager.sqlalchemy.model.base import Base
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now

if TYPE_CHECKING:
    from palace.manager.sqlalchemy.model.collection import Collection
    from palace.manager.sqlalchemy.model.lane import Lane
    from palace.manager.sqlalchemy.model.library import Library


@total_ordering
class CustomList(Base):
    """A custom grouping of Editions."""

    STAFF_PICKS_NAME = "Staff Picks"

    INIT = "init"
    UPDATED = "updated"
    REPOPULATE = "repopulate"

    __tablename__ = "customlists"
    id: Mapped[int] = Column(Integer, primary_key=True)
    primary_language = Column(Unicode, index=True)
    data_source_id = Column(Integer, ForeignKey("datasources.id"), index=True)
    data_source: Mapped[DataSource | None] = relationship(
        "DataSource", back_populates="custom_lists"
    )
    foreign_identifier = Column(Unicode, index=True)
    name = Column(Unicode, index=True)
    description = Column(Unicode)
    created = Column(DateTime(timezone=True), index=True)
    updated = Column(DateTime(timezone=True), index=True)
    responsible_party = Column(Unicode)
    library_id = Column(Integer, ForeignKey("libraries.id"), index=True, nullable=True)
    library: Mapped[Library | None] = relationship(
        "Library", back_populates="custom_lists"
    )

    # How many titles are in this list? This is calculated and
    # cached when the list contents change.
    size: Mapped[int] = Column(Integer, nullable=False, default=0)

    entries: Mapped[list[CustomListEntry]] = relationship(
        "CustomListEntry", back_populates="customlist", uselist=True
    )

    # List sharing mechanisms
    shared_locally_with_libraries: Mapped[list[Library]] = relationship(
        "Library",
        secondary="customlist_sharedlibraries",
        back_populates="shared_custom_lists",
        uselist=True,
    )

    auto_update_enabled: Mapped[bool] = Column(Boolean, default=False, nullable=False)
    auto_update_query = Column(Unicode, nullable=True)  # holds json data
    auto_update_facets = Column(Unicode, nullable=True)  # holds json data
    auto_update_last_update = Column(DateTime, nullable=True)
    auto_update_status: Mapped[str] = Column(
        Enum(INIT, UPDATED, REPOPULATE, name="auto_update_status"),
        default=INIT,
        nullable=False,
    )

    collections: Mapped[list[Collection]] = relationship(
        "Collection", secondary="collections_customlists", back_populates="customlists"
    )

    lane: Mapped[list[Lane]] = relationship(
        "Lane", secondary="lanes_customlists", back_populates="customlists"
    )

    __table_args__ = (
        UniqueConstraint("data_source_id", "foreign_identifier"),
        UniqueConstraint("name", "library_id"),
    )

    # TODO: It should be possible to associate a CustomList with an
    # audience, fiction status, and subject, but there is no planned
    # interface for managing this.

    def __repr__(self):
        return '<Custom List name="%s" foreign_identifier="%s" [%d entries]>' % (
            self.name,
            self.foreign_identifier,
            len(self.entries),
        )

    def __eq__(self, other):
        """Equality implementation for total_ordering."""
        if other is None or not isinstance(other, CustomList):
            return False
        return (self.foreign_identifier, self.name) == (
            other.foreign_identifier,
            other.name,
        )

    def __lt__(self, other):
        """Comparison implementation for total_ordering."""
        if other is None or not isinstance(other, CustomList):
            return False
        return (self.foreign_identifier, self.name) < (
            other.foreign_identifier,
            other.name,
        )

    @classmethod
    def entries_having_works(cls, _db: Session, list_id: int):
        return (
            _db.query(Work)
            .join(CustomListEntry)
            .join(Edition)
            .filter(CustomListEntry.list_id == list_id)
            .order_by(Edition.sort_title)
        )

    @classmethod
    def all_from_data_sources(cls, _db, data_sources):
        """All custom lists from the given data sources."""
        if not isinstance(data_sources, list):
            data_sources = [data_sources]
        ids = []
        for ds in data_sources:
            if isinstance(ds, (bytes, str)):
                ds = DataSource.lookup(_db, ds)
            ids.append(ds.id)
        return _db.query(CustomList).filter(CustomList.data_source_id.in_(ids))

    @classmethod
    def find(cls, _db, foreign_identifier_or_name, data_source=None, library=None):
        """Finds a foreign list in the database by its foreign_identifier
        or its name.
        """
        source_name = data_source
        if isinstance(data_source, DataSource):
            source_name = data_source.name
        foreign_identifier = str(foreign_identifier_or_name)

        qu = _db.query(cls)
        if source_name:
            qu = qu.join(CustomList.data_source).filter(
                DataSource.name == str(source_name)
            )

        qu = qu.filter(
            or_(
                CustomList.foreign_identifier == foreign_identifier,
                CustomList.name == foreign_identifier,
            )
        )
        if library:
            qu = qu.filter(CustomList.library_id == library.id)
        else:
            qu = qu.filter(CustomList.library_id == None)

        custom_lists = qu.all()

        if not custom_lists:
            return None
        return custom_lists[0]

    @property
    def featured_works(self):
        _db = Session.object_session(self)
        editions = [e.edition for e in self.entries if e.featured]
        if not editions:
            return None

        identifiers = [ed.primary_identifier for ed in editions]
        return Work.from_identifiers(_db, identifiers)

    def add_entry(
        self,
        work_or_edition,
        annotation=None,
        first_appearance=None,
        featured=None,
        update_external_index=True,
    ):
        """Add a Work or Edition to a CustomList.

        :param work_or_edition: A Work or an Edition. If this is a
          Work, that specific Work will be added to the CustomList. If
          this is an Edition, that Edition will be added to the
          CustomList, assuming there's no equivalent Edition already
          in the list.

        :param update_external_index: When a Work is added to a list,
          its external index needs to be updated. TODO: This
          is probably no longer be necessary since we no longer update the
          external index in real time.
        """
        first_appearance = first_appearance or utc_now()
        _db = Session.object_session(self)

        if isinstance(work_or_edition, Work):
            work = work_or_edition
            edition = work.presentation_edition

            # Don't look for duplicate entries. get_one_or_create will
            # find an existing entry for this Work, and any other Work
            # -- even for the same title -- is not considered a
            # 'duplicate'.
            existing_entries = []
        else:
            edition = work_or_edition
            work = edition.work

            # Look for other entries in this CustomList for this Edition,
            # or an equivalent Edition. This can avoid situations where
            # the same book shows up on a CustomList multiple times.
            existing_entries = list(self.entries_for_work(work_or_edition))

            # There's no guarantee this Edition _has_ a work, so don't
            # filter by Work when looking for a duplicate.
            kwargs = dict()

        if existing_entries:
            # There is a book equivalent to this one on the list.
            # Update one of the equivalent CustomListEntries,
            # potentially giving it a new .edition and .work
            was_new = False
            entry = existing_entries[0]
            if len(existing_entries) > 1:
                entry.update(_db, equivalent_entries=existing_entries[1:])
            entry.edition = edition
            entry.work = work
        else:
            # There is no equivalent book on the CustomList, but the
            # exact same book may already be on the list. Either find
            # an exact duplicate, or create a new entry.
            entry, was_new = get_one_or_create(
                _db,
                CustomListEntry,
                customlist=self,
                edition=edition,
                work=work,
                create_method_kwargs=dict(first_appearance=first_appearance),
            )

        if (
            not entry.most_recent_appearance
            or entry.most_recent_appearance < first_appearance
        ):
            entry.most_recent_appearance = first_appearance
        if annotation:
            entry.annotation = str(annotation)
        if work and not entry.work:
            entry.work = edition.work
        if featured is not None:
            entry.featured = featured

        if was_new:
            self.updated = utc_now()
            self.size += 1
        # Make sure the Work's search document is updated to reflect its new
        # list membership.
        if work and update_external_index:
            work.external_index_needs_updating()

        return entry, was_new

    def remove_entry(self, work_or_edition):
        """Remove the entry for a particular Work or Edition and/or any of its
        equivalent Editions.
        """
        _db = Session.object_session(self)

        existing_entries = list(self.entries_for_work(work_or_edition))
        for entry in existing_entries:
            if entry.work:
                # Make sure the Work's search document is updated to
                # reflect its new list membership.
                entry.work.external_index_needs_updating()

            _db.delete(entry)

        if existing_entries:
            self.updated = utc_now()
            self.size -= len(existing_entries)
        _db.commit()

    def entries_for_work(self, work_or_edition):
        """Find all of the entries in the list representing a particular
        Edition or Work.
        """
        if isinstance(work_or_edition, Work):
            work = work_or_edition
            edition = work_or_edition.presentation_edition
        else:
            edition = work_or_edition
            work = edition.work

        equivalent_ids = [x.id for x in edition.equivalent_editions()]

        _db = Session.object_session(work_or_edition)
        clauses = []
        if equivalent_ids:
            clauses.append(CustomListEntry.edition_id.in_(equivalent_ids))
        if work:
            clauses.append(CustomListEntry.work == work)
        if len(clauses) == 0:
            # This shouldn't happen, but if it does, there can be
            # no matching results.
            return _db.query(CustomListEntry).filter(False)
        elif len(clauses) == 1:
            clause = clauses[0]
        else:
            clause = or_(*clauses)

        qu = (
            _db.query(CustomListEntry)
            .filter(CustomListEntry.customlist == self)
            .filter(clause)
        )
        return qu

    def update_size(self, db: Session):
        if self.id is not None:
            self.size = CustomList.entries_having_works(db, self.id).count()

    def get_entry_count(self, db: Session) -> int:
        """Get the number of entries for this CustomList from the database.

        TODO: This query is similar to the one used in `update_size` (from
         `CustomListQueries.populate_query_pages`), but does not factor in
         the presence of Works or Editions. This query reproduces the
         behavior of `CustomListQueries.populate_query_pages`'s updated size
         calculation, but in a more efficient manner.
         I'm not sure which is the correct behavior, so this is left as is for now.
        """
        return db.execute(
            select(func.count()).where(CustomListEntry.list_id == self.id)
        ).scalar_one()


customlist_sharedlibrary: Table = Table(
    "customlist_sharedlibraries",
    Base.metadata,
    Column(
        "customlist_id",
        Integer,
        ForeignKey("customlists.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    ),
    Column(
        "library_id",
        Integer,
        ForeignKey("libraries.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    ),
    UniqueConstraint("customlist_id", "library_id"),
)


class CustomListEntry(Base):
    __tablename__ = "customlistentries"
    id: Mapped[int] = Column(Integer, primary_key=True)
    list_id = Column(Integer, ForeignKey("customlists.id"), index=True)
    customlist: Mapped[CustomList | None] = relationship(
        "CustomList", back_populates="entries"
    )

    edition_id = Column(Integer, ForeignKey("editions.id"), index=True)
    edition: Mapped[Edition | None] = relationship(
        "Edition", back_populates="custom_list_entries"
    )

    work_id = Column(Integer, ForeignKey("works.id"), index=True)
    work: Mapped[Work | None] = relationship(
        "Work", back_populates="custom_list_entries"
    )

    featured: Mapped[bool] = Column(Boolean, nullable=False, default=False)
    annotation = Column(Unicode)

    # These two fields are for best-seller lists. Even after a book
    # drops off the list, the fact that it once was on the list is
    # still relevant.
    first_appearance = Column(DateTime(timezone=True), index=True)
    most_recent_appearance = Column(DateTime(timezone=True), index=True)

    def set_work(self, metadata=None, policy=None):
        """If possible, identify a locally known Work that is the same
        title as the title identified by this CustomListEntry.

        :param policy: A PresentationCalculationPolicy, used to
           determine how far to go when looking for equivalent
           Identifiers.
        """
        _db = Session.object_session(self)
        edition = self.edition
        if not self.edition:
            # This shouldn't happen, but no edition means no work
            self.work = None
            return self.work

        new_work = None
        if not metadata:
            from palace.manager.data_layer.bibliographic import BibliographicData

            metadata = BibliographicData.from_edition(edition)

        # Try to guess based on metadata, if we can get a high-quality
        # guess.
        potential_license_pools = metadata.guess_license_pools(_db)
        for lp, quality in sorted(
            list(potential_license_pools.items()), key=lambda x: -x[1]
        ):
            if lp.deliverable and lp.work and quality >= 0.8:
                # This work has at least one deliverable LicensePool
                # associated with it, so it's likely to be real
                # data and not leftover junk.
                new_work = lp.work
                break

        if not new_work:
            # Try using the less reliable, more expensive method of
            # matching based on equivalent identifiers.
            equivalent_identifier_id_subquery = (
                Identifier.recursively_equivalent_identifier_ids_query(
                    self.edition.primary_identifier.id, policy=policy
                )
            )
            pool_q = (
                _db.query(LicensePool)
                .filter(
                    LicensePool.identifier_id.in_(equivalent_identifier_id_subquery)
                )
                .order_by(
                    LicensePool.licenses_available.desc(),
                    LicensePool.patrons_in_hold_queue.asc(),
                )
            )
            pools = [x for x in pool_q if x.deliverable]
            for pool in pools:
                if pool.deliverable and pool.work:
                    new_work = pool.work
                    break

        old_work = self.work
        if old_work != new_work:
            if old_work:
                logging.info(
                    "Changing work for list entry %r to %r (was %r)",
                    self.edition,
                    new_work,
                    old_work,
                )
            else:
                logging.info(
                    "Setting work for list entry %r to %r", self.edition, new_work
                )
        self.work = new_work
        return self.work

    def update(self, _db, equivalent_entries=None):
        """Combines any number of equivalent entries into a single entry
        and updates the edition being used to represent the Work.
        """
        work = None
        if not equivalent_entries:
            # There are no entries to compare against. Leave it be.
            return
        equivalent_entries += [self]
        equivalent_entries = list(set(equivalent_entries))

        # Confirm that all the entries are from the same CustomList.
        list_ids = {e.list_id for e in equivalent_entries}
        if not len(list_ids) == 1:
            raise ValueError("Cannot combine entries on different CustomLists.")

        # Confirm that all the entries are equivalent.
        error = "Cannot combine entries that represent different Works."
        equivalents = self.edition.equivalent_editions()
        for equivalent_entry in equivalent_entries:
            if equivalent_entry.edition not in equivalents:
                raise ValueError(error)

        # And get a Work if one exists.
        works = set()
        for e in equivalent_entries:
            work = e.edition.work
            if work:
                works.add(work)
        works = [w for w in works if w]

        if works:
            if not len(works) == 1:
                # This shouldn't happen, given all the Editions are equivalent.
                raise ValueError(error)
            [work] = works

        self.first_appearance = min(e.first_appearance for e in equivalent_entries)
        self.most_recent_appearance = max(
            e.most_recent_appearance for e in equivalent_entries
        )

        annotations = [str(e.annotation) for e in equivalent_entries if e.annotation]
        if annotations:
            if len(annotations) > 1:
                # Just pick the longest one?
                self.annotation = max(annotations, key=lambda a: len(a))
            else:
                self.annotation = annotations[0]

        # Reset the entry's edition to be the Work's presentation edition.
        if work:
            best_edition = work.presentation_edition
        else:
            best_edition = None
        if work and not best_edition:
            work.calculate_presentation()
            best_edition = work.presentation_edition
        if best_edition and not best_edition == self.edition:
            logging.info(
                "Changing edition for list entry %r to %r from %r",
                self,
                best_edition,
                self.edition,
            )
            self.edition = best_edition

        self.set_work()

        for entry in equivalent_entries:
            if entry != self:
                _db.delete(entry)
        _db.commit


# TODO: This was originally designed to speed up queries against the
# materialized view that use custom list membership as a way to cut
# down on the result set. Now that we've removed the materialized
# view, is this still necessary? It might still be necessary for
# similar queries against Work.
Index(
    "ix_customlistentries_work_id_list_id",
    CustomListEntry.work_id,
    CustomListEntry.list_id,
)
