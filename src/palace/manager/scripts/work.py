from __future__ import annotations

import sys
from collections.abc import Iterator, Sequence
from typing import Any

from sqlalchemy.orm import Query, Session

from palace.manager.celery.tasks.work import classify_unchecked_subjects
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.scripts.base import Script
from palace.manager.scripts.input import IdentifierInputScript, SupportsReadlines
from palace.manager.scripts.timestamp import TimestampScript
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class WorkProcessingScript(IdentifierInputScript):
    name = "Work processing script"

    def __init__(
        self,
        force: bool = False,
        batch_size: int = 10,
        _db: Session | None = None,
        cmd_args: Sequence[str | None] | None = None,
        stdin: SupportsReadlines = sys.stdin,
    ) -> None:
        super().__init__(_db=_db)

        args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        self.identifier_type: str | None = args.identifier_type
        self.data_source: str | None = args.identifier_data_source

        if args.identifier_strings and not self.identifier_type:
            raise PalaceValueError(
                "No identifier type specified! Use '--identifier-type=\"Database ID\"' "
                "to name identifiers by database ID."
            )

        if args.identifiers is not None:
            self.identifiers: list[Identifier] = args.identifiers
        else:
            self.identifiers = []

        self.batch_size = batch_size
        self.query: Query[Work] | Query[LicensePool] = self.make_query(
            self._db,
            self.identifier_type,
            self.identifiers,
            self.data_source,
            log=self.log,
        )
        self.force = force

    def paginate_query(
        self, query: Query[Work] | Query[LicensePool]
    ) -> Iterator[list[Work | LicensePool]]:
        raise NotImplementedError()

    @classmethod
    def make_query(
        cls,
        _db: Session,
        identifier_type: str | None,
        identifiers: Sequence[Identifier] | None,
        data_source: str | None,
        log: Any | None = None,
    ) -> Query[Work] | Query[LicensePool]:
        query = _db.query(Work)
        if identifiers or identifier_type:
            query = query.join(Work.license_pools).join(LicensePool.identifier)

        if identifiers:
            if log:
                log.info("Restricted to %d specific identifiers." % len(identifiers))
            query = query.filter(
                LicensePool.identifier_id.in_([x.id for x in identifiers])
            )
        elif data_source:
            if log:
                log.info('Restricted to identifiers from DataSource "%s".', data_source)
            source = DataSource.lookup(_db, data_source)
            query = query.filter(LicensePool.data_source == source)

        if identifier_type:
            if log:
                log.info('Restricted to identifier type "%s".' % identifier_type)
            query = query.filter(Identifier.type == identifier_type)

        if log:
            log.info("Processing %d works.", query.count())
        return query.order_by(Work.id)

    def do_run(self) -> None:
        offset = 0
        paged_query: Iterator[list[Work | LicensePool]] | None = None

        # Does this script class allow uniquely paged queries
        # If not we will default to OFFSET paging
        try:
            paged_query = self.paginate_query(self.query)
        except NotImplementedError:
            paged_query = None

        while True:
            works: Sequence[Work | LicensePool]
            if paged_query is None:
                works = self.query.offset(offset).limit(self.batch_size).all()
            else:
                works = next(paged_query, [])
            if not works:
                break

            for work in works:
                self.process_work(work)
            offset += self.batch_size
            self._db.commit()
        self._db.commit()

    def process_work(self, work: Work | LicensePool) -> None:
        raise NotImplementedError()


class WorkConsolidationScript(WorkProcessingScript):
    """Given an Identifier, make sure all the LicensePools for that
    Identifier are in Works that follow these rules:

    a) For a given permanent work ID, there may be at most one Work
    containing open-access LicensePools.

    b) Each non-open-access LicensePool has its own individual Work.
    """

    name = "Work consolidation script"

    @classmethod
    def make_query(
        cls,
        _db: Session,
        identifier_type: str | None,
        identifiers: Sequence[Identifier] | None,
        data_source: str | None,
        log: Any | None = None,
    ) -> Query[LicensePool]:
        # We actually process LicensePools, not Works.
        qu = _db.query(LicensePool).join(LicensePool.identifier)
        if identifier_type:
            qu = qu.filter(Identifier.type == identifier_type)
        if identifiers:
            qu = qu.filter(
                Identifier.identifier.in_([x.identifier for x in identifiers])
            )
        return qu

    def process_work(self, work: Work | LicensePool) -> None:
        # We call it 'work' for signature compatibility with the superclass,
        # but it's actually a LicensePool.
        if not isinstance(work, LicensePool):
            return
        work.calculate_work()

    def do_run(self) -> None:
        super().do_run()
        qu = (
            self._db.query(Work)
            .outerjoin(Work.license_pools)
            .filter(LicensePool.id == None)
        )
        self.log.info("Deleting %d Works that have no LicensePools." % qu.count())
        for i in qu:
            self._db.delete(i)
        self._db.commit()


class WorkPresentationScript(TimestampScript, WorkProcessingScript):
    """Calculate the presentation for Work objects."""

    name = "Recalculate the presentation for works that need it."

    # Do a complete recalculation of the presentation.
    policy = PresentationCalculationPolicy()

    def process_work(self, work: Work | LicensePool) -> None:
        if not isinstance(work, Work):
            return
        work.calculate_presentation(policy=self.policy)


class WorkClassificationScript(WorkPresentationScript):
    """Recalculate the classification--and nothing else--for Work objects."""

    name = "Recalculate the classification for works that need it." ""

    policy = PresentationCalculationPolicy(
        choose_edition=False,
        set_edition_metadata=False,
        classify=True,
        choose_summary=False,
        calculate_quality=False,
        choose_cover=False,
        update_search_index=False,
    )


class ReclassifyWorksForUncheckedSubjectsScript(Script):
    """Reclassify all Works whose current classifications appear to
    depend on Subjects in the 'unchecked' state.

    This generally means that some migration script reset those
    Subjects because the rules for processing them changed.
    """

    name = "Reclassify works that use unchecked subjects." ""

    def run(self) -> None:

        classify_unchecked_subjects.delay()
        self.log.info(
            'Successfully queued "class_unchecked_subjects" task for future processing.  See '
            "celery logs for task execution details."
        )


class WorkOPDSScript(WorkPresentationScript):
    """Recalculate the OPDS entries, MARC record, and search index entries
    for Work objects.

    This is intended to verify that a problem has already been resolved and just
    needs to be propagated to these three 'caches'.
    """

    name = "Recalculate OPDS entries, MARC record, and search index entries for works that need it."

    policy = PresentationCalculationPolicy(
        choose_edition=False,
        set_edition_metadata=False,
        classify=True,
        choose_summary=False,
        calculate_quality=False,
        choose_cover=False,
        update_search_index=True,
    )
