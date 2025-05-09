import sys
from collections.abc import Generator

from sqlalchemy import tuple_
from sqlalchemy.orm import Query, defer

from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.scripts.input import IdentifierInputScript
from palace.manager.scripts.timestamp import TimestampScript
from palace.manager.sqlalchemy.model.classification import Classification, Subject
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class WorkProcessingScript(IdentifierInputScript):
    name = "Work processing script"

    def __init__(
        self, force=False, batch_size=10, _db=None, cmd_args=None, stdin=sys.stdin
    ):
        super().__init__(_db=_db)

        args = self.parse_command_line(self._db, cmd_args=cmd_args, stdin=stdin)
        self.identifier_type = args.identifier_type
        self.data_source = args.identifier_data_source

        self.identifiers = self.parse_identifier_list(
            self._db, self.identifier_type, self.data_source, args.identifier_strings
        )

        self.batch_size = batch_size
        self.query = self.make_query(
            self._db,
            self.identifier_type,
            self.identifiers,
            self.data_source,
            log=self.log,
        )
        self.force = force

    def paginate_query(self, query):
        raise NotImplementedError()

    @classmethod
    def make_query(cls, _db, identifier_type, identifiers, data_source, log=None):
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

    def do_run(self):
        works = True
        offset = 0

        # Does this script class allow uniquely paged queries
        # If not we will default to OFFSET paging
        try:
            paged_query = self.paginate_query(self.query)
        except NotImplementedError:
            paged_query = None

        while works:
            if not paged_query:
                works = self.query.offset(offset).limit(self.batch_size).all()
            else:
                works = next(paged_query, [])

            for work in works:
                self.process_work(work)
            offset += self.batch_size
            self._db.commit()
        self._db.commit()

    def process_work(self, work):
        raise NotImplementedError()


class WorkConsolidationScript(WorkProcessingScript):
    """Given an Identifier, make sure all the LicensePools for that
    Identifier are in Works that follow these rules:

    a) For a given permanent work ID, there may be at most one Work
    containing open-access LicensePools.

    b) Each non-open-access LicensePool has its own individual Work.
    """

    name = "Work consolidation script"

    def make_query(self, _db, identifier_type, identifiers, data_source, log=None):
        # We actually process LicensePools, not Works.
        qu = _db.query(LicensePool).join(LicensePool.identifier)
        if identifier_type:
            qu = qu.filter(Identifier.type == identifier_type)
        if identifiers:
            qu = qu.filter(
                Identifier.identifier.in_([x.identifier for x in identifiers])
            )
        return qu

    def process_work(self, work):
        # We call it 'work' for signature compatibility with the superclass,
        # but it's actually a LicensePool.
        licensepool = work
        licensepool.calculate_work()

    def do_run(self):
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

    def process_work(self, work):
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


class ReclassifyWorksForUncheckedSubjectsScript(WorkClassificationScript):
    """Reclassify all Works whose current classifications appear to
    depend on Subjects in the 'unchecked' state.

    This generally means that some migration script reset those
    Subjects because the rules for processing them changed.
    """

    name = "Reclassify works that use unchecked subjects." ""

    policy = WorkClassificationScript.policy

    batch_size = 100

    def __init__(self, _db=None):
        self.timestamp_collection = None
        if _db:
            self._session = _db
        self.query = self._optimized_query()

    def _optimized_query(self):
        """Optimizations include
        - Order by each joined table's PK, so that paging is consistent
        - Deferred loading of large text columns"""

        # No filter clause yet, we will filter this PER SUBJECT ID
        # in the paginate query
        query = (
            self._db.query(Work)
            .join(Work.license_pools)
            .join(LicensePool.identifier)
            .join(Identifier.classifications)
            .join(Classification.subject)
        )

        # Must order by all joined attributes
        query = (
            query.order_by(None)
            .order_by(
                Subject.id, Work.id, LicensePool.id, Identifier.id, Classification.id
            )
            .options(
                defer(Work.summary_text),
            )
        )

        return query

    def _unchecked_subjects(self):
        """Yield one unchecked subject at a time"""
        query = (
            self._db.query(Subject)
            .filter(Subject.checked == False)
            .order_by(Subject.id)
        )
        last_id = None
        while True:
            qu = query
            if last_id:
                qu = qu.filter(Subject.id > last_id)
            subject = qu.first()

            if not subject:
                return

            last_id = subject.id
            yield subject

    def paginate_query(self, query) -> Generator:
        """Page this query using the row-wise comparison
        technique unique to this job. We have already ensured
        the ordering of the rows follows all the joined tables"""

        for subject in self._unchecked_subjects():
            last_work: Work | None = None  # Last work object of the previous page
            # IDs of the last work, for paging
            work_id, license_id, iden_id, classn_id = (
                None,
                None,
                None,
                None,
            )

            while True:
                # We are a "per subject" filter, this is the MOST efficient method
                qu: Query = query.filter(Subject.id == subject.id)
                # Add the columns we need to page with explicitly in the query
                qu = qu.add_columns(LicensePool.id, Identifier.id, Classification.id)
                # We're not on the first page, add the row-wise comparison
                if last_work is not None:
                    qu = qu.filter(
                        tuple_(
                            Work.id,
                            LicensePool.id,
                            Identifier.id,
                            Classification.id,
                        )
                        > (work_id, license_id, iden_id, classn_id)
                    )

                qu = qu.limit(self.batch_size)
                works = qu.all()
                if not len(works):
                    break

                last_work_row = works[-1]
                last_work = last_work_row[0]
                # set comprehension ensures we get unique works per loop
                # Works will get duplicated in the query because of the addition
                # of the ID columns in the select, it is possible and expected
                # that works will get duplicated across loops. It is not a desired
                # outcome to duplicate works across loops, but the alternative is to maintain
                # the IDs in memory and add a NOT IN operator in the query
                # which would grow quite large, quite fast
                only_works = list({w[0] for w in works})

                yield only_works

                work_id, license_id, iden_id, classn_id = (
                    last_work_row[0].id,
                    last_work_row[1],
                    last_work_row[2],
                    last_work_row[3],
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
