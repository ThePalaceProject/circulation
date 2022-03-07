from typing import List, Set

from sqlalchemy import delete
from sqlalchemy.orm import joinedload

from core.coverage import BaseCoverageProvider, CoverageFailure
from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache


class EquivalentIdentifiersCoverageProvider(BaseCoverageProvider):

    SERVICE_NAME = "Equivalent identifiers coverage provider"

    DEFAULT_BATCH_SIZE = 50

    OPERATION = EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH

    def __init__(
        self, _db, batch_size=None, cutoff_time=None, registered_only=False, **kwargs
    ):
        # Set of identifiers covered this run of the provider
        self._already_covered_identifiers = set()
        super().__init__(_db, batch_size, cutoff_time, registered_only)

    def run(self):
        self.update_missing_coverage_records()
        return super().run()

    def items_that_need_coverage(self, identifiers=None, **kwargs):
        qu = (
            self._db.query(EquivalencyCoverageRecord)
            .filter(EquivalencyCoverageRecord.operation == self.operation)
            .order_by(EquivalencyCoverageRecord.id)
            .options(joinedload(EquivalencyCoverageRecord.equivalency))
        )

        # Need this function exactly, unfortuanately its in workcoveragerecord
        missing = EquivalencyCoverageRecord.not_covered(
            kwargs.get("count_as_covered"), kwargs.get("count_as_missing_before")
        )
        qu = qu.filter(missing)

        return qu

    def _identifiers_for_coverage(
        self, records: List[EquivalencyCoverageRecord]
    ) -> Set[int]:

        equivs = [r.equivalency for r in records]
        # process both inputs and outputs
        identifier_ids = [eq.input_id for eq in equivs]
        identifier_ids.extend([eq.output_id for eq in equivs])
        identifier_ids = set(identifier_ids)

        # Any identifier found, should be recalculated
        # However we must recalculate any other chain these identifiers were part of also
        # Since now those chains MAY contain this modified equivalency
        other_chains = self._db.query(RecursiveEquivalencyCache).filter(
            RecursiveEquivalencyCache.identifier_id.in_(identifier_ids)
        )
        for item in other_chains.all():
            identifier_ids.add(item.parent_identifier_id)

        return identifier_ids

    def process_batch(self, batch):
        completed_identifiers = set()
        identifier_ids = self._identifiers_for_coverage(batch)

        qu = Identifier.recursively_equivalent_identifier_ids_query(Identifier.id)
        qu = (
            qu.select_from(Identifier)
            .where(Identifier.id.in_(identifier_ids))
            .column(Identifier.id)
        )

        chained_identifiers = self._db.execute(qu).fetchall()

        # We don't want to cover identifiers already looped over
        identifier_ids.difference_update(self._already_covered_identifiers)

        recursive_equivs = []
        for link_id, parent_id in chained_identifiers:

            # First time around we MUST delete any chains formed from this identifier before
            if parent_id not in completed_identifiers:
                delete_stmt = delete(RecursiveEquivalencyCache).where(
                    RecursiveEquivalencyCache.parent_identifier_id == parent_id
                )
                self._db.execute(delete_stmt)

            recursive_equivs.append(
                RecursiveEquivalencyCache(
                    parent_identifier_id=parent_id, identifier_id=link_id
                )
            )
            completed_identifiers.add(parent_id)

        self._db.add_all(recursive_equivs)

        self._already_covered_identifiers.update(completed_identifiers)

        ret = [
            b
            for b in batch
            if completed_identifiers.issuperset(
                (b.equivalency.input_id, b.equivalency.output_id)
            )
        ]
        return ret

    def failure_for_ignored_item(self, equivalency):
        """TODO: this method"""
        print("RECORD FAILED IGNORE", equivalency)
        return None

    def record_failure_as_coverage_record(self, failure):
        """TODO: this method"""
        print("RECORD FAILED", failure)
        return CoverageFailure(failure, "Did not run")

    def add_coverage_record_for(self, item: EquivalencyCoverageRecord):
        return EquivalencyCoverageRecord.add_for(
            item.equivalency, operation=self.operation
        )

    def update_missing_coverage_records(self):
        """
        Register coveragerecords for all equivalents without records
        This is required so we don't run a seq scan per loop
        but rather ONCE at the start of the job
        """
        qu = (
            self._db.query(Equivalency)
            .outerjoin(
                EquivalencyCoverageRecord,
                Equivalency.id == EquivalencyCoverageRecord.equivalency_id,
            )
            .filter(EquivalencyCoverageRecord.id == None)
        )

        eqs_without_records = qu.all()

        EquivalencyCoverageRecord.bulk_add(
            self._db, eqs_without_records, self.operation, batch_size=500
        )
