from typing import List, Optional, Set

from sqlalchemy import and_, delete, select
from sqlalchemy.orm import Query, joinedload

from core.coverage import BaseCoverageProvider, CoverageFailure
from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache


class EquivalentIdentifiersCoverageProvider(BaseCoverageProvider):
    """Computes the chain of equivalence between identifiers
    This is a DB compute intensive operation that is currently handled by
    all the jobs that use the recursive identifiers equivalences
    The equivalences do not change nearly as often as the other jobs run
    so it is prudent to pre-compute and store these values
    """

    SERVICE_NAME = "Equivalent identifiers coverage provider"

    DEFAULT_BATCH_SIZE = 200

    OPERATION = EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH

    def __init__(
        self, _db, batch_size=None, cutoff_time=None, registered_only=False, **kwargs
    ):
        # Set of identifiers covered this run of the provider
        self._already_covered_identifiers: Set[int] = set()
        super().__init__(_db, batch_size, cutoff_time, registered_only)

    def run(self):
        self.update_missing_coverage_records()
        ret = super().run()
        self.update_identity_recursive_equivalents()
        return ret

    def items_that_need_coverage(self, identifiers=None, **kwargs) -> Query:
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
    ) -> Set[Optional[int]]:
        """Get all identifiers this coverage run should recompute
        This involves inputs and outputs, and also any parent_identifier
        that has a direct relation with these identifiers
        """

        equivs = [r.equivalency for r in records]
        # process both inputs and outputs
        identifier_ids_list: List[Optional[int]] = [eq.input_id for eq in equivs]
        identifier_ids_list.extend([eq.output_id for eq in equivs])
        identifier_ids: Set[Optional[int]] = set(identifier_ids_list)

        # Any identifier found, should be recalculated
        # However we must recalculate any other chain these identifiers were part of also
        # Since now those chains MAY contain this modified equivalency
        other_chains = self._db.query(RecursiveEquivalencyCache).filter(
            RecursiveEquivalencyCache.identifier_id.in_(identifier_ids)
        )
        for item in other_chains.all():
            identifier_ids.add(item.parent_identifier_id)

        return identifier_ids

    def process_batch(
        self, batch: List[EquivalencyCoverageRecord]
    ) -> List[EquivalencyCoverageRecord]:
        """Query for and store the chain of equivalent identifiers
        batch sizes are not exact since we pull the related identifiers into
        the current batch too, so they would start out larger than intended
        but towards the end of the job should be smaller than the batch size
        There is no failure path here
        """
        completed_identifiers = set()
        identifier_ids = self._identifiers_for_coverage(batch)

        # Use the stored procedure to identify the chain of equivalency
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

    def failure_for_ignored_item(self, equivalency: Equivalency) -> CoverageFailure:
        """Item was ignored by the batch processing"""
        return CoverageFailure(equivalency, "Was ignored by CoverageProvider.")

    def record_failure_as_coverage_record(self, failure: CoverageFailure):
        """Convert the CoverageFailure to a EquivalencyCoverageRecord"""
        return failure.to_equivalency_coverage_record(self.operation)

    def add_coverage_record_for(
        self, item: EquivalencyCoverageRecord
    ) -> EquivalencyCoverageRecord:
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

    def update_identity_recursive_equivalents(self):
        """Update identifiers within the recursives table
        with (id, id) in case they do not have equivalents
        This is required so that when pulling recursives for an identifier
        we always atleast get the identifier itself
        NOTE: There are no coveragerecords for these
        This might deserve its own batchable job
        """
        missing_identifiers = (
            select(Identifier.id)
            .outerjoin(
                RecursiveEquivalencyCache,
                and_(
                    RecursiveEquivalencyCache.parent_identifier_id == Identifier.id,
                    RecursiveEquivalencyCache.is_parent == True,
                ),
            )
            .filter(RecursiveEquivalencyCache.id == None)
            .execution_options(yield_per=self.batch_size)
        )

        processed = []
        for identifier in self._db.execute(missing_identifiers):
            self._db.add(
                RecursiveEquivalencyCache(
                    parent_identifier_id=identifier.id, identifier_id=identifier.id
                )
            )
            processed.append(identifier.id)
        self._db.commit()

        return processed
