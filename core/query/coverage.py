from sqlalchemy.orm.session import Session

from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache


class EquivalencyCoverageQueries:
    @classmethod
    def add_coverage_for_identifiers_chain(
        cls, identifiers: list[Identifier], _db=None
    ) -> list[EquivalencyCoverageRecord]:
        """Hunt down any recursive identifiers that may be touched by these identifiers
        set all the possible coverages to reset and recompute the chain
        """
        if not len(identifiers):
            return []

        if not _db:
            _db = Session.object_session(identifiers[0])
        ids = list(idn.id for idn in identifiers)

        # Any parent ids that touch these identifiers
        parent_ids = (
            _db.query(RecursiveEquivalencyCache.parent_identifier_id)
            .filter(RecursiveEquivalencyCache.identifier_id.in_(ids))
            .all()
        )

        # Need to be reset
        equivs: list[Equivalency] = Equivalency.for_identifiers(
            _db, (p[0] for p in parent_ids)
        )
        records = []

        # Make sure we haven't already added this record to the session, but not committed it yet
        existing_records = {
            (record.equivalency, record.operation)
            for record in _db.new
            if isinstance(record, EquivalencyCoverageRecord)
        }
        for equiv in equivs:
            if (
                equiv,
                EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            ) in existing_records:
                continue
            record, is_new = EquivalencyCoverageRecord.add_for(
                equiv,
                EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
                status=EquivalencyCoverageRecord.REGISTERED,
            )
            records.append(record)

        _db.add_all(records)

        return records
