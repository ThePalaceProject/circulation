from typing import List

from sqlalchemy.orm.session import Session

from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache


class EquivalencyCoverageQueries:
    @classmethod
    def add_coverage_for_identifiers_chain(
        cls, identifiers: List[Identifier], _db=None
    ) -> List[EquivalencyCoverageRecord]:
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
        equivs: List[Equivalency] = Equivalency.for_identifiers(
            _db, (p[0] for p in parent_ids)
        )
        records = []
        for equiv in equivs:

            record, is_new = EquivalencyCoverageRecord.add_for(
                equiv,
                EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
                status=EquivalencyCoverageRecord.REGISTERED,
            )
            records.append(record)

        _db.add_all(records)
        _db.commit()

        return records
