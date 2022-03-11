import pytest
import sqlalchemy
from sqlalchemy import or_

from core.equivalents_coverage import EquivalentIdentifiersCoverageProvider
from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, RecursiveEquivalencyCache
from core.query.coverage import EquivalencyCoverageQueries
from core.testing import DatabaseTest


class TestEquivalentCoverage(DatabaseTest):
    def setup_method(self):
        super().setup_method()
        self.idens = [
            self._identifier(),
            self._identifier(),
            self._identifier(),
            self._identifier(),
        ]
        idn = self.idens
        self.equivalencies = [
            Equivalency(input_id=idn[0].id, output_id=idn[1].id, strength=1),
            Equivalency(input_id=idn[1].id, output_id=idn[2].id, strength=1),
            Equivalency(input_id=idn[1].id, output_id=idn[0].id, strength=1),
        ]
        self._db.add_all(self.equivalencies)
        self._db.commit()
        self.provider = EquivalentIdentifiersCoverageProvider(self._db)

    def _drop_all_recursives(self):
        # Remove the identity items
        self._db.query(RecursiveEquivalencyCache).delete()
        self._db.commit()

    def test_update_missing_coverage_records(self):
        assert self._db.query(EquivalencyCoverageRecord).count() == 0

        self.provider.update_missing_coverage_records()

        assert self._db.query(EquivalencyCoverageRecord).count() == 3

        records = self._db.query(EquivalencyCoverageRecord).all()
        assert [r.equivalency_id for r in records] == [r.id for r in self.equivalencies]
        assert (
            len(
                list(
                    filter(
                        lambda x: x.operation
                        == EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
                        records,
                    )
                )
            )
            == 3
        )
        assert (
            len(
                list(
                    filter(
                        lambda x: x.status == EquivalencyCoverageRecord.REGISTERED,
                        records,
                    )
                )
            )
            == 3
        )

    def test_items_that_need_coverage(self):
        qu = self.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 0

        self.provider.update_missing_coverage_records()
        qu = self.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 3

    def test_items_that_need_coverage_few(self):
        qu = self.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 0

        eq, inew = EquivalencyCoverageRecord.add_for(
            self.equivalencies[0],
            EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            status=EquivalencyCoverageRecord.REGISTERED,
        )

        qu = self.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 1

        eq, inew = EquivalencyCoverageRecord.add_for(
            self.equivalencies[0],
            EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            status=EquivalencyCoverageRecord.SUCCESS,
        )

        qu = self.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 0

        eq, inew = EquivalencyCoverageRecord.add_for(
            self.equivalencies[0],
            EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            status=EquivalencyCoverageRecord.PERSISTENT_FAILURE,
        )

        qu = self.provider.items_that_need_coverage(
            count_as_covered=[
                EquivalencyCoverageRecord.SUCCESS,
                EquivalencyCoverageRecord.TRANSIENT_FAILURE,
            ]
        )
        records = qu.all()
        assert len(records) == 1

    def test_identifiers_for_coverage(self):
        self.provider.update_missing_coverage_records()
        items = self.provider.items_that_need_coverage().all()
        identifier_ids = self.provider._identifiers_for_coverage(items)

        assert len(items) == 3
        assert len(identifier_ids) == 3

        rec = RecursiveEquivalencyCache(
            parent_identifier_id=self.idens[1].id, identifier_id=self.idens[2].id
        )
        self._db.add(rec)

        eq = Equivalency(
            input_id=self.idens[2].id, output_id=self.idens[3].id, strength=1
        )
        self._db.add(eq)

        record, is_new = EquivalencyCoverageRecord.add_for(
            eq, EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH
        )

        identifier_ids = self.provider._identifiers_for_coverage([record])

        assert len(identifier_ids) == 3
        assert identifier_ids == {self.idens[1].id, self.idens[2].id, self.idens[3].id}

    def test_process_batch(self):
        self._drop_all_recursives()

        self.provider.update_missing_coverage_records()
        batch = self.provider.items_that_need_coverage().all()

        assert len(self.provider._already_covered_identifiers) == 0

        return_batch = self.provider.process_batch(batch)

        assert len(batch) == 3
        assert len(return_batch) == 3

        # still in the registered mode
        assert {b.status for b in return_batch} == {
            EquivalencyCoverageRecord.REGISTERED,
        }
        assert len(self.provider._already_covered_identifiers) == 3

        recursives = self._db.query(RecursiveEquivalencyCache).all()
        assert len(recursives) == 9

    def test_process_batch_on_delete(self):
        self._drop_all_recursives()
        self.provider.update_missing_coverage_records()

        # An identifier was deleted thereafter
        self._db.delete(self.idens[0])
        self._db.commit()

        batch = self.provider.items_that_need_coverage().all()
        assert len(batch) == 1

        self.provider.process_batch(batch)

        # 2 for each identifier left in the chain
        assert len(self._db.query(RecursiveEquivalencyCache).all()) == 4

    def test_on_delete_listeners(self):
        self.provider.update_missing_coverage_records()
        batch = self.provider.items_that_need_coverage()
        self.provider.process_batch(batch)

        all_records = self._db.query(EquivalencyCoverageRecord).all()
        for r in all_records:
            r.status = r.SUCCESS

        self._db.commit()

        target_recursives = (
            self._db.query(RecursiveEquivalencyCache)
            .filter(
                or_(
                    RecursiveEquivalencyCache.parent_identifier_id == self.idens[0].id,
                    RecursiveEquivalencyCache.identifier_id == self.idens[0].id,
                )
            )
            .all()
        )

        self._db.delete(self.idens[0])
        self._db.commit()
        self._db.expire_all()

        all_records = self._db.query(EquivalencyCoverageRecord).all()
        for r in all_records:
            if self.idens[1] in (r.equivalency.input_id, r.equivalency.input_id):
                assert r.status == r.REGISTERED

        recursives_count = (
            self._db.query(RecursiveEquivalencyCache)
            .filter(
                or_(
                    RecursiveEquivalencyCache.parent_identifier_id == self.idens[0].id,
                    RecursiveEquivalencyCache.identifier_id == self.idens[0].id,
                )
            )
            .count()
        )
        assert recursives_count == 0

        assert len(target_recursives) != 0
        for t in target_recursives:
            with pytest.raises(sqlalchemy.exc.InvalidRequestError):
                self._db.refresh(t)

    def test_add_coverage_for_identifiers_chain(self):
        self.provider.update_missing_coverage_records()
        batch = self.provider.items_that_need_coverage()
        self.provider.process_batch(batch)

        records = EquivalencyCoverageQueries.add_coverage_for_identifiers_chain(
            [self.idens[0]]
        )

        for r in records:
            assert r.status == r.REGISTERED
            # identity 1 is connected only to identitity 2
            assert self.idens[1].id in (r.equivalency.input_id, r.equivalency.output_id)

    def test_update_identity_recursive_equivalents(self):
        self._drop_all_recursives()

        self.provider.update_missing_coverage_records()
        batch = self.provider.items_that_need_coverage()
        self.provider.process_batch(batch)

        missing = self.provider.update_identity_recursive_equivalents()

        assert len(missing) == 1
        assert missing[0].id == self.idens[3].id

        recursives = (
            self._db.query(RecursiveEquivalencyCache)
            .filter(RecursiveEquivalencyCache.parent_identifier_id == self.idens[3].id)
            .all()
        )

        assert len(recursives) == 1
        assert recursives[0].is_parent == True

    def test_newly_added_identifier(self):
        # Identifiers and equivalencies are added but not acted upon
        # We must still have the self recursion available, from the listener

        all_recursives = self._db.query(RecursiveEquivalencyCache).all()
        assert len(all_recursives) == 4
