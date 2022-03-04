from core.equivalents_coverage import EquivalentIdentifiersCoverageProvider
from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, RecursiveEquivalencyCache
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

    def test_update_missing_coverage_records(self):
        assert self._db.query(EquivalencyCoverageRecord).count() == 0

        self.provider.update_missing_coverage_records()

        assert self._db.query(EquivalencyCoverageRecord).count() == 3

        records = self._db.query(EquivalencyCoverageRecord).all()
        assert [r.equivalency_id for r in records] == [r.id for r in self.equivalencies]
        assert [r.input_id for r in records] == [r.input_id for r in self.equivalencies]
        assert [r.output_id for r in records] == [
            r.output_id for r in self.equivalencies
        ]
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

    def test_process_batch_on_delete(self):
        self.provider.update_missing_coverage_records()

        # An identifier was deleted thereafter
        self._db.delete(self.idens[0])
        self._db.commit()

        batch = self.provider.items_that_need_coverage().all()
        assert len(batch) == 3
        self.provider.process_batch(batch)
