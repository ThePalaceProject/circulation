from typing import List

import pytest
import sqlalchemy
from sqlalchemy import or_

from core.equivalents_coverage import EquivalentIdentifiersCoverageProvider
from core.model.coverage import EquivalencyCoverageRecord
from core.model.identifier import Equivalency, Identifier, RecursiveEquivalencyCache
from core.query.coverage import EquivalencyCoverageQueries
from tests.core.models.test_coverage import ExampleEquivalencyCoverageRecordFixture
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture()
def equivalency_coverage_record_fixture(
    db: DatabaseTransactionFixture,
) -> ExampleEquivalencyCoverageRecordFixture:
    return ExampleEquivalencyCoverageRecordFixture(db)


class EquivalentCoverageFixture:
    coverage_records: ExampleEquivalencyCoverageRecordFixture
    provider: EquivalentIdentifiersCoverageProvider
    transaction: DatabaseTransactionFixture
    identifiers: List[Identifier]
    equivalencies: List[Equivalency]


@pytest.fixture()
def equivalent_coverage_fixture(
    equivalency_coverage_record_fixture: ExampleEquivalencyCoverageRecordFixture,
) -> EquivalentCoverageFixture:
    coverage = equivalency_coverage_record_fixture
    data = EquivalentCoverageFixture()
    data.coverage_records = coverage
    data.provider = EquivalentIdentifiersCoverageProvider(coverage.transaction.session)
    data.transaction = coverage.transaction
    data.equivalencies = coverage.equivalencies
    data.identifiers = coverage.identifiers
    return data


class TestEquivalentCoverage:
    def _drop_all_recursives(self, session):
        # Remove the identity items
        session.query(RecursiveEquivalencyCache).delete()
        session.commit()

    def test_update_missing_coverage_records(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        data = equivalent_coverage_fixture
        session = data.transaction.session
        assert session.query(EquivalencyCoverageRecord).count() == 0

        data.provider.update_missing_coverage_records()

        assert session.query(EquivalencyCoverageRecord).count() == 3

        records = session.query(EquivalencyCoverageRecord).all()
        assert [r.equivalency_id for r in records] == [r.id for r in data.equivalencies]
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

    def test_items_that_need_coverage(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        qu = data.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 0

        data.provider.update_missing_coverage_records()
        qu = data.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 3

    def test_items_that_need_coverage_few(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        qu = data.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 0

        eq, inew = EquivalencyCoverageRecord.add_for(
            data.equivalencies[0],
            EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            status=EquivalencyCoverageRecord.REGISTERED,
        )

        qu = data.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 1

        eq, inew = EquivalencyCoverageRecord.add_for(
            data.equivalencies[0],
            EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            status=EquivalencyCoverageRecord.SUCCESS,
        )

        qu = data.provider.items_that_need_coverage()
        records = qu.all()
        assert len(records) == 0

        eq, inew = EquivalencyCoverageRecord.add_for(
            data.equivalencies[0],
            EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
            status=EquivalencyCoverageRecord.PERSISTENT_FAILURE,
        )

        qu = data.provider.items_that_need_coverage(
            count_as_covered=[
                EquivalencyCoverageRecord.SUCCESS,
                EquivalencyCoverageRecord.TRANSIENT_FAILURE,
            ]
        )
        records = qu.all()
        assert len(records) == 1

    def test_identifiers_for_coverage(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        data.provider.update_missing_coverage_records()
        items = data.provider.items_that_need_coverage().all()
        identifier_ids = data.provider._identifiers_for_coverage(items)

        assert len(items) == 3
        assert len(identifier_ids) == 3

        rec = RecursiveEquivalencyCache(
            parent_identifier_id=data.identifiers[1].id,
            identifier_id=data.identifiers[2].id,
        )
        session.add(rec)

        eq = Equivalency(
            input_id=data.identifiers[2].id,
            output_id=data.identifiers[3].id,
            strength=1,
        )
        session.add(eq)
        session.commit()

        record, is_new = EquivalencyCoverageRecord.add_for(
            eq, EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH
        )

        identifier_ids = data.provider._identifiers_for_coverage([record])

        assert len(identifier_ids) == 3
        assert identifier_ids == {
            data.identifiers[1].id,
            data.identifiers[2].id,
            data.identifiers[3].id,
        }

    def test_process_batch(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        self._drop_all_recursives(session)

        data.provider.update_missing_coverage_records()
        batch = data.provider.items_that_need_coverage().all()

        assert len(data.provider._already_covered_identifiers) == 0

        return_batch = data.provider.process_batch(batch)

        assert len(batch) == 3
        assert len(return_batch) == 3

        # still in the registered mode
        assert {b.status for b in return_batch} == {
            EquivalencyCoverageRecord.REGISTERED,
        }
        assert len(data.provider._already_covered_identifiers) == 3

        recursives = session.query(RecursiveEquivalencyCache).all()
        assert len(recursives) == 9

    def test_process_batch_on_delete(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        self._drop_all_recursives(session)
        data.provider.update_missing_coverage_records()

        # An identifier was deleted thereafter
        session.delete(data.identifiers[0])
        session.commit()

        batch = data.provider.items_that_need_coverage().all()
        assert len(batch) == 1

        data.provider.process_batch(batch)

        # 2 for each identifier left in the chain
        assert len(session.query(RecursiveEquivalencyCache).all()) == 4

    def test_on_delete_listeners(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        data.provider.update_missing_coverage_records()
        batch = list(data.provider.items_that_need_coverage())
        data.provider.process_batch(batch)

        all_records = session.query(EquivalencyCoverageRecord).all()
        for r in all_records:
            r.status = r.SUCCESS

        session.commit()

        target_recursives = (
            session.query(RecursiveEquivalencyCache)
            .filter(
                or_(
                    RecursiveEquivalencyCache.parent_identifier_id
                    == data.identifiers[0].id,
                    RecursiveEquivalencyCache.identifier_id == data.identifiers[0].id,
                )
            )
            .all()
        )

        session.delete(data.identifiers[0])
        session.commit()
        session.expire_all()

        all_records = session.query(EquivalencyCoverageRecord).all()
        for r in all_records:
            if data.identifiers[1] in (r.equivalency.input_id, r.equivalency.input_id):
                assert r.status == r.REGISTERED

        recursives_count = (
            session.query(RecursiveEquivalencyCache)
            .filter(
                or_(
                    RecursiveEquivalencyCache.parent_identifier_id
                    == data.identifiers[0].id,
                    RecursiveEquivalencyCache.identifier_id == data.identifiers[0].id,
                )
            )
            .count()
        )
        assert recursives_count == 0

        assert len(target_recursives) != 0
        for t in target_recursives:
            with pytest.raises(sqlalchemy.exc.InvalidRequestError):
                session.refresh(t)

    def test_add_coverage_for_identifiers_chain(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        data.provider.update_missing_coverage_records()
        batch = list(data.provider.items_that_need_coverage())
        data.provider.process_batch(batch)

        records = EquivalencyCoverageQueries.add_coverage_for_identifiers_chain(
            [data.identifiers[0]]
        )

        for r in records:
            assert r.status == r.REGISTERED
            # identity 1 is connected only to identitity 2
            assert data.identifiers[1].id in (
                r.equivalency.input_id,
                r.equivalency.output_id,
            )

    def test_update_identity_recursive_equivalents(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        self._drop_all_recursives(session)

        data.provider.update_missing_coverage_records()
        batch = list(data.provider.items_that_need_coverage())
        data.provider.process_batch(batch)

        missing = data.provider.update_identity_recursive_equivalents()

        assert len(missing) == 1
        assert missing[0] == data.identifiers[3].id

        recursives = (
            session.query(RecursiveEquivalencyCache)
            .filter(
                RecursiveEquivalencyCache.parent_identifier_id == data.identifiers[3].id
            )
            .all()
        )

        assert len(recursives) == 1
        assert recursives[0].is_parent == True

    def test_newly_added_identifier(
        self, equivalent_coverage_fixture: EquivalentCoverageFixture
    ):
        session, data = (
            equivalent_coverage_fixture.transaction.session,
            equivalent_coverage_fixture,
        )

        # Identifiers and equivalencies are added but not acted upon
        # We must still have the self recursion available, from the listener

        all_recursives = session.query(RecursiveEquivalencyCache).all()
        assert len(all_recursives) == 4
