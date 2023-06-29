import datetime
from typing import List

import pytest

from core.metadata_layer import TimestampData
from core.model.coverage import (
    BaseCoverageRecord,
    CoverageRecord,
    EquivalencyCoverageRecord,
    Timestamp,
    WorkCoverageRecord,
)
from core.model.datasource import DataSource
from core.model.edition import Edition
from core.model.identifier import Equivalency, Identifier
from core.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestTimestamp:
    def test_lookup(self, db: DatabaseTransactionFixture):
        c1 = db.default_collection()
        c2 = db.collection()

        # Create a timestamp.
        timestamp = Timestamp.stamp(db.session, "service", Timestamp.SCRIPT_TYPE, c1)

        # Look it up.
        assert timestamp == Timestamp.lookup(
            db.session, "service", Timestamp.SCRIPT_TYPE, c1
        )

        # There are a number of ways to _fail_ to look up this timestamp.
        assert None == Timestamp.lookup(
            db.session, "other service", Timestamp.SCRIPT_TYPE, c1
        )
        assert None == Timestamp.lookup(
            db.session, "service", Timestamp.MONITOR_TYPE, c1
        )
        assert None == Timestamp.lookup(
            db.session, "service", Timestamp.SCRIPT_TYPE, c2
        )

        # value() works the same way as lookup() but returns the actual
        # timestamp.finish value.
        assert timestamp.finish == Timestamp.value(
            db.session, "service", Timestamp.SCRIPT_TYPE, c1
        )
        assert None == Timestamp.value(db.session, "service", Timestamp.SCRIPT_TYPE, c2)

    def test_stamp(self, db: DatabaseTransactionFixture):
        service = "service"
        type = Timestamp.SCRIPT_TYPE

        # If no date is specified, the value of the timestamp is the time
        # stamp() was called.
        stamp = Timestamp.stamp(db.session, service, type)
        now = utc_now()
        assert (now - stamp.finish).total_seconds() < 2
        assert stamp.start == stamp.finish
        assert service == stamp.service
        assert type == stamp.service_type
        assert None == stamp.collection
        assert None == stamp.achievements
        assert None == stamp.counter
        assert None == stamp.exception

        # Calling stamp() again will update the Timestamp.
        stamp2 = Timestamp.stamp(
            db.session, service, type, achievements="yay", counter=100, exception="boo"
        )
        assert stamp == stamp2
        now = utc_now()
        assert (now - stamp.finish).total_seconds() < 2
        assert stamp.start == stamp.finish
        assert service == stamp.service
        assert type == stamp.service_type
        assert None == stamp.collection
        assert "yay" == stamp.achievements
        assert 100 == stamp.counter
        assert "boo" == stamp.exception

        # Passing in a different collection will create a new Timestamp.
        stamp3 = Timestamp.stamp(
            db.session, service, type, collection=db.default_collection()
        )
        assert stamp3 != stamp
        assert db.default_collection() == stamp3.collection

        # Passing in CLEAR_VALUE for start, end, or exception will
        # clear an existing Timestamp.
        stamp4 = Timestamp.stamp(
            db.session,
            service,
            type,
            start=Timestamp.CLEAR_VALUE,
            finish=Timestamp.CLEAR_VALUE,
            exception=Timestamp.CLEAR_VALUE,
        )
        assert stamp4 == stamp
        assert None == stamp4.start
        assert None == stamp4.finish
        assert None == stamp4.exception

    def test_update(self, db: DatabaseTransactionFixture):
        # update() can modify the fields of a Timestamp that aren't
        # used to identify it.
        stamp = Timestamp.stamp(db.session, "service", Timestamp.SCRIPT_TYPE)
        start = datetime_utc(2010, 1, 2)
        finish = datetime_utc(2018, 3, 4)
        achievements = db.fresh_str()
        counter = db.fresh_id()
        exception = db.fresh_str()
        stamp.update(start, finish, achievements, counter, exception)

        assert start == stamp.start
        assert finish == stamp.finish
        assert achievements == stamp.achievements
        assert counter == stamp.counter
        assert exception == stamp.exception

        # .exception is the only field update() will set to a value of
        # None. For all other fields, None means "don't update the existing
        # value".
        stamp.update()
        assert start == stamp.start
        assert finish == stamp.finish
        assert achievements == stamp.achievements
        assert counter == stamp.counter
        assert None == stamp.exception

    def to_data(self, db: DatabaseTransactionFixture):
        stamp = Timestamp.stamp(
            db.session,
            "service",
            Timestamp.SCRIPT_TYPE,
            collection=db.default_collection(),
            counter=10,
            achievements="a",
        )
        data = stamp.to_data()
        assert isinstance(data, TimestampData)

        # The TimestampData is not finalized.
        assert None == data.service
        assert None == data.service_type
        assert None == data.collection_id

        # But all the other information is there.
        assert stamp.start == data.start
        assert stamp.finish == data.finish
        assert stamp.achievements == data.achievements
        assert stamp.counter == data.counter


class TestBaseCoverageRecord:
    def test_not_covered(self, db: DatabaseTransactionFixture):
        source = DataSource.lookup(db.session, DataSource.OCLC)

        # Here are four identifiers with four relationships to a
        # certain coverage provider: no coverage at all, successful
        # coverage, a transient failure and a permanent failure.

        no_coverage = db.identifier()

        success = db.identifier()
        success_record = db.coverage_record(success, source)
        success_record.timestamp = utc_now() - datetime.timedelta(seconds=3600)
        assert CoverageRecord.SUCCESS == success_record.status

        transient = db.identifier()
        transient_record = db.coverage_record(
            transient, source, status=CoverageRecord.TRANSIENT_FAILURE
        )
        assert CoverageRecord.TRANSIENT_FAILURE == transient_record.status

        persistent = db.identifier()
        persistent_record = db.coverage_record(
            persistent, source, status=BaseCoverageRecord.PERSISTENT_FAILURE
        )
        assert CoverageRecord.PERSISTENT_FAILURE == persistent_record.status

        # Here's a query that finds all four.
        qu = db.session.query(Identifier).outerjoin(CoverageRecord)
        assert 4 == qu.count()

        def check_not_covered(expect, **kwargs):
            missing = CoverageRecord.not_covered(**kwargs)
            assert sorted(expect) == sorted(qu.filter(missing).all())

        # By default, not_covered() only finds the identifier with no
        # coverage and the one with a transient failure.
        check_not_covered([no_coverage, transient])

        # If we pass in different values for covered_status, we change what
        # counts as 'coverage'. In this case, we allow transient failures
        # to count as 'coverage'.
        check_not_covered(
            [no_coverage],
            count_as_covered=[
                CoverageRecord.PERSISTENT_FAILURE,
                CoverageRecord.TRANSIENT_FAILURE,
                CoverageRecord.SUCCESS,
            ],
        )

        # Here, only success counts as 'coverage'.
        check_not_covered(
            [no_coverage, transient, persistent],
            count_as_covered=CoverageRecord.SUCCESS,
        )

        # We can also say that coverage doesn't count if it was achieved before
        # a certain time. Here, we'll show that passing in the timestamp
        # of the 'success' record means that record still counts as covered.
        check_not_covered(
            [no_coverage, transient],
            count_as_not_covered_if_covered_before=success_record.timestamp,
        )

        # But if we pass in a time one second later, the 'success'
        # record no longer counts as covered.
        assert isinstance(success_record.timestamp, datetime.datetime)
        one_second_after = success_record.timestamp + datetime.timedelta(seconds=1)
        check_not_covered(
            [success, no_coverage, transient],
            count_as_not_covered_if_covered_before=one_second_after,
        )


class TestCoverageRecord:
    def test_lookup(self, db: DatabaseTransactionFixture):
        source = DataSource.lookup(db.session, DataSource.OCLC)
        edition = db.edition()
        operation = "foo"
        collection = db.default_collection()
        record = db.coverage_record(edition, source, operation, collection=collection)

        # To find the CoverageRecord, edition, source, operation,
        # and collection must all match.
        result = CoverageRecord.lookup(
            edition, source, operation, collection=collection
        )
        assert record == result

        # You can substitute the Edition's primary identifier for the
        # Edition iteslf.
        lookup = CoverageRecord.lookup(
            edition.primary_identifier,
            source,
            operation,
            collection=db.default_collection(),
        )
        assert lookup == record

        # Omit the collection, and you find nothing.
        result = CoverageRecord.lookup(edition, source, operation)
        assert None == result

        # Same for operation.
        result = CoverageRecord.lookup(edition, source, collection=collection)
        assert None == result

        result = CoverageRecord.lookup(
            edition, source, "other operation", collection=collection
        )
        assert None == result

        # Same for data source.
        other_source = DataSource.lookup(db.session, DataSource.OVERDRIVE)
        result = CoverageRecord.lookup(
            edition, other_source, operation, collection=collection
        )
        assert None == result

    def test_add_for(self, db: DatabaseTransactionFixture):
        source = DataSource.lookup(db.session, DataSource.OCLC)
        edition = db.edition()
        operation = "foo"
        record, is_new = CoverageRecord.add_for(edition, source, operation)
        assert True == is_new

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = utc_now() - datetime.timedelta(days=7)
        record2, is_new = CoverageRecord.add_for(edition, source, operation, a_week_ago)
        assert record == record2
        assert False == is_new
        assert a_week_ago == record2.timestamp

        # If we don't specify an operation we get a totally different
        # record.
        record3, ignore = CoverageRecord.add_for(edition, source)
        assert record3 != record
        assert None == record3.operation
        seconds = (utc_now() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = CoverageRecord.lookup(edition.primary_identifier, source)
        assert record3 == record4

        # We can change the status.
        record5, is_new = CoverageRecord.add_for(
            edition, source, operation, status=CoverageRecord.PERSISTENT_FAILURE
        )
        assert record5 == record
        assert CoverageRecord.PERSISTENT_FAILURE == record.status

    def test_bulk_add(self, db: DatabaseTransactionFixture):
        source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        operation = "testing"

        # An untouched identifier.
        i1 = db.identifier()

        # An identifier that already has failing coverage.
        covered = db.identifier()
        existing = db.coverage_record(
            covered,
            source,
            operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            exception="Uh oh",
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation
        )

        # A new coverage record is created for the uncovered identifier.
        assert i1.coverage_records == resulting_records
        [new_record] = resulting_records
        assert source == new_record.data_source
        assert operation == new_record.operation
        assert CoverageRecord.SUCCESS == new_record.status
        assert None == new_record.exception

        # The existing coverage record is untouched.
        assert [covered] == ignored_identifiers
        assert [existing] == covered.coverage_records
        assert CoverageRecord.TRANSIENT_FAILURE == existing.status
        assert original_timestamp == existing.timestamp
        assert "Uh oh" == existing.exception

        # Newly untouched identifier.
        i2 = db.identifier()

        # Force bulk add.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation, force=True
        )

        # The new identifier has the expected coverage.
        [new_record] = i2.coverage_records
        assert new_record in resulting_records

        # The existing record has been updated.
        assert existing in resulting_records
        assert covered not in ignored_identifiers
        assert CoverageRecord.SUCCESS == existing.status
        assert isinstance(existing.timestamp, datetime.datetime)
        assert isinstance(original_timestamp, datetime.datetime)
        assert existing.timestamp > original_timestamp
        assert None == existing.exception

        # If no records are created or updated, no records are returned.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i2, covered], source, operation=operation
        )

        assert [] == resulting_records
        assert sorted([i2, covered]) == sorted(ignored_identifiers)

    def test_bulk_add_with_collection(self, db: DatabaseTransactionFixture):
        source = DataSource.lookup(db.session, DataSource.GUTENBERG)
        operation = "testing"

        c1 = db.collection()
        c2 = db.collection()

        # An untouched identifier.
        i1 = db.identifier()

        # An identifier with coverage for a different collection.
        covered = db.identifier()
        existing = db.coverage_record(
            covered,
            source,
            operation=operation,
            status=CoverageRecord.TRANSIENT_FAILURE,
            collection=c1,
            exception="Danger, Will Robinson",
        )
        original_timestamp = existing.timestamp

        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [i1, covered], source, operation=operation, collection=c1, force=True
        )

        assert 2 == len(resulting_records)
        assert [] == ignored_identifiers

        # A new record is created for the new identifier.
        [new_record] = i1.coverage_records
        assert new_record in resulting_records
        assert source == new_record.data_source
        assert operation == new_record.operation
        assert CoverageRecord.SUCCESS == new_record.status
        assert c1 == new_record.collection

        # The existing record has been updated.
        assert existing in resulting_records
        assert CoverageRecord.SUCCESS == existing.status
        assert isinstance(existing.timestamp, datetime.datetime)
        assert isinstance(original_timestamp, datetime.datetime)
        assert existing.timestamp > original_timestamp
        assert None == existing.exception

        # Bulk add for a different collection.
        resulting_records, ignored_identifiers = CoverageRecord.bulk_add(
            [covered],
            source,
            operation=operation,
            collection=c2,
            status=CoverageRecord.TRANSIENT_FAILURE,
            exception="Oh no",
        )

        # A new record has been added to the identifier.
        assert existing not in resulting_records
        [new_record] = resulting_records
        assert covered == new_record.identifier
        assert CoverageRecord.TRANSIENT_FAILURE == new_record.status
        assert source == new_record.data_source
        assert operation == new_record.operation
        assert "Oh no" == new_record.exception

    def test_assert_coverage_operation(self, db: DatabaseTransactionFixture):
        """Ensure all the methods that should raise errors, do raise the errors"""
        edition: Edition = db.edition()
        with pytest.raises(ValueError):
            CoverageRecord.add_for(
                edition,
                edition.data_source,
                CoverageRecord.IMPORT_OPERATION,
            )

        with pytest.raises(ValueError):
            CoverageRecord.lookup(
                edition,
                edition.data_source,
                CoverageRecord.IMPORT_OPERATION,
            )

        with pytest.raises(ValueError):
            CoverageRecord.bulk_add(
                [edition.primary_identifier],
                edition.data_source,
                CoverageRecord.IMPORT_OPERATION,
            )


class TestWorkCoverageRecord:
    def test_lookup(self, db: DatabaseTransactionFixture):
        work = db.work()
        operation = "foo"

        lookup = WorkCoverageRecord.lookup(work, operation)
        assert None == lookup

        record = db.work_coverage_record(work, operation)

        lookup = WorkCoverageRecord.lookup(work, operation)
        assert lookup == record

        assert None == WorkCoverageRecord.lookup(work, "another operation")

    def test_add_for(self, db: DatabaseTransactionFixture):
        work = db.work()
        operation = "foo"
        record, is_new = WorkCoverageRecord.add_for(work, operation)
        assert True == is_new

        # If we call add_for again we get the same record back, but we
        # can modify the timestamp.
        a_week_ago = utc_now() - datetime.timedelta(days=7)
        record2, is_new = WorkCoverageRecord.add_for(work, operation, a_week_ago)
        assert record == record2
        assert False == is_new
        assert a_week_ago == record2.timestamp

        # If we don't specify an operation we get a totally different
        # record.
        record3, ignore = WorkCoverageRecord.add_for(work, None)
        assert record3 != record
        assert None == record3.operation
        seconds = (utc_now() - record3.timestamp).seconds
        assert seconds < 10

        # If we call lookup we get the same record.
        record4 = WorkCoverageRecord.lookup(work, None)
        assert record3 == record4

        # We can change the status.
        record5, is_new = WorkCoverageRecord.add_for(
            work, operation, status=WorkCoverageRecord.PERSISTENT_FAILURE
        )
        assert record5 == record
        assert WorkCoverageRecord.PERSISTENT_FAILURE == record.status

    def test_bulk_add(self, db: DatabaseTransactionFixture):
        operation = "relevant"
        irrelevant_operation = "irrelevant"

        # This Work will get a new WorkCoverageRecord for the relevant
        # operation, even though it already has a WorkCoverageRecord
        # for an irrelevant operation.
        not_already_covered = db.work()
        irrelevant_record, ignore = WorkCoverageRecord.add_for(
            not_already_covered, irrelevant_operation, status=WorkCoverageRecord.SUCCESS
        )

        # This Work will have its existing, relevant CoverageRecord
        # updated.
        already_covered = db.work()
        previously_failed, ignore = WorkCoverageRecord.add_for(
            already_covered,
            operation,
            status=WorkCoverageRecord.TRANSIENT_FAILURE,
        )
        previously_failed.exception = "Some exception"

        # This work will not have a record created for it, because
        # we're not passing it in to the method.
        not_affected = db.work()
        WorkCoverageRecord.add_for(
            not_affected, irrelevant_operation, status=WorkCoverageRecord.SUCCESS
        )

        # This work will not have its existing record updated, because
        # we're not passing it in to the method.
        not_affected_2 = db.work()
        not_modified, ignore = WorkCoverageRecord.add_for(
            not_affected_2, operation, status=WorkCoverageRecord.SUCCESS
        )

        # Tell bulk_add to update or create WorkCoverageRecords for
        # not_already_covered and already_covered, but not not_affected.
        new_timestamp = utc_now()
        new_status = WorkCoverageRecord.REGISTERED
        WorkCoverageRecord.bulk_add(
            [not_already_covered, already_covered],
            operation,
            new_timestamp,
            status=new_status,
        )
        db.session.commit()

        def relevant_records(work):
            return [x for x in work.coverage_records if x.operation == operation]

        # No coverage records were added or modified for works not
        # passed in to the method.
        assert [] == relevant_records(not_affected)
        assert not_modified.timestamp < new_timestamp

        # The record associated with already_covered has been updated,
        # and its exception removed.
        [record] = relevant_records(already_covered)
        assert new_timestamp == record.timestamp
        assert new_status == record.status
        assert None == previously_failed.exception

        # A new record has been associated with not_already_covered
        [record] = relevant_records(not_already_covered)
        assert new_timestamp == record.timestamp
        assert new_status == record.status

        # The irrelevant WorkCoverageRecord is not affected by the update,
        # even though its Work was affected, because it's a record for
        # a different operation.
        assert WorkCoverageRecord.SUCCESS == irrelevant_record.status
        assert irrelevant_record.timestamp < new_timestamp


class ExampleEquivalencyCoverageRecordFixture:
    identifiers: List[Identifier]
    equivalencies: List[Equivalency]
    transaction: DatabaseTransactionFixture

    def __init__(self, transaction: DatabaseTransactionFixture):
        self.transaction = transaction
        self.identifiers = [
            transaction.identifier(),
            transaction.identifier(),
            transaction.identifier(),
            transaction.identifier(),
        ]
        idn = self.identifiers
        self.equivalencies = [
            Equivalency(input_id=idn[0].id, output_id=idn[1].id, strength=1),
            Equivalency(input_id=idn[1].id, output_id=idn[2].id, strength=1),
            Equivalency(input_id=idn[1].id, output_id=idn[0].id, strength=1),
        ]
        session = transaction.session
        session.add_all(self.equivalencies)
        session.commit()


@pytest.fixture()
def example_equivalency_coverage_record_fixture(
    db,
) -> ExampleEquivalencyCoverageRecordFixture:
    return ExampleEquivalencyCoverageRecordFixture(db)


class TestEquivalencyCoverageRecord:
    def test_add_for(
        self,
        example_equivalency_coverage_record_fixture: ExampleEquivalencyCoverageRecordFixture,
    ):
        operation = EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH
        equivalencies = example_equivalency_coverage_record_fixture.equivalencies
        session = example_equivalency_coverage_record_fixture.transaction.session

        for eq in equivalencies:
            record, is_new = EquivalencyCoverageRecord.add_for(
                eq, operation, status=CoverageRecord.REGISTERED
            )

            assert record.equivalency_id == eq.id
            assert record.status == CoverageRecord.REGISTERED
            assert record.operation == operation

    def test_bulk_add(
        self,
        example_equivalency_coverage_record_fixture: ExampleEquivalencyCoverageRecordFixture,
    ):
        equivalencies = example_equivalency_coverage_record_fixture.equivalencies
        session = example_equivalency_coverage_record_fixture.transaction.session

        operation = EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH
        EquivalencyCoverageRecord.bulk_add(session, equivalencies, operation)
        all_records = session.query(EquivalencyCoverageRecord).all()

        assert len(all_records) == 3
        # All equivalencies are the same
        assert {r.equivalency_id for r in all_records} == {e.id for e in equivalencies}

    def test_delete_identifier(
        self,
        example_equivalency_coverage_record_fixture: ExampleEquivalencyCoverageRecordFixture,
    ):
        equivalencies = example_equivalency_coverage_record_fixture.equivalencies
        session = example_equivalency_coverage_record_fixture.transaction.session

        for eq in equivalencies:
            record, is_new = EquivalencyCoverageRecord.add_for(
                eq,
                EquivalencyCoverageRecord.RECURSIVE_EQUIVALENCY_REFRESH,
                status=CoverageRecord.REGISTERED,
            )
        session.commit()

        all_equivs = session.query(EquivalencyCoverageRecord).all()
        assert len(all_equivs) == 3

        session.delete(example_equivalency_coverage_record_fixture.identifiers[0])
        session.commit()

        all_equivs = session.query(EquivalencyCoverageRecord).all()
        assert len(all_equivs) == 1
