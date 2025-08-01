import datetime
from typing import cast

import pytest

from palace.manager.core.coverage import (
    BaseCoverageProvider,
    CoverageFailure,
    CoverageProviderProgress,
    IdentifierCoverageProvider,
)
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.data_layer.format import FormatData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.link import LinkData
from palace.manager.data_layer.policy.presentation import (
    PresentationCalculationPolicy,
)
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.data_layer.subject import SubjectData
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.opds.opds1 import OPDSAPI
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.classification import Subject
from palace.manager.sqlalchemy.model.collection import CollectionMissing
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.coverage import (
    CoverageRecord,
    Timestamp,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink, Representation
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.mock import (
    AlwaysSuccessfulBibliographicCoverageProvider,
    AlwaysSuccessfulCollectionCoverageProvider,
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulBibliographicCoverageProvider,
    NeverSuccessfulCoverageProvider,
    TaskIgnoringCoverageProvider,
    TransientFailureCoverageProvider,
)


class TestCoverageProviderProgress:
    def test_achievements(self):
        progress = CoverageProviderProgress()
        progress.successes = 1
        progress.transient_failures = 2
        progress.persistent_failures = 0

        expect = "Items processed: 3. Successes: 1, transient failures: 2, persistent failures: 0"
        assert expect == progress.achievements

        # You can't set .achievements directly -- it's a calculated value.
        progress.achievements = "new value"
        assert expect == progress.achievements


@pytest.fixture
def bibliographic_data() -> BibliographicData:
    return BibliographicData(
        data_source_name=DataSource.OVERDRIVE,
        publisher="Perfection Learning",
        language="eng",
        title="A Girl Named Disaster",
        published=datetime_utc(1998, 3, 1, 0, 0),
        primary_identifier_data=IdentifierData(
            type=Identifier.OVERDRIVE_ID,
            identifier="ba9b3419-b0bd-4ca7-a24f-26c4246b6b44",
        ),
        identifiers=[
            IdentifierData(
                type=Identifier.OVERDRIVE_ID,
                identifier="ba9b3419-b0bd-4ca7-a24f-26c4246b6b44",
            ),
            IdentifierData(type=Identifier.ISBN, identifier="9781402550805"),
        ],
        contributors=[
            ContributorData(
                sort_name="Nancy Farmer", roles=[Contributor.Role.PRIMARY_AUTHOR]
            )
        ],
        subjects=[
            SubjectData(type=Subject.TOPIC, identifier="Action & Adventure"),
            SubjectData(type=Subject.FREEFORM_AUDIENCE, identifier="Young Adult"),
            SubjectData(type=Subject.PLACE, identifier="Africa"),
        ],
    )


@pytest.fixture
def circulation_data(bibliographic_data: BibliographicData) -> CirculationData:
    # This data is used to test the insertion of circulation data
    # into a Collection.
    return CirculationData(
        data_source_name=DataSource.OVERDRIVE,
        primary_identifier_data=bibliographic_data.primary_identifier_data,
        formats=[
            FormatData(
                content_type=Representation.EPUB_MEDIA_TYPE,
                drm_scheme=DeliveryMechanism.NO_DRM,
                rights_uri=RightsStatus.IN_COPYRIGHT,
            )
        ],
    )


class TestBaseCoverageProvider:
    def test_instantiation(self, db: DatabaseTransactionFixture):
        """Verify variable initialization."""

        class ValidMock(BaseCoverageProvider):
            SERVICE_NAME = "A Service"
            OPERATION = "An Operation"
            DEFAULT_BATCH_SIZE = 50

        now = cutoff_time = utc_now()
        provider = ValidMock(db.session, cutoff_time=now)

        # Class variables defined in subclasses become appropriate
        # instance variables.
        assert "A Service (An Operation)" == provider.service_name
        assert "An Operation" == provider.operation
        assert 50 == provider.batch_size
        assert now == provider.cutoff_time

        # If you pass in an invalid value for batch_size, you get the default.
        provider = ValidMock(db.session, batch_size=-10)
        assert 50 == provider.batch_size

    def test_subclass_must_define_service_name(self, db: DatabaseTransactionFixture):
        class NoServiceName(BaseCoverageProvider):
            pass

        with pytest.raises(ValueError) as excinfo:
            NoServiceName(db.session)
        assert "NoServiceName must define SERVICE_NAME" in str(excinfo.value)

    def test_run(self, db: DatabaseTransactionFixture):
        """Verify that run() calls run_once_and_update_timestamp()."""

        class MockProvider(BaseCoverageProvider):
            SERVICE_NAME = "I do nothing"
            was_run = False

            def run_once_and_update_timestamp(self):
                """Set a variable."""
                self.was_run = True
                return None

        provider = MockProvider(db.session)
        result = provider.run()

        # run_once_and_update_timestamp() was called.
        assert True == provider.was_run

        # run() returned a CoverageProviderProgress with basic
        # timing information, since run_once_and_update_timestamp()
        # didn't provide anything.
        assert isinstance(result, CoverageProviderProgress)
        now = utc_now()
        assert isinstance(result.start, datetime.datetime)
        assert isinstance(result.finish, datetime.datetime)
        assert result.start < result.finish
        for time in (result.start, result.finish):
            assert (now - time).total_seconds() < 5

    def test_run_with_custom_result(self, db: DatabaseTransactionFixture):
        start = datetime_utc(2011, 1, 1)
        finish = datetime_utc(2012, 1, 1)
        counter = -100

        class MockProvider(BaseCoverageProvider):
            """A BaseCoverageProvider that returns a strange
            CoverageProviderProgress representing the work it did.
            """

            SERVICE_NAME = "I do nothing"
            was_run = False

            custom_timestamp_data = CoverageProviderProgress(
                start=start, finish=finish, counter=counter
            )

            def run_once_and_update_timestamp(self):
                return self.custom_timestamp_data

        provider = MockProvider(db.session)
        result = provider.run()

        # The TimestampData returned by run_once_and_update_timestamp
        # is the return value of run().
        assert result == provider.custom_timestamp_data

        # The TimestampData data was written to the database, even
        # though some of it doesn't make apparent sense.
        assert start == provider.timestamp.start
        assert finish == provider.timestamp.finish
        assert counter == provider.timestamp.counter

    def test_run_once_and_update_timestamp(self, db: DatabaseTransactionFixture):
        """Test that run_once_and_update_timestamp calls run_once until all
        the work is done, and then updates a Timestamp.
        """

        class MockProvider(BaseCoverageProvider):
            SERVICE_NAME = "I do nothing"
            run_once_calls = []
            expect_offset = 0

            def run_once(self, progress, count_as_covered=None):
                now = utc_now()

                # We never see progress.finish set to a non-None
                # value. When _we_ set it to a non-None value, it means
                # the work is done. If we get called again, it'll be
                # with different `count_as_covered` settings, and
                # .finish will have been reset to None.
                assert None == progress.finish

                # Verify that progress.offset is cleared when we
                # expect, and left alone when we expect. This lets
                assert self.expect_offset == progress.offset

                self.run_once_calls.append((count_as_covered, now))
                progress.offset = len(self.run_once_calls)

                if len(self.run_once_calls) == 1:
                    # This is the first call. We will not be setting
                    # .finish, so the offset will not be reset on the
                    # next call. This simulates what happens when a
                    # given `count_as_covered` setting can't be
                    # handled in one batch.
                    self.expect_offset = progress.offset
                else:
                    # This is the second or third call. Set .finish to
                    # indicate we're done with this `count_as_covered`
                    # setting.
                    progress.finish = now

                    # If there is another call, progress.offset will be
                    # reset to zero. (So will .finish.)
                    self.expect_offset = 0
                return progress

        # We start with no Timestamp.
        service_name = "I do nothing"
        service_type = Timestamp.COVERAGE_PROVIDER_TYPE
        timestamp = Timestamp.value(
            db.session, service_name, service_type, collection=None
        )
        assert None == timestamp

        # Instantiate the Provider, and call
        # run_once_and_update_timestamp.
        provider = MockProvider(db.session)
        final_progress = provider.run_once_and_update_timestamp()

        # The Timestamp's .start and .finish are now set to recent
        # values -- the start and end points of run_once().
        timestamp = provider.timestamp
        now = utc_now()
        assert (now - timestamp.start).total_seconds() < 1
        assert (now - timestamp.finish).total_seconds() < 1
        assert timestamp.start < timestamp.finish

        # run_once was called three times: twice to exclude items that
        # have any coverage record whatsoever (PREVIOUSLY_ATTEMPTED),
        # and a third time to exclude only items that have coverage
        # records that indicate success or persistent failure
        # (DEFAULT_COUNT_AS_COVERED).
        first_call, second_call, third_call = provider.run_once_calls
        assert CoverageRecord.PREVIOUSLY_ATTEMPTED == first_call[0]
        assert CoverageRecord.PREVIOUSLY_ATTEMPTED == second_call[0]
        assert CoverageRecord.DEFAULT_COUNT_AS_COVERED == third_call[0]

        # On the second and third calls, final_progress.finish was set
        # to the current time, and .offset was set to the number of
        # calls so far.
        #
        # These values are cleared out before each run_once() call
        # -- we tested that above -- so the surviving values are the
        # ones associated with the third call.
        assert third_call[1] == final_progress.finish
        assert 3 == final_progress.offset

    def test_run_once_and_update_timestamp_catches_exception(
        self, db: DatabaseTransactionFixture
    ):
        # Test that run_once_and_update_timestamp catches an exception
        # and stores a stack trace in the CoverageProvider's Timestamp.
        class MockProvider(BaseCoverageProvider):
            SERVICE_NAME = "I fail"

            def run_once(self, progress, count_as_covered=None):
                raise Exception("Unhandled exception")

        provider = MockProvider(db.session)
        provider.run_once_and_update_timestamp()

        timestamp = provider.timestamp
        now = utc_now()
        assert (now - timestamp.start).total_seconds() < 1
        assert (now - timestamp.finish).total_seconds() < 1
        assert timestamp.start < timestamp.finish

        assert "Exception: Unhandled exception" in timestamp.exception

    def test_run_once_and_update_timestamp_handled_exception(
        self, db: DatabaseTransactionFixture
    ):
        # Test that run_once_and_update_timestamp handles the
        # case where the run_once() implementation sets TimestampData.exception
        # rather than raising an exception.
        #
        # This also tests the case where run_once() modifies the
        # TimestampData in place rather than returning a new one.
        class MockProvider(BaseCoverageProvider):
            SERVICE_NAME = "I fail"

            def run_once(self, progress, count_as_covered=None):
                progress.exception = "oops"

        provider = MockProvider(db.session)
        provider.run_once_and_update_timestamp()

        timestamp = provider.timestamp
        now = utc_now()
        assert (now - timestamp.start).total_seconds() < 1
        assert (now - timestamp.finish).total_seconds() < 1
        assert timestamp.start < timestamp.finish

        assert "oops" == timestamp.exception

    def test_run_once(self, db: DatabaseTransactionFixture):
        # Test run_once, showing how it covers items with different types of
        # CoverageRecord.

        # We start with no CoverageRecords.
        assert [] == db.session.query(CoverageRecord).all()

        # Four identifiers.
        transient = db.identifier()
        persistent = db.identifier()
        uncovered = db.identifier()
        covered = db.identifier()

        # This provider will try to cover them.
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        data_source = provider.data_source

        # We previously tried to cover one of them, but got a
        # transient failure.
        db.coverage_record(
            transient, data_source, status=CoverageRecord.TRANSIENT_FAILURE
        )

        # Another of the four has a persistent failure.
        db.coverage_record(
            persistent, data_source, status=CoverageRecord.PERSISTENT_FAILURE
        )

        # The third one has no coverage record at all.

        # And the fourth one has been successfully covered.
        db.coverage_record(covered, data_source, status=CoverageRecord.SUCCESS)

        # Now let's run the coverage provider. Every Identifier
        # that's covered will succeed, so the question is which ones
        # get covered.
        progress = CoverageProviderProgress()
        assert 0 == progress.offset
        result = provider.run_once(progress)

        # The TimestampData we passed in was given back to us.
        assert progress == result

        # The offset (an extension specific to
        # CoverageProviderProgress, not stored in the database)
        # has not changed -- if we were to call run_once again we
        # would not need to skip any records.
        assert 0 == progress.offset

        # Various internal totals were updated and a value for .achievements
        # can be generated from those totals.
        assert 2 == progress.successes

        # By default, run_once() finds Identifiers that have no coverage
        # or which have transient failures.
        [transient_failure_has_gone] = transient.coverage_records
        assert CoverageRecord.SUCCESS == transient_failure_has_gone.status

        [now_has_coverage] = uncovered.coverage_records
        assert CoverageRecord.SUCCESS == now_has_coverage.status

        assert transient in provider.attempts
        assert uncovered in provider.attempts

        # Nothing happened to the identifier that had a persistent
        # failure or the identifier that was successfully covered.
        assert [CoverageRecord.PERSISTENT_FAILURE] == [
            x.status for x in persistent.coverage_records
        ]
        assert [CoverageRecord.SUCCESS] == [x.status for x in covered.coverage_records]

        assert persistent not in provider.attempts
        assert covered not in provider.attempts

        # We can change which identifiers get processed by changing
        # what counts as 'coverage'.
        result = provider.run_once(progress, count_as_covered=[CoverageRecord.SUCCESS])
        assert progress == result
        assert 0 == progress.offset

        # That processed the persistent failure, but not the success.
        assert persistent in provider.attempts
        assert covered not in provider.attempts

        # Let's call it again and say that we are covering everything
        # _except_ persistent failures.
        result = provider.run_once(
            progress, count_as_covered=[CoverageRecord.PERSISTENT_FAILURE]
        )
        assert progress == result

        # That got us to cover the identifier that had already been
        # successfully covered.
        assert covered in provider.attempts

        # *Now* the offset has changed, so that the first four results
        # -- which we've decided to skip -- won't be considered again
        # this run.
        assert 4 == progress.offset

    def test_run_once_records_successes_and_failures(
        self, db: DatabaseTransactionFixture
    ):
        class Mock(AlwaysSuccessfulCoverageProvider):
            def process_batch_and_handle_results(self, batch):
                # Simulate 1 success, 2 transient failures,
                # and 3 persistent failures.
                return (1, 2, 3), []

        # process_batch_and_handle_results won't even be called if the
        # batch is empty.
        provider = Mock(db.session)
        progress = CoverageProviderProgress()
        progress2 = provider.run_once(progress)
        assert progress2 == progress
        assert 0 == progress.successes

        # Let's register an identifier so that the method we're testing
        # will be called.
        needs_coverage = db.identifier()
        progress = provider.run_once(progress)

        # The numbers returned from process_batch_and_handle_results
        # were added to the CoverageProviderProgress object.
        assert 1 == progress.successes
        assert 2 == progress.transient_failures
        assert 3 == progress.persistent_failures
        assert provider.finalize_batch_called

        assert (
            "Items processed: 6. Successes: 1, transient failures: 2, persistent failures: 3"
            == progress.achievements
        )

    def test_process_batch_and_handle_results(self, db: DatabaseTransactionFixture):
        """Test that process_batch_and_handle_results passes the identifiers
        its given into the appropriate BaseCoverageProvider, and deals
        correctly with the successes and failures it might return.
        """
        e1, p1 = db.edition(with_license_pool=True)
        i1 = e1.primary_identifier

        e2, p2 = db.edition(with_license_pool=True)
        i2 = e2.primary_identifier

        class MockProvider1(AlwaysSuccessfulCoverageProvider):
            OPERATION = "i succeed"

        success_provider = MockProvider1(db.session)

        batch = [i1, i2]
        counts, successes = success_provider.process_batch_and_handle_results(batch)

        # Two successes.
        assert (2, 0, 0) == counts

        # Each represented with a CoverageRecord with status='success'
        assert all(isinstance(x, CoverageRecord) for x in successes)
        assert [CoverageRecord.SUCCESS] * 2 == [x.status for x in successes]

        # Each associated with one of the identifiers...
        assert {i1, i2} == {x.identifier for x in successes}

        # ...and with the coverage provider's operation.
        assert ["i succeed"] * 2 == [x.operation for x in successes]

        # Now try a different CoverageProvider which creates transient
        # failures.
        class MockProvider2(TransientFailureCoverageProvider):
            OPERATION = "i fail transiently"

        transient_failure_provider = MockProvider2(db.session)
        counts, failures = transient_failure_provider.process_batch_and_handle_results(
            batch
        )
        # Two transient failures.
        assert (0, 2, 0) == counts

        # New coverage records were added to track the transient
        # failures.
        assert [CoverageRecord.TRANSIENT_FAILURE] * 2 == [x.status for x in failures]
        assert ["i fail transiently"] * 2 == [x.operation for x in failures]

        # Another way of getting transient failures is to just ignore every
        # item you're told to process.
        class MockProvider3(TaskIgnoringCoverageProvider):
            OPERATION = "i ignore"

        task_ignoring_provider = MockProvider3(db.session)
        counts, records = task_ignoring_provider.process_batch_and_handle_results(batch)

        assert (0, 2, 0) == counts
        assert [CoverageRecord.TRANSIENT_FAILURE] * 2 == [x.status for x in records]
        assert ["i ignore"] * 2 == [x.operation for x in records]

        # If a transient failure becomes a success, the it won't have
        # an exception anymore.
        assert ["Was ignored by CoverageProvider."] * 2 == [
            x.exception for x in records
        ]
        records = success_provider.process_batch_and_handle_results(batch)[1]
        assert [None, None] == [x.exception for x in records]

        # Or you can go really bad and have persistent failures.
        class MockProvider4(NeverSuccessfulCoverageProvider):
            OPERATION = "i will always fail"

        persistent_failure_provider = MockProvider4(db.session)
        counts, results = persistent_failure_provider.process_batch_and_handle_results(
            batch
        )

        # Two persistent failures.
        assert (0, 0, 2) == counts
        assert all([isinstance(x, CoverageRecord) for x in results])
        assert ["What did you expect?", "What did you expect?"] == [
            x.exception for x in results
        ]
        assert [CoverageRecord.PERSISTENT_FAILURE] * 2 == [x.status for x in results]
        assert ["i will always fail"] * 2 == [x.operation for x in results]

        assert not success_provider.finalize_batch_called
        assert not persistent_failure_provider.finalize_batch_called

    def test_process_batch(self, db: DatabaseTransactionFixture):
        class Mock(BaseCoverageProvider):
            SERVICE_NAME = "Some succeed, some fail."

            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.processed = []
                self.successes = []

            def process_item(self, item):
                self.processed.append(item)
                if item.identifier == "fail":
                    return CoverageFailure(item, "oops")
                return item

            def handle_success(self, item):
                self.successes.append(item)

        # Two Identifiers. One will succeed, one will fail.
        succeed = db.identifier(foreign_id="succeed")
        fail = db.identifier(foreign_id="fail")
        provider = Mock(db.session)

        r1, r2 = provider.process_batch([succeed, fail])

        # Here's the success.
        assert r1 == succeed

        # Here's the failure.
        assert isinstance(r2, CoverageFailure)
        assert "oops" == r2.exception

        # Both identifiers were added to .processed, indicating that
        # process_item was called twice, but only the success was
        # added to .success, indicating that handle_success was only
        # called once.
        assert [succeed, fail] == provider.processed
        assert [succeed] == provider.successes

    def test_should_update(self, db: DatabaseTransactionFixture):
        """Verify that should_update gives the correct answer when we
        ask if a CoverageRecord needs to be updated.
        """
        cutoff = datetime_utc(2016, 1, 1)
        provider = AlwaysSuccessfulCoverageProvider(db.session, cutoff_time=cutoff)
        identifier = db.identifier()

        # If coverage is missing, we should update.
        assert True == provider.should_update(None)

        # If coverage is outdated, we should update.
        record, ignore = CoverageRecord.add_for(identifier, provider.data_source)
        record.timestamp = datetime_utc(2015, 1, 1)
        assert True == provider.should_update(record)

        # If coverage is up-to-date, we should not update.
        record.timestamp = cutoff
        assert False == provider.should_update(record)

        # If coverage is only 'registered', we should update.
        record.status = CoverageRecord.REGISTERED
        assert True == provider.should_update(record)


class TestIdentifierCoverageProvider:
    def test_input_identifier_types(self, db: DatabaseTransactionFixture):
        """Test various acceptable and unacceptable values for the class
        variable INPUT_IDENTIFIER_TYPES.
        """

        # It's okay to set INPUT_IDENTIFIER_TYPES to None it means you
        # will cover any and all identifier types.
        class Base(IdentifierCoverageProvider):
            SERVICE_NAME = "Test provider"
            DATA_SOURCE_NAME = DataSource.GUTENBERG

        class MockProvider1(Base):
            INPUT_IDENTIFIER_TYPES = None

        provider = MockProvider1(db.session)
        assert None == provider.input_identifier_types

        # It's okay to set a single value.
        class MockProvider2(Base):
            INPUT_IDENTIFIER_TYPES = Identifier.ISBN

        provider2 = MockProvider2(db.session)
        assert [Identifier.ISBN] == provider2.input_identifier_types

        # It's okay to set a list of values.
        class MockProvider3(Base):
            INPUT_IDENTIFIER_TYPES = [Identifier.ISBN, Identifier.OVERDRIVE_ID]

        provider3 = MockProvider3(db.session)
        assert [
            Identifier.ISBN,
            Identifier.OVERDRIVE_ID,
        ] == provider3.input_identifier_types

        # It's not okay to do nothing.
        class MockProvider4(Base):
            pass

        with pytest.raises(ValueError) as excinfo:
            MockProvider4(db.session)
        assert (
            "MockProvider4 must define INPUT_IDENTIFIER_TYPES, even if the value is None."
            in str(excinfo.value)
        )

    def test_can_cover(self, db: DatabaseTransactionFixture):
        """Verify that can_cover gives the correct answer when
        asked if an IdentifierCoverageProvider can handle a given Identifier.
        """
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        identifier = db.identifier(identifier_type=Identifier.ISBN)
        m = provider.can_cover

        # This provider handles all identifier types.
        provider.input_identifier_types = None
        assert True == m(identifier)

        # This provider handles ISBNs.
        provider.input_identifier_types = [Identifier.OVERDRIVE_ID, Identifier.ISBN]
        assert True == m(identifier)

        # This provider doesn't.
        provider.input_identifier_types = [Identifier.OVERDRIVE_ID]
        assert False == m(identifier)

    def test_replacement_policy(self, db: DatabaseTransactionFixture):
        """Unless a different replacement policy is passed in, the
        default is ReplacementPolicy.from_metadata_source().
        """
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        assert True == provider.replacement_policy.identifiers
        assert False == provider.replacement_policy.formats

        policy = ReplacementPolicy.from_license_source(db.session)
        provider = AlwaysSuccessfulCoverageProvider(
            db.session, replacement_policy=policy
        )
        assert policy == provider.replacement_policy

    def test_register(self, db: DatabaseTransactionFixture):
        # The identifier has no coverage.
        identifier = db.identifier()
        assert 0 == len(identifier.coverage_records)

        provider = AlwaysSuccessfulCoverageProvider

        # If a CoverageRecord doesn't exist for the provider,
        # a 'registered' record is created.
        new_record, was_registered = provider.register(identifier)

        assert identifier.coverage_records == [new_record]
        assert provider.DATA_SOURCE_NAME == new_record.data_source.name
        assert CoverageRecord.REGISTERED == new_record.status
        assert None == new_record.exception

        # If a CoverageRecord exists already, it's returned.
        existing = new_record
        existing.status = CoverageRecord.SUCCESS

        new_record, was_registered = provider.register(identifier)
        assert existing == new_record
        assert False == was_registered
        # Its details haven't been changed in any way.
        assert CoverageRecord.SUCCESS == new_record.status
        assert None == new_record.exception

    def test_bulk_register(self, db: DatabaseTransactionFixture):
        provider = AlwaysSuccessfulCoverageProvider
        source = DataSource.lookup(db.session, provider.DATA_SOURCE_NAME)

        i1 = db.identifier()
        covered = db.identifier()
        existing = db.coverage_record(covered, source, operation=provider.OPERATION)

        new_records, ignored_identifiers = provider.bulk_register([i1, covered])

        assert i1.coverage_records == new_records
        [new_record] = new_records
        assert provider.DATA_SOURCE_NAME == new_record.data_source.name
        assert provider.OPERATION == new_record.operation
        assert CoverageRecord.REGISTERED == new_record.status

        assert [covered] == ignored_identifiers
        # The existing CoverageRecord hasn't been changed.
        assert CoverageRecord.SUCCESS == existing.status

    def test_bulk_register_can_overwrite_existing_record_status(
        self, db: DatabaseTransactionFixture
    ):
        provider = AlwaysSuccessfulCoverageProvider
        identifier = db.identifier()

        # Create an existing record, and give it a SUCCESS status.
        provider.bulk_register([identifier])
        [existing] = identifier.coverage_records
        existing.status = CoverageRecord.SUCCESS
        db.session.commit()

        # If registration is forced, an existing record is updated.
        records, ignored = provider.bulk_register([identifier], force=True)
        assert [existing] == records
        assert CoverageRecord.REGISTERED == existing.status

    def test_bulk_register_with_collection(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        provider = AlwaysSuccessfulCoverageProvider
        collection = db.collection(protocol=BoundlessApi)

        try:
            # If a DataSource or data source name is provided and
            # autocreate is set True, the record is created with that source.
            provider.bulk_register(
                [identifier],
                data_source=collection.name,
                collection=collection,
                autocreate=True,
            )
            [record] = identifier.coverage_records

            # A DataSource with the given name has been created.
            collection_source = DataSource.lookup(db.session, collection.name)
            assert collection_source
            assert provider.DATA_SOURCE_NAME != record.data_source.name
            assert collection_source == record.data_source

            # Even though a collection was given, the record's collection isn't
            # set.
            assert None == record.collection

            # However, when coverage is collection-specific the
            # CoverageRecord is related to the given collection.
            provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False

            provider.bulk_register(
                [identifier], collection_source, collection=collection
            )
            records = identifier.coverage_records
            assert 2 == len(records)
            assert [r for r in records if r.collection == collection]
        finally:
            # Return the mock class to its original state for other tests.
            provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = True

    def test_ensure_coverage(self, db: DatabaseTransactionFixture):
        """Verify that ensure_coverage creates a CoverageRecord for an
        Identifier, assuming that the CoverageProvider succeeds.
        """
        identifier = db.identifier()
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())
        provider.OPERATION = db.fresh_str()
        record = provider.ensure_coverage(identifier)
        assert isinstance(record, CoverageRecord)
        assert identifier == record.identifier
        assert provider.data_source == record.data_source
        assert provider.OPERATION == record.operation
        assert None == record.exception

        # There is now one CoverageRecord -- the one returned by
        # ensure_coverage().
        [record2] = db.session.query(CoverageRecord).all()
        assert record2 == record

        # Because this provider counts coverage in one Collection as
        # coverage for all Collections, the coverage record was not
        # associated with any particular collection.
        assert True == provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION
        assert None == record2.collection

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage on a single record.
        assert None == Timestamp.value(
            db.session,
            provider.service_name,
            Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )

        # Now let's try a CollectionCoverageProvider that needs to
        # grant coverage separately for every collection.
        provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        record3 = provider.ensure_coverage(identifier)

        # This creates a new CoverageRecord associated with the
        # provider's collection.
        assert record3 != record2
        assert provider.collection == record3.collection

    def test_ensure_coverage_works_on_edition(self, db: DatabaseTransactionFixture):
        """Verify that ensure_coverage() works on an Edition by covering
        its primary identifier.
        """
        edition = db.edition()
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        record = provider.ensure_coverage(edition)
        assert isinstance(record, CoverageRecord)
        assert edition.primary_identifier == record.identifier

    def test_ensure_coverage_respects_operation(self, db: DatabaseTransactionFixture):
        # Two providers with the same output source but different operations.
        class Mock1(AlwaysSuccessfulCoverageProvider):
            OPERATION = "foo"

        provider1 = Mock1(db.session)

        class Mock2(NeverSuccessfulCoverageProvider):
            OPERATION = "bar"

        provider2 = Mock2(db.session)

        identifier = db.identifier()
        # Ensure coverage from both providers.
        coverage1 = provider1.ensure_coverage(identifier)
        assert "foo" == coverage1.operation
        old_timestamp = coverage1.timestamp

        coverage2 = provider2.ensure_coverage(identifier)
        assert "bar" == coverage2.operation

        # There are now two CoverageRecords, one for each operation.
        assert {coverage1, coverage2} == set(db.session.query(CoverageRecord))

        # If we try to ensure coverage again, no work is done and we
        # get the old coverage record back.
        new_coverage = provider1.ensure_coverage(identifier)
        assert new_coverage == coverage1
        new_coverage.timestamp = old_timestamp

    def test_ensure_coverage_persistent_coverage_failure(
        self, db: DatabaseTransactionFixture
    ):
        identifier = db.identifier()
        provider = NeverSuccessfulCoverageProvider(db.session)
        failure = provider.ensure_coverage(identifier)

        # A CoverageRecord has been created to memorialize the
        # persistent failure.
        assert isinstance(failure, CoverageRecord)
        assert "What did you expect?" == failure.exception

        # Here it is in the database.
        [record] = db.session.query(CoverageRecord).all()
        assert record == failure

        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage.
        # The coverage provider's timestamp was not updated, because
        # we're using ensure_coverage on a single record.
        assert None == Timestamp.value(
            db.session,
            provider.service_name,
            service_type=Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )

    def test_ensure_coverage_transient_coverage_failure(
        self, db: DatabaseTransactionFixture
    ):
        identifier = db.identifier()
        provider = TransientFailureCoverageProvider(db.session)
        failure = provider.ensure_coverage(identifier)
        assert [failure] == identifier.coverage_records
        assert CoverageRecord.TRANSIENT_FAILURE == failure.status
        assert "Oops!" == failure.exception

        # Timestamp was not updated.
        assert None == Timestamp.value(
            db.session,
            provider.service_name,
            service_type=Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )

    def test_ensure_coverage_changes_status(self, db: DatabaseTransactionFixture):
        """Verify that processing an item that has a preexisting
        CoverageRecord can change the status of that CoverageRecord.
        """

        identifier = db.identifier()
        always = AlwaysSuccessfulCoverageProvider(db.session)
        persistent = NeverSuccessfulCoverageProvider(db.session)
        transient = TransientFailureCoverageProvider(db.session)

        # Cover the same identifier multiple times, simulating all
        # possible states of a CoverageRecord. The same CoverageRecord
        # is used every time and the status is changed appropriately
        # after every run.
        c1 = persistent.ensure_coverage(identifier, force=True)
        assert CoverageRecord.PERSISTENT_FAILURE == c1.status

        c2 = transient.ensure_coverage(identifier, force=True)
        assert c2 == c1
        assert CoverageRecord.TRANSIENT_FAILURE == c1.status

        c3 = always.ensure_coverage(identifier, force=True)
        assert c3 == c1
        assert CoverageRecord.SUCCESS == c1.status

        c4 = persistent.ensure_coverage(identifier, force=True)
        assert c4 == c1
        assert CoverageRecord.PERSISTENT_FAILURE == c1.status

    def test_edition(self, db: DatabaseTransactionFixture):
        """Verify that CoverageProvider.edition() returns an appropriate
        Edition, even when there is no associated Collection.
        """
        # This CoverageProvider fetches bibliographic information
        # from Overdrive. It is not capable of creating LicensePools
        # because it has no Collection.
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        assert None == provider.collection

        # Here's an Identifier, with no Editions.
        identifier = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        assert [] == identifier.primarily_identifies

        # Calling CoverageProvider.edition() on the Identifier gives
        # us a container for the provider's bibliographic information,
        # as given to us by the provider's data source.
        #
        # It doesn't matter that there's no Collection, because the
        # book's bibliographic information is the same across
        # Collections.
        edition = provider.edition(identifier)
        assert provider.data_source == edition.data_source
        assert [edition] == identifier.primarily_identifies

        # Calling edition() again gives us the same Edition as before.
        edition2 = provider.edition(identifier)
        assert edition == edition2

    def test_set_bibliographic(
        self, bibliographic_data, db: DatabaseTransactionFixture
    ):
        """Test that set_bibliographic can create and populate an
        appropriate Edition.

        set_bibliographic is tested in more detail in
        TestCollectionCoverageProvider.
        """
        # Here's a provider that is not associated with any particular
        # Collection.
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        assert None == provider.collection

        # It can't set circulation data, because it's not a
        # CollectionCoverageProvider.
        assert not hasattr(provider, "set_bibliographic_and_circulation_data")

        # But it can set bibliographic data.
        identifier = db.identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id=bibliographic_data.primary_identifier_data.identifier,
        )
        assert [] == identifier.primarily_identifies
        result = provider.set_bibliographic(identifier, bibliographic_data)

        # Here's the proof.
        edition = provider.edition(identifier)
        assert "A Girl Named Disaster" == edition.title

        # If no bibliographic data is passed in, a CoverageFailure results.
        result = provider.set_bibliographic(identifier, None)
        assert isinstance(result, CoverageFailure)
        assert (
            "Did not receive bibliographic data from input source" == result.exception
        )

        # If there's an exception setting the bibliographic data, a
        # CoverageFailure results. This call raises a ValueError
        # because the primary identifier & the edition's primary
        # identifier don't match.
        bibliographic_data.primary_identifier_data = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier="abcd"
        )
        result = provider.set_bibliographic(identifier, bibliographic_data)
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception

    def test_items_that_need_coverage_respects_registration_reqs(
        self, db: DatabaseTransactionFixture
    ):
        identifier = db.identifier()
        provider = AlwaysSuccessfulCoverageProvider(db.session, registered_only=True)

        items = provider.items_that_need_coverage()
        assert identifier not in items

        # Once the identifier is registered, it shows up.
        provider.register(identifier)
        assert identifier in items

        # With a failing CoverageRecord, the item shows up.
        [record] = identifier.coverage_records
        record.status = CoverageRecord.TRANSIENT_FAILURE
        record.exception = "Oh no!"
        assert identifier in items

    def test_items_that_need_coverage_respects_operation(
        self, db: DatabaseTransactionFixture
    ):
        # Here's a provider that carries out the 'foo' operation.
        class Mock1(AlwaysSuccessfulCoverageProvider):
            OPERATION = "foo"

        identifier = db.identifier()
        provider = Mock1(db.session)

        # Here's a generic CoverageRecord for an identifier.
        record1 = CoverageRecord.add_for(identifier, provider.data_source)

        # That record doesn't count for purposes of
        # items_that_need_coverage, because the CoverageRecord doesn't
        # have an operation, and the CoverageProvider does.
        assert [identifier] == provider.items_that_need_coverage().all()

        # Here's a provider that has no operation set.
        successful_provider = AlwaysSuccessfulCoverageProvider(db.session)
        assert None == successful_provider.OPERATION

        # For purposes of items_that_need_coverage, the identifier is
        # considered covered, because the operations match.
        assert [] == successful_provider.items_that_need_coverage().all()

    def test_run_on_specific_identifiers(self, db: DatabaseTransactionFixture):
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        to_be_tested = [db.identifier() for i in range(6)]
        not_to_be_tested = [db.identifier() for i in range(6)]
        counts, records = provider.run_on_specific_identifiers(to_be_tested)

        # Six identifiers were covered in two batches.
        assert (6, 0, 0) == counts
        assert 6 == len(records)

        # Only the identifiers in to_be_tested were covered.
        assert all(isinstance(x, CoverageRecord) for x in records)
        assert set(to_be_tested) == {x.identifier for x in records}
        for i in to_be_tested:
            assert i in provider.attempts
        for i in not_to_be_tested:
            assert i not in provider.attempts

        assert provider.finalize_batch_called

    def test_run_on_specific_identifiers_respects_cutoff_time(
        self, db: DatabaseTransactionFixture
    ):
        last_run = datetime_utc(2016, 1, 1)

        # Once upon a time we successfully added coverage for
        # self.identifier. But now something has gone wrong, and if we
        # ever run the coverage provider again we will get a
        # persistent failure.
        identifier = db.identifier()
        provider = NeverSuccessfulCoverageProvider(db.session)
        record, ignore = CoverageRecord.add_for(identifier, provider.data_source)
        record.timestamp = last_run

        # You might think this would result in a persistent failure...
        (
            success,
            transient_failure,
            persistent_failure,
        ), records = provider.run_on_specific_identifiers([identifier])

        # ...but we get an automatic success. We didn't even try to
        # run the coverage provider on self.identifier because the
        # coverage record was up-to-date.
        assert 1 == success
        assert 0 == persistent_failure
        assert [] == records

        # But if we move the cutoff time forward, the provider will run
        # on self.identifier and fail.
        provider.cutoff_time = datetime_utc(2016, 2, 1)
        (
            success,
            transient_failure,
            persistent_failure,
        ), records = provider.run_on_specific_identifiers([identifier])
        assert 0 == success
        assert 1 == persistent_failure

        # The formerly successful CoverageRecord will be updated to
        # reflect the failure.
        assert records[0] == record
        assert "What did you expect?" == record.exception
        assert provider.finalize_batch_called

    def test_run_never_successful(self, db: DatabaseTransactionFixture):
        """Verify that NeverSuccessfulCoverageProvider works the
        way we'd expect.
        """

        identifier = db.identifier()
        provider = NeverSuccessfulCoverageProvider(db.session)

        # We start with no CoverageRecords and no Timestamp.
        assert [] == db.session.query(CoverageRecord).all()
        assert None == Timestamp.value(
            db.session,
            provider.service_name,
            service_type=Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )

        provider.run()

        # We have a CoverageRecord that signifies failure.
        [record] = db.session.query(CoverageRecord).all()
        assert identifier == record.identifier
        assert record.data_source == provider.data_source
        assert "What did you expect?" == record.exception

        # But the coverage provider did run, and the timestamp is now set to
        # a recent value.
        value = Timestamp.value(
            db.session,
            provider.service_name,
            service_type=Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )
        assert (utc_now() - value).total_seconds() < 1

    def test_run_transient_failure(self, db: DatabaseTransactionFixture):
        """Verify that TransientFailureCoverageProvider works the
        way we'd expect.
        """

        provider = TransientFailureCoverageProvider(db.session)
        identifier = db.identifier()

        # We start with no CoverageRecords and no Timestamp.
        assert [] == db.session.query(CoverageRecord).all()
        assert None == Timestamp.value(
            db.session,
            provider.service_name,
            service_type=Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )

        now = utc_now()
        provider.run()

        # We have a CoverageRecord representing the transient failure.
        [failure] = identifier.coverage_records
        assert CoverageRecord.TRANSIENT_FAILURE == failure.status

        # The timestamp was set.
        timestamp = Timestamp.value(
            db.session,
            provider.service_name,
            service_type=Timestamp.COVERAGE_PROVIDER_TYPE,
            collection=None,
        )
        assert (timestamp - now).total_seconds() < 1

    def test_add_coverage_record_for(self, db: DatabaseTransactionFixture):
        """Calling CollectionCoverageProvider.add_coverage_record is the same
        as calling CoverageRecord.add_for with the relevant
        information.
        """

        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())
        identifier = db.identifier()
        record = provider.add_coverage_record_for(identifier)

        # This is the same as calling CoverageRecord.add_for with
        # appropriate arguments.
        record2, is_new = CoverageRecord.add_for(
            identifier,
            data_source=provider.data_source,
            operation=provider.operation,
            collection=provider.collection_or_not,
        )
        assert False == is_new
        assert record == record2

        # By default, the CoverageRecord is not associated with any
        # particular collection.
        assert None == record.collection

        # Setting COVERAGE_COUNTS_FOR_EVERY_COLLECTION to False will
        # change that -- a CoverageRecord will only count for the
        # collection associated with the CoverageProvider.
        provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        record = provider.add_coverage_record_for(identifier)
        assert db.default_collection() == record.collection

        record2, is_new = CoverageRecord.add_for(
            identifier,
            data_source=provider.data_source,
            operation=provider.operation,
            collection=provider.collection_or_not,
        )
        assert False == is_new
        assert record == record2

    def test_record_failure_as_coverage_record(self):
        """TODO: We need test coverage here."""

    def test_failure(self, db: DatabaseTransactionFixture):
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())
        identifier = db.identifier()
        failure = provider.failure(identifier, error="an error", transient=False)
        assert provider.data_source == failure.data_source
        assert "an error" == failure.exception
        assert False == failure.transient

        # By default, the failure is not associated with any
        # particular collection.
        assert None == failure.collection

        # Setting COVERAGE_COUNTS_FOR_EVERY_COLLECTION to False
        # will change that -- a failure will only count for the
        # collection associated with the CoverageProvider.
        provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        failure = provider.failure(identifier, error="an error", transient=False)
        assert db.default_collection() == failure.collection

    def test_failure_for_ignored_item(self, db: DatabaseTransactionFixture):
        """Test that failure_for_ignored_item creates an appropriate
        CoverageFailure.
        """

        identifier = db.identifier()
        provider = NeverSuccessfulCoverageProvider(db.session)
        result = provider.failure_for_ignored_item(identifier)
        assert isinstance(result, CoverageFailure)
        assert True == result.transient
        assert "Was ignored by CoverageProvider." == result.exception
        assert identifier == result.obj
        assert provider.data_source == result.data_source


class TestCollectionCoverageProvider:
    def test_class_variables(self, db: DatabaseTransactionFixture):
        """Verify that class variables become appropriate instance
        variables.
        """
        collection = db.collection(protocol=OPDSAPI)
        provider = AlwaysSuccessfulCollectionCoverageProvider(collection)
        assert provider.DATA_SOURCE_NAME == provider.data_source.name

    def test_must_have_collection(self):
        with pytest.raises(CollectionMissing) as excinfo:
            AlwaysSuccessfulCollectionCoverageProvider(None)
        assert (
            "AlwaysSuccessfulCollectionCoverageProvider must be instantiated with a Collection."
            in str(excinfo.value)
        )

    def test_collection_protocol_must_match_class_protocol(
        self, db: DatabaseTransactionFixture
    ):
        collection = db.collection(protocol=OverdriveAPI)
        with pytest.raises(ValueError) as excinfo:
            AlwaysSuccessfulCollectionCoverageProvider(collection)
        assert (
            "Collection protocol (Overdrive) does not match CoverageProvider protocol (OPDS Import)"
            in str(excinfo.value)
        )

    def test_items_that_need_coverage_ignores_collection_when_collection_is_irrelevant(
        self, db
    ):
        # Two providers that do the same work, but one is associated
        # with a collection and the other is not.
        collection_provider = AlwaysSuccessfulCollectionCoverageProvider(
            db.default_collection()
        )
        no_collection_provider = AlwaysSuccessfulCoverageProvider(db.session)

        # This distinction is irrelevant because they both consider an
        # Identifier covered when it has a CoverageRecord not
        # associated with any particular collection.
        assert True == collection_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION
        assert True == no_collection_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION

        assert collection_provider.data_source == no_collection_provider.data_source
        data_source = collection_provider.data_source

        # Create a license pool belonging to the default collection.
        pool = db.licensepool(None, collection=db.default_collection())
        identifier = pool.identifier

        def needs():
            """Returns all items that need coverage from both test
            CoverageProviders.
            """
            return tuple(
                p.items_that_need_coverage().all()
                for p in (collection_provider, no_collection_provider)
            )

        # We start out in the state where the identifier appears to need
        # coverage from both CoverageProviders.
        assert ([identifier], [identifier]) == needs()

        # Add coverage for the default collection, and both
        # CoverageProviders still consider the identifier
        # uncovered. (This shouldn't happen, but if it does, we don't
        # count it.)
        db.coverage_record(identifier, data_source, collection=db.default_collection())
        assert ([identifier], [identifier]) == needs()

        # Add coverage not associated with any collection, and both
        # CoverageProviders consider it covered.
        db.coverage_record(identifier, data_source, collection=None)
        assert ([], []) == needs()

    def test_items_that_need_coverage_respects_collection_when_collection_is_relevant(
        self, db
    ):
        # Two providers that do the same work, but are associated
        # with different collections.
        collection_1_provider = AlwaysSuccessfulCollectionCoverageProvider(
            db.default_collection()
        )
        collection_2 = db.collection()
        collection_2_provider = AlwaysSuccessfulCollectionCoverageProvider(collection_2)

        # And one that does the same work but is not associated with
        # any collection.
        no_collection_provider = AlwaysSuccessfulCoverageProvider(db.session)

        # The 'collection' distinction is relevant, because these
        # CoverageProviders consider an identifier covered only when
        # it has a CoverageRecord for _their_ collection.
        collection_1_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        collection_2_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False
        no_collection_provider.COVERAGE_COUNTS_FOR_EVERY_COLLECTION = False

        assert collection_1_provider.data_source == collection_2_provider.data_source
        data_source = collection_1_provider.data_source

        # Create a license pool belonging to the default collection so
        # that its Identifier will show up as needing coverage by the
        # CoverageProvider that manages that collection.
        pool = db.licensepool(None, collection=db.default_collection())
        identifier = pool.identifier

        def needs():
            """Returns all items that need coverage from both test
            CoverageProviders.
            """
            return tuple(
                p.items_that_need_coverage().all()
                for p in (collection_1_provider, no_collection_provider)
            )

        # We start out in the state where the identifier needs
        # coverage from the CoverageProvider not associated with
        # any Collection, and the CoverageProvider associated with
        # the Collection where the LicensePool lives.
        #
        assert ([identifier], [identifier]) == needs()

        # The CoverageProvider associated with a different Collection
        # doesn't care about this Identifier, because its Collection
        # doesn't include that Identiifer.
        assert [] == collection_2_provider.items_that_need_coverage().all()

        # Add coverage for an irrelevant collection, and nothing happens.
        db.coverage_record(identifier, data_source, collection=db.collection())
        assert ([identifier], [identifier]) == needs()

        # Add coverage for a relevant collection, and it's treated as
        # covered by the provider that uses that collection.
        db.coverage_record(identifier, data_source, collection=db.default_collection())
        assert ([], [identifier]) == needs()

        # Add coverage not associated with a collection, and it's
        # treated as covered by the provider not associated with
        # any collection.
        db.coverage_record(identifier, data_source, collection=None)
        assert ([], []) == needs()

    def test_replacement_policy(self, db: DatabaseTransactionFixture):
        """Unless a different replacement policy is passed in, the
        replacement policy is ReplacementPolicy.from_license_source().
        """
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())
        assert True == provider.replacement_policy.identifiers
        assert True == provider.replacement_policy.formats

        policy = ReplacementPolicy.from_metadata_source()
        provider = AlwaysSuccessfulCollectionCoverageProvider(
            db.default_collection(), replacement_policy=policy
        )
        assert policy == provider.replacement_policy

    def test_all(self, db: DatabaseTransactionFixture):
        """Verify that all() gives a sequence of CollectionCoverageProvider
        objects, one for each Collection that implements the
        appropriate protocol.
        """
        opds1 = db.collection(protocol=OPDSAPI)
        opds2 = db.collection(protocol=OPDSAPI)
        overdrive = db.collection(protocol=OverdriveAPI)
        providers = list(
            AlwaysSuccessfulCollectionCoverageProvider.all(db.session, batch_size=34)
        )

        # The providers were returned in a random order, but there's one
        # for each collection that supports the 'OPDS Import' protocol.
        assert 2 == len(providers)
        collections = {x.collection for x in providers}
        assert {opds1, opds2} == collections

        # The providers are of the appropriate type and the keyword arguments
        # passed into all() were propagated to the constructor.
        for provider in providers:
            assert isinstance(provider, AlwaysSuccessfulCollectionCoverageProvider)
            assert 34 == provider.batch_size

    def test_set_circulationdata_errors(self, db: DatabaseTransactionFixture):
        """Verify that errors when setting circulation data
        are turned into CoverageFailure objects.
        """
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())
        identifier = db.identifier()

        # No db.
        failure = provider._set_circulationdata(identifier, None)
        assert "Did not receive circulationdata from input source" == failure.exception
        data_source_name = provider.data_source.name

        # No identifier in CirculationData.
        empty = CirculationData(
            data_source_name=data_source_name, primary_identifier_data=None
        )
        failure = provider._set_circulationdata(identifier, empty)
        assert (
            "Identifier did not match CirculationData's primary identifier."
            == failure.exception
        )

        # Mismatched identifier in CirculationData.
        wrong = CirculationData(
            data_source_name=data_source_name,
            primary_identifier_data=IdentifierData.from_identifier(db.identifier()),
        )
        failure = provider._set_circulationdata(identifier, empty)
        assert (
            "Identifier did not match CirculationData's primary identifier."
            == failure.exception
        )

        # Here, the data is okay, but the ReplacementPolicy is
        # going to cause an error the first time we try to use it.
        correct = CirculationData(
            data_source_name=data_source_name,
            primary_identifier_data=IdentifierData.from_identifier(identifier),
        )
        provider.replacement_policy = object()
        failure = provider._set_circulationdata(identifier, correct)
        assert isinstance(failure, CoverageFailure)

        # Verify that the general error handling works whether or not
        # the provider is associated with a Collection.
        provider.collection_id = None
        failure = provider._set_circulationdata(identifier, correct)
        assert isinstance(failure, CoverageFailure)

    def test_set_bibliographic_incorporates_replacement_policy(
        self, db: DatabaseTransactionFixture
    ):
        # Make sure that if a ReplacementPolicy is passed in to
        # set_bibliographic(), the policy's settings (and those of its
        # .presentation_calculation_policy) are respected.
        #
        # This is tested in this class rather than in
        # TestIdentifierCoverageProvider because with a collection in
        # place we can test a lot more aspects of the ReplacementPolicy.

        edition, pool = db.edition(with_license_pool=True)
        identifier = edition.primary_identifier

        class Tripwire:
            # This class sets a variable if one of its properties is
            # accessed.
            def __init__(self, *args, **kwargs):
                self.tripped = False

            def __getattr__(self, name):
                self.tripped = True
                if name.startswith("equivalent_identifier_"):
                    # These need to be numbers rather than booleans,
                    # but the exact number doesn't matter.
                    return 100
                return True

        presentation_calculation_policy = Tripwire()
        replacement_policy = ReplacementPolicy.model_construct(
            presentation_calculation_policy=cast(
                PresentationCalculationPolicy, presentation_calculation_policy
            ),
        )

        provider = AlwaysSuccessfulCollectionCoverageProvider(
            db.default_collection(), replacement_policy=replacement_policy
        )

        bibliographic = BibliographicData(
            data_source_name=provider.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(identifier),
        )
        # We've got a CirculationData object that includes an open-access download.
        link = LinkData(rel=Hyperlink.OPEN_ACCESS_DOWNLOAD, href="http://foo.com/")

        # We get an error if the CirculationData's identifier is
        # doesn't match what we pass in.
        circulationdata = CirculationData(
            data_source_name=provider.data_source.name,
            primary_identifier_data=IdentifierData.from_identifier(db.identifier()),
            links=[link],
        )
        failure = provider.set_bibliographic_and_circulation_data(
            identifier, bibliographic, circulationdata
        )
        assert (
            "Identifier did not match CirculationData's primary identifier."
            == failure.exception
        )

        # Otherwise, the data is applied.
        circulationdata = CirculationData(
            data_source_name=provider.data_source.name,
            primary_identifier_data=bibliographic.primary_identifier_data,
            links=[link],
        )

        provider.set_bibliographic_and_circulation_data(
            identifier, bibliographic, circulationdata
        )

        # Our custom PresentationCalculationPolicy was used when
        # determining whether to recalculate the work's
        # presentation. We know this because the tripwire was
        # triggered.
        assert True == presentation_calculation_policy.tripped

    def test_items_that_need_coverage(self, db: DatabaseTransactionFixture):
        # Here's an Identifier that was covered on 01/01/2016.
        identifier = db.identifier()
        cutoff_time = datetime_utc(2016, 1, 1)
        provider = AlwaysSuccessfulCoverageProvider(db.session)
        record, is_new = CoverageRecord.add_for(
            identifier, provider.data_source, timestamp=cutoff_time
        )

        # Since the Identifier was covered, it doesn't show up in
        # items_that_need_coverage.
        assert [] == provider.items_that_need_coverage().all()

        # If we set the CoverageProvider's cutoff_time to the time of
        # coverage, the Identifier is still treated as covered.
        provider = AlwaysSuccessfulCoverageProvider(db.session, cutoff_time=cutoff_time)
        assert [] == provider.items_that_need_coverage().all()

        # But if we set the cutoff time to immediately after the time
        # the Identifier was covered...
        one_second_after = cutoff_time + datetime.timedelta(seconds=1)
        provider = AlwaysSuccessfulCoverageProvider(
            db.session, cutoff_time=one_second_after
        )

        # The identifier is treated as lacking coverage.
        assert [identifier] == provider.items_that_need_coverage().all()

    def test_work(self, db: DatabaseTransactionFixture):
        """Verify that a CollectionCoverageProvider can create a Work."""
        # Here's an Gutenberg ID.
        identifier = db.identifier(identifier_type=Identifier.GUTENBERG_ID)

        # Here's a CollectionCoverageProvider that is associated
        # with an OPDS import-style Collection.
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())

        # This CoverageProvider cannot create a Work for the given
        # Identifier, because that would require creating a
        # LicensePool, and work() won't create a LicensePool if one
        # doesn't already exist.
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        assert "Cannot locate LicensePool" == result.exception

        # The CoverageProvider _can_ automatically create a
        # LicensePool, but since there is no Edition associated with
        # the Identifier, a Work still can't be created.
        pool = provider.license_pool(identifier)
        result = provider.work(identifier)
        assert isinstance(result, CoverageFailure)
        assert "Work could not be calculated" == result.exception

        # So let's use the CoverageProvider to create an Edition
        # with minimal bibliographic information.
        edition = provider.edition(identifier)
        edition.title = "A title"

        # Now we can create a Work.
        work = provider.work(identifier)
        assert isinstance(work, Work)
        assert "A title" == work.title

        # If necessary, we can tell work() to use a specific
        # LicensePool when calculating the Work. This is an extreme
        # example in which the LicensePool to use has a different
        # Identifier (identifier2) than the Identifier we're
        # processing (identifier1).
        #
        # In a real case, this would be used by a CoverageProvider
        # that just had to create a LicensePool using an
        # INTERNAL_PROCESSING DataSource rather than the DataSource
        # associated with the CoverageProvider.
        db.session.delete(pool)
        identifier2 = db.identifier()
        identifier.licensed_through = []
        collection2 = db.collection()
        edition2 = db.edition(
            identifier_type=identifier2.type, identifier_id=identifier2.identifier
        )
        pool2 = db.licensepool(edition=edition2, collection=collection2)
        work2 = provider.work(identifier, pool2)
        assert work2 != work
        assert [pool2] == work2.license_pools

        # Once an identifier has a work associated with it,
        # that's always the one that's used, and the value of license_pool
        # is ignored.
        work3 = provider.work(identifier2, object())
        assert work2 == work3

        # Any keyword arguments passed into work() are propagated to
        # calculate_work(). This lets use (e.g.) create a Work even
        # when there is no title.
        edition, pool = db.edition(with_license_pool=True)
        edition.title = None
        work = provider.work(pool.identifier, pool, even_if_no_title=True)
        assert isinstance(work, Work)
        assert None == work.title

        # If a work exists but is not presentation-ready,
        # CollectionCoverageProvider.work() will call calculate_work()
        # in an attempt to fix it.
        edition.title = "Finally a title"
        work2 = provider.work(pool.identifier, pool)
        assert work2 == work
        assert "Finally a title" == work.title
        assert True == work.presentation_ready

        # Once the work is presentation_ready, calling
        # CollectionCoverageProvider.work() will no longer call
        # calculate_work() -- it will just return the work.
        def explode():
            raise Exception("don't call me!")

        pool.calculate_work = explode
        work2 = provider.work(pool.identifier, pool)
        assert work2 == work

    def test_set_bibliographic_and_circulation_data(
        self,
        bibliographic_data,
        circulation_data,
        db,
    ):
        """Verify that a CollectionCoverageProvider can set both
        bibliographic data (on an Edition) and circulation data (on a LicensePool).
        """
        # Here's an Overdrive Identifier to work with.
        identifier = db.identifier(
            identifier_type=Identifier.OVERDRIVE_ID,
            foreign_id=bibliographic_data.primary_identifier_data.identifier,
        )

        # Here's a CollectionCoverageProvider that is associated with
        # an Overdrive-type Collection. (We have to subclass and talk
        # about Overdrive because BIBLIOGRAPHIC_DATA and
        # CIRCULATION_DATA are data for an Overdrive book.)
        class OverdriveProvider(AlwaysSuccessfulCollectionCoverageProvider):
            DATA_SOURCE_NAME = DataSource.OVERDRIVE
            PROTOCOL = OverdriveAPI.label()
            IDENTIFIER_TYPES = Identifier.OVERDRIVE_ID

        collection = db.collection(protocol=OverdriveAPI)
        provider = OverdriveProvider(collection)

        # We get a CoverageFailure if we don't pass in any data at all.
        result = provider.set_bibliographic_and_circulation_data(identifier, None, None)
        assert isinstance(result, CoverageFailure)
        assert (
            "Received neither bibliographic data nor circulation data from input source"
            == result.exception
        )

        # We get a CoverageFailure if no work can be created. In this
        # case, that happens because the bibliographic data doesn't provide a
        # title.
        old_title = bibliographic_data.title
        bibliographic_data.title = None
        result = provider.set_bibliographic_and_circulation_data(
            identifier, bibliographic_data, circulation_data
        )
        assert isinstance(result, CoverageFailure)
        assert "Work could not be calculated" == result.exception

        # Restore the title and try again. This time it will work.
        bibliographic_data.title = old_title
        result = provider.set_bibliographic_and_circulation_data(
            identifier, bibliographic_data, circulation_data
        )
        assert result == identifier

        # An Edition was created to hold the bibliographic data, a LicensePool
        # was created to hold the circulation data, and a Work
        # was created to bind everything together.
        [edition] = identifier.primarily_identifies
        assert "A Girl Named Disaster" == edition.title
        [pool] = identifier.licensed_through
        work = identifier.work
        assert work == pool.work

        # CoverageProviders that offer bibliographic information
        # typically don't have circulation information in the sense of
        # 'how many copies are in this Collection?', but sometimes
        # they do have circulation information in the sense of 'what
        # formats are available?'
        [lpdm] = pool.delivery_mechanisms
        mechanism = lpdm.delivery_mechanism
        assert "application/epub+zip (DRM-free)" == mechanism.name

        # If there's an exception setting the bibliographic data, a
        # CoverageFailure results. This call raises a ValueError
        # because the identifier we're trying to cover doesn't match
        # the identifier found in the BibliographicData object.
        old_identifier = bibliographic_data.primary_identifier_data
        bibliographic_data.primary_identifier_data = IdentifierData(
            type=Identifier.OVERDRIVE_ID, identifier="abcd"
        )
        result = provider.set_bibliographic_and_circulation_data(
            identifier, bibliographic_data, circulation_data
        )
        assert isinstance(result, CoverageFailure)
        assert "ValueError" in result.exception
        bibliographic_data.primary_identifier_data = old_identifier

    def test_autocreate_licensepool(self, db: DatabaseTransactionFixture):
        """A CollectionCoverageProvider can locate (or, if necessary, create)
        a LicensePool for an identifier.
        """
        identifier = db.identifier()
        assert [] == identifier.licensed_through
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())
        pool = provider.license_pool(identifier)
        assert [pool] == identifier.licensed_through
        assert pool.data_source == provider.data_source
        assert pool.identifier == identifier
        assert pool.collection == provider.collection

        # Calling license_pool again finds the same LicensePool
        # as before.
        pool2 = provider.license_pool(identifier)
        assert pool == pool2

        # It's possible for a CollectionCoverageProvider to create a
        # LicensePool for a different DataSource than the one
        # associated with the Collection. Only the metadata wrangler
        # needs to do this -- it's so a CoverageProvider for a
        # third-party DataSource can create an 'Internal Processing'
        # LicensePool when some other part of the metadata wrangler
        # failed to do this earlier.

        # If a working pool already exists, it's returned and no new
        # pool is created.
        same_pool = provider.license_pool(identifier, DataSource.INTERNAL_PROCESSING)
        assert same_pool == pool2
        assert provider.data_source == same_pool.data_source

        # A new pool is only created if no working pool can be found.
        identifier2 = db.identifier()
        new_pool = provider.license_pool(identifier2, DataSource.INTERNAL_PROCESSING)
        assert new_pool.data_source.name == DataSource.INTERNAL_PROCESSING
        assert new_pool.identifier == identifier2
        assert new_pool.collection == provider.collection

    def test_set_presentation_ready(self, db: DatabaseTransactionFixture):
        """Test that a CollectionCoverageProvider can set a Work
        as presentation-ready.
        """
        identifier = db.identifier()
        provider = AlwaysSuccessfulCollectionCoverageProvider(db.default_collection())

        # If there is no LicensePool for the Identifier,
        # set_presentation_ready will not try to create one,
        # and so no Work will be created.
        result = provider.set_presentation_ready(identifier)
        assert isinstance(result, CoverageFailure)
        assert "Cannot locate LicensePool" == result.exception

        # Once a LicensePool and a suitable Edition exist,
        # set_presentation_ready will create a Work for the item and
        # mark it presentation ready.
        pool = provider.license_pool(identifier)
        edition = provider.edition(identifier)
        edition.title = "A title"
        result = provider.set_presentation_ready(identifier)
        assert result == identifier
        assert True == pool.work.presentation_ready


class BibliographicCoverageProviderFixture:
    transaction: DatabaseTransactionFixture
    work: Work
    pool: LicensePool
    identifier: Identifier

    @classmethod
    def create(
        cls, transaction: DatabaseTransactionFixture
    ) -> "BibliographicCoverageProviderFixture":
        data = BibliographicCoverageProviderFixture()
        data.transaction = transaction
        data.work = transaction.work(
            with_license_pool=True, with_open_access_download=True
        )
        data.work.presentation_ready = False
        [data.pool] = data.work.license_pools
        data.identifier = data.pool.identifier
        return data


@pytest.fixture
def bibliographic_coverage_provider_fixture(
    db,
) -> BibliographicCoverageProviderFixture:
    return BibliographicCoverageProviderFixture.create(db)


class TestBibliographicCoverageProvider:
    """Test the features specific to BibliographicCoverageProvider."""

    def test_work_set_presentation_ready_on_success(
        self,
        bibliographic_coverage_provider_fixture: BibliographicCoverageProviderFixture,
    ):
        data = bibliographic_coverage_provider_fixture

        # When a Work is successfully run through a
        # BibliographicCoverageProvider, it's set as presentation-ready.
        provider = AlwaysSuccessfulBibliographicCoverageProvider(data.pool.collection)
        [result] = provider.process_batch([data.identifier])
        assert result == data.identifier
        assert True == data.work.presentation_ready

        # ensure_coverage does the same thing.
        data.work.presentation_ready = False
        result = provider.ensure_coverage(data.identifier)
        assert isinstance(result, CoverageRecord)
        assert result.identifier == data.identifier
        assert True == data.work.presentation_ready

    def test_failure_does_not_set_work_presentation_ready(
        self,
        bibliographic_coverage_provider_fixture: BibliographicCoverageProviderFixture,
    ):
        """A Work is not set as presentation-ready except on success."""
        data = bibliographic_coverage_provider_fixture

        provider = NeverSuccessfulBibliographicCoverageProvider(data.pool.collection)
        result = provider.ensure_coverage(data.identifier)
        assert CoverageRecord.TRANSIENT_FAILURE == result.status
        assert False == data.work.presentation_ready


class MockWork:
    """A Work-like object that keeps track of the policy that was used
    to recalculate its presentation.
    """

    def calculate_presentation(self, policy):
        self.calculate_presentation_called_with = policy
