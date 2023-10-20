import datetime
from unittest.mock import MagicMock

import pytest

from core.config import Configuration
from core.metadata_layer import TimestampData
from core.model import (
    CirculationEvent,
    Collection,
    CollectionMissing,
    ConfigurationSetting,
    Credential,
    DataSource,
    Edition,
    ExternalIntegration,
    Genre,
    Identifier,
    Measurement,
    Patron,
    Subject,
    Timestamp,
    Work,
    WorkCoverageRecord,
    create,
    get_one,
    get_one_or_create,
)
from core.monitor import (
    CirculationEventLocationScrubber,
    CollectionMonitor,
    CollectionReaper,
    CoverageProvidersFailed,
    CredentialReaper,
    CustomListEntrySweepMonitor,
    CustomListEntryWorkUpdateMonitor,
    EditionSweepMonitor,
    IdentifierSweepMonitor,
    MakePresentationReadyMonitor,
    MeasurementReaper,
    Monitor,
    NotPresentationReadyWorkSweepMonitor,
    PatronNeighborhoodScrubber,
    PatronRecordReaper,
    PermanentWorkIDRefreshMonitor,
    PresentationReadyWorkSweepMonitor,
    ReaperMonitor,
    SubjectSweepMonitor,
    SweepMonitor,
    TimelineMonitor,
    WorkReaper,
    WorkSweepMonitor,
)
from core.service import container
from core.util.datetime_helpers import datetime_utc, utc_now
from tests.core.mock import (
    AlwaysSuccessfulCoverageProvider,
    NeverSuccessfulCoverageProvider,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.time import Time


class MockMonitor(Monitor):
    SERVICE_NAME = "Dummy monitor for test"

    def __init__(self, _db, collection=None):
        super().__init__(_db, collection)
        self.run_records = []
        self.cleanup_records = []

    def run_once(self, progress):
        # Record the TimestampData object passed in.
        self.run_records.append(progress)

    def cleanup(self):
        self.cleanup_records.append(True)


class TestMonitor:
    def test_must_define_service_name(self, db: DatabaseTransactionFixture):
        class NoServiceName(MockMonitor):
            SERVICE_NAME = None  # type: ignore[assignment]

        with pytest.raises(ValueError) as excinfo:
            NoServiceName(db.session)
        assert "NoServiceName must define SERVICE_NAME." in str(excinfo.value)

    def test_collection(self, db: DatabaseTransactionFixture):
        monitor = MockMonitor(db.session, db.default_collection())
        assert db.default_collection() == monitor.collection
        monitor.collection_id = None
        assert None == monitor.collection

    def test_initial_start_time(self, db: DatabaseTransactionFixture):
        monitor = MockMonitor(db.session, db.default_collection())

        # Setting the default start time to NEVER explicitly says to use
        # None as the initial time.
        monitor.default_start_time = monitor.NEVER
        assert None == monitor.initial_start_time

        # Setting the value to None means "use the current time".
        monitor.default_start_time = None
        Time.time_eq(utc_now(), monitor.initial_start_time)

        # Any other value is returned as-is.
        default = object()
        monitor.default_start_time = default
        assert default == monitor.initial_start_time

    def test_monitor_lifecycle(self, db: DatabaseTransactionFixture):
        monitor = MockMonitor(db.session, db.default_collection())
        monitor.default_start_time = datetime_utc(2010, 1, 1)

        # There is no timestamp for this monitor.
        def get_timestamp():
            return get_one(db.session, Timestamp, service=monitor.service_name)

        assert None == get_timestamp()

        # Run the monitor.
        monitor.run()

        # The monitor ran once and then stopped.
        [progress] = monitor.run_records

        # The TimestampData passed in to run_once() had the
        # Monitor's default start time as its .start, and an empty
        # time for .finish.
        assert monitor.default_start_time == progress.start
        assert None == progress.finish

        # But the Monitor's underlying timestamp has been updated with
        # the time that the monitor actually took to run.
        timestamp = get_timestamp()
        assert timestamp.start > monitor.default_start_time
        assert timestamp.finish > timestamp.start
        Time.time_eq(utc_now(), timestamp.start)

        # cleanup() was called once.
        assert [True] == monitor.cleanup_records

    def test_initial_timestamp(self, db: DatabaseTransactionFixture):
        class NeverRunMonitor(MockMonitor):
            SERVICE_NAME = "Never run"
            DEFAULT_START_TIME = MockMonitor.NEVER

        # The Timestamp object is created, but its .start is None,
        # indicating that it has never run to completion.
        m = NeverRunMonitor(db.session, db.default_collection())
        assert None == m.timestamp().start

        class RunLongAgoMonitor(MockMonitor):
            SERVICE_NAME = "Run long ago"
            DEFAULT_START_TIME = MockMonitor.ONE_YEAR_AGO

        # The Timestamp object is created, and its .timestamp is long ago.
        m2 = RunLongAgoMonitor(db.session, db.default_collection())
        timestamp = m2.timestamp()
        now = utc_now()
        assert timestamp.start < now

        # Timestamp.finish is set to None, on the assumption that the
        # first run is still in progress.
        assert timestamp.finish == None

    def test_run_once_returning_timestampdata(self, db: DatabaseTransactionFixture):
        # If a Monitor's run_once implementation returns a TimestampData,
        # that's the data used to set the Monitor's Timestamp, even if
        # the data doesn't make sense by the standards used by the main
        # Monitor class.
        start = datetime_utc(2011, 1, 1)
        finish = datetime_utc(2012, 1, 1)

        class Mock(MockMonitor):
            def run_once(self, progress):
                return TimestampData(start=start, finish=finish, counter=-100)

        monitor = Mock(db.session, db.default_collection())
        monitor.run()

        timestamp = monitor.timestamp()
        assert start == timestamp.start
        assert finish == timestamp.finish
        assert -100 == timestamp.counter

    def test_run_once_with_exception(self, db: DatabaseTransactionFixture):
        # If an exception happens during a Monitor's run_once
        # implementation, a traceback for that exception is recorded
        # in the appropriate Timestamp, but the timestamp itself is
        # not updated.

        # This test function shows the behavior we expect from a
        # Monitor.
        def assert_run_sets_exception(monitor, check_for):
            timestamp = monitor.timestamp()
            old_start = timestamp.start
            old_finish = timestamp.finish
            assert None == timestamp.exception

            monitor.run()

            # The timestamp has been updated, but the times have not.
            assert check_for in timestamp.exception
            assert old_start == timestamp.start
            assert old_finish == timestamp.finish

        # Try a monitor that raises an unhandled exception.
        class DoomedMonitor(MockMonitor):
            SERVICE_NAME = "Doomed"

            def run_once(self, *args, **kwargs):
                raise Exception("I'm doomed")

        m = DoomedMonitor(db.session, db.default_collection())
        assert_run_sets_exception(m, "Exception: I'm doomed")

        # Try a monitor that sets .exception on the TimestampData it
        # returns.
        class AlsoDoomed(MockMonitor):
            SERVICE_NAME = "Doomed, but in a different way."

            def run_once(self, progress):
                return TimestampData(exception="I'm also doomed")

        m2 = AlsoDoomed(db.session, db.default_collection())
        assert_run_sets_exception(m2, "I'm also doomed")

    def test_same_monitor_different_collections(self, db: DatabaseTransactionFixture):
        """A single Monitor has different Timestamps when run against
        different Collections.
        """
        c1 = db.collection()
        c2 = db.collection()
        m1 = MockMonitor(db.session, c1)
        m2 = MockMonitor(db.session, c2)

        # The two Monitors have the same service name but are operating
        # on different Collections.
        assert m1.service_name == m2.service_name
        assert c1 == m1.collection
        assert c2 == m2.collection

        assert [] == c1.timestamps
        assert [] == c2.timestamps

        # Run the first Monitor.
        m1.run()
        [t1] = c1.timestamps
        assert m1.service_name == t1.service
        assert m1.collection == t1.collection
        old_m1_timestamp = m1.timestamp

        # Running the first Monitor did not create a timestamp for the
        # second Monitor.
        assert [] == c2.timestamps

        # Run the second monitor.
        m2.run()

        # The timestamp for the first monitor was not updated when
        # we ran the second monitor.
        assert old_m1_timestamp == m1.timestamp

        # But the second Monitor now has its own timestamp.
        [t2] = c2.timestamps
        assert isinstance(t1.start, datetime.datetime)
        assert isinstance(t2.start, datetime.datetime)
        assert t2.start > t1.start

    def test_init_configures_logging(self, db: DatabaseTransactionFixture):
        mock_services = MagicMock()
        container._container_instance = mock_services
        collection = db.collection()
        MockMonitor(db.session, collection)
        mock_services.init_resources.assert_called_once()
        container._container_instance = None


class TestCollectionMonitor:
    """Test the special features of CollectionMonitor."""

    def test_protocol_enforcement(self, db: DatabaseTransactionFixture):
        """A CollectionMonitor can require that it be instantiated
        with a Collection that implements a certain protocol.
        """

        class NoProtocolMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor 1"
            PROTOCOL = None

        class OverdriveMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor 2"
            PROTOCOL = ExternalIntegration.OVERDRIVE

        # Two collections.
        c1 = db.collection(protocol=ExternalIntegration.OVERDRIVE)
        c2 = db.collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # The NoProtocolMonitor can be instantiated with either one,
        # or with no Collection at all.
        NoProtocolMonitor(db.session, c1)
        NoProtocolMonitor(db.session, c2)
        NoProtocolMonitor(db.session, None)

        # The OverdriveMonitor can only be instantiated with the first one.
        OverdriveMonitor(db.session, c1)
        with pytest.raises(ValueError) as excinfo:
            OverdriveMonitor(db.session, c2)
        assert (
            "Collection protocol (Bibliotheca) does not match Monitor protocol (Overdrive)"
            in str(excinfo.value)
        )
        with pytest.raises(CollectionMissing):
            OverdriveMonitor(db.session, None)

    def test_all(self, db: DatabaseTransactionFixture):
        """Test that we can create a list of Monitors using all()."""

        class OPDSCollectionMonitor(CollectionMonitor):
            SERVICE_NAME = "Test Monitor"
            PROTOCOL = ExternalIntegration.OPDS_IMPORT

        # Here we have three OPDS import Collections...
        o1 = db.collection("o1")
        o2 = db.collection("o2")
        o3 = db.collection("o3")

        # ...and a Bibliotheca collection.
        b1 = db.collection(protocol=ExternalIntegration.BIBLIOTHECA)

        # o1 just had its Monitor run.
        Timestamp.stamp(
            db.session, OPDSCollectionMonitor.SERVICE_NAME, Timestamp.MONITOR_TYPE, o1
        )

        # o2 and b1 have never had their Monitor run, but o2 has had some other Monitor run.
        Timestamp.stamp(db.session, "A Different Service", Timestamp.MONITOR_TYPE, o2)

        # o3 had its Monitor run an hour ago.
        now = utc_now()
        an_hour_ago = now - datetime.timedelta(seconds=3600)
        Timestamp.stamp(
            db.session,
            OPDSCollectionMonitor.SERVICE_NAME,
            Timestamp.MONITOR_TYPE,
            o3,
            start=an_hour_ago,
            finish=an_hour_ago,
        )

        monitors = list(OPDSCollectionMonitor.all(db.session))

        # Three OPDSCollectionMonitors were returned, one for each
        # appropriate collection. The monitor that needs to be run the
        # worst was returned first in the list. The monitor that was
        # run most recently is returned last. There is no
        # OPDSCollectionMonitor for the Bibliotheca collection.
        assert [o2, o3, o1] == [x.collection for x in monitors]

        # If `collections` are specified, monitors should be yielded in the same order.
        opds_collections = [o3, o1, o2]
        monitors = list(
            OPDSCollectionMonitor.all(db.session, collections=opds_collections)
        )
        monitor_collections = [m.collection for m in monitors]
        # We should get a monitor for each collection.
        assert set(opds_collections) == set(monitor_collections)
        # We should get them back in order.
        assert opds_collections == monitor_collections

        # If `collections` are specified, monitors should be yielded in the same order.
        opds_collections = [o3, o1]
        monitors = list(
            OPDSCollectionMonitor.all(db.session, collections=opds_collections)
        )
        monitor_collections = [m.collection for m in monitors]
        # We should get a monitor for each collection.
        assert set(opds_collections) == set(monitor_collections)
        # We should get them back in order.
        assert opds_collections == monitor_collections

        # If collections are specified, they must match the monitor's protocol.
        with pytest.raises(ValueError) as excinfo:
            monitors = list(OPDSCollectionMonitor.all(db.session, collections=[b1]))
        assert (
            "Collection protocol (Bibliotheca) does not match Monitor protocol (OPDS Import)"
            in str(excinfo.value)
        )
        assert "Only the following collections are available: " in str(excinfo.value)


class TestTimelineMonitor:
    def test_run_once(self, db: DatabaseTransactionFixture):
        class Mock(TimelineMonitor):
            SERVICE_NAME = "Just a timeline"
            catchups = []

            def catch_up_from(self, start, cutoff, progress):
                self.catchups.append((start, cutoff, progress))

        m = Mock(db.session)
        progress = m.timestamp().to_data()
        m.run_once(progress)
        now = utc_now()

        # catch_up_from() was called once.
        (start, cutoff, progress) = m.catchups.pop()
        assert m.initial_start_time == start
        Time.time_eq(cutoff, now)

        # progress contains a record of the timespan now covered
        # by this Monitor.
        assert start == progress.start
        assert cutoff == progress.finish

    def test_subclass_cannot_modify_dates(self, db: DatabaseTransactionFixture):
        """The subclass can modify some fields of the TimestampData
        passed in to it, but it can't modify the start or end dates.

        If you want that, you shouldn't subclass TimelineMonitor.
        """

        class Mock(TimelineMonitor):
            DEFAULT_START_TIME = Monitor.NEVER
            SERVICE_NAME = "I aim to misbehave"

            def catch_up_from(self, start, cutoff, progress):
                progress.start = 1
                progress.finish = 2
                progress.counter = 3
                progress.achievements = 4

        m = Mock(db.session)
        progress = m.timestamp().to_data()
        m.run_once(progress)
        now = utc_now()

        # The timestamp values have been set to appropriate values for
        # the portion of the timeline covered, overriding our values.
        assert None == progress.start
        Time.time_eq(now, progress.finish)

        # The non-timestamp values have been left alone.
        assert 3 == progress.counter
        assert 4 == progress.achievements

    def test_timestamp_not_updated_on_exception(self, db: DatabaseTransactionFixture):
        """If the subclass sets .exception on the TimestampData
        passed into it, the dates aren't modified.
        """

        class Mock(TimelineMonitor):
            DEFAULT_START_TIME = datetime_utc(2011, 1, 1)
            SERVICE_NAME = "doomed"

            def catch_up_from(self, start, cutoff, progress):
                self.started_at = start
                progress.exception = "oops"

        m = Mock(db.session)
        progress = m.timestamp().to_data()
        m.run_once(progress)

        # The timestamp value is set to a value indicating that the
        # initial run never completed.
        assert m.DEFAULT_START_TIME == progress.start
        assert None == progress.finish

    def test_slice_timespan(self, db: DatabaseTransactionFixture):
        # Test the slice_timespan utility method.

        # Slicing up the time between 121 minutes ago and now in increments
        # of one hour will yield three slices:
        #
        # 121 minutes ago -> 61 minutes ago
        # 61 minutes ago -> 1 minute ago
        # 1 minute ago -> now
        now = utc_now()
        one_hour = datetime.timedelta(minutes=60)
        ago_1 = now - datetime.timedelta(minutes=1)
        ago_61 = ago_1 - one_hour
        ago_121 = ago_61 - one_hour

        slice1, slice2, slice3 = list(
            TimelineMonitor.slice_timespan(ago_121, now, one_hour)
        )
        assert slice1 == (ago_121, ago_61, True)
        assert slice2 == (ago_61, ago_1, True)
        assert slice3 == (ago_1, now, False)

        # The True/True/False indicates that the first two slices are
        # complete -- they cover a span of an entire hour. The final
        # slice is incomplete -- it covers only one minute.


class MockSweepMonitor(SweepMonitor):
    """A SweepMonitor that does nothing."""

    MODEL_CLASS = Identifier
    SERVICE_NAME = "Sweep Monitor"
    DEFAULT_BATCH_SIZE = 2

    def __init__(self, _db, **kwargs):
        super().__init__(_db, **kwargs)
        self.cleanup_called = []
        self.batches = []
        self.processed = []

    def scope_to_collection(self, qu, collection):
        return qu

    def process_batch(self, batch):
        self.batches.append(batch)
        return super().process_batch(batch)

    def process_item(self, item):
        self.processed.append(item)

    def cleanup(self):
        self.cleanup_called.append(True)


class SweepMonitorFixture:
    monitor: MockSweepMonitor


@pytest.fixture()
def sweep_monitor_fixture(
    db: DatabaseTransactionFixture,
) -> SweepMonitorFixture:
    data = SweepMonitorFixture()
    data.monitor = MockSweepMonitor(db.session)
    return data


class TestSweepMonitor:
    def test_model_class_is_required(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        class NoModelClass(SweepMonitor):
            MODEL_CLASS = None

        with pytest.raises(ValueError) as excinfo:
            NoModelClass(db.session)
        assert "NoModelClass must define MODEL_CLASS" in str(excinfo.value)

    def test_batch_size(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        assert (
            MockSweepMonitor.DEFAULT_BATCH_SIZE
            == sweep_monitor_fixture.monitor.batch_size
        )

        monitor = MockSweepMonitor(db.session, batch_size=29)
        assert 29 == monitor.batch_size

        # If you pass in an invalid value you get the default.
        monitor = MockSweepMonitor(db.session, batch_size=-1)
        assert MockSweepMonitor.DEFAULT_BATCH_SIZE == monitor.batch_size

    def test_run_against_empty_table(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        # If there's nothing in the table to be swept, a SweepMonitor runs
        # to completion and accomplishes nothing.
        sweep_monitor_fixture.monitor.run()
        timestamp = sweep_monitor_fixture.monitor.timestamp()
        assert "Records processed: 0." == timestamp.achievements
        assert None == timestamp.exception

    def test_run_sweeps_entire_table(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        # Three Identifiers -- the batch size is 2.
        i1, i2, i3 = (db.identifier() for i in range(3))
        assert 2 == sweep_monitor_fixture.monitor.batch_size

        # Run the monitor.
        sweep_monitor_fixture.monitor.run()

        # All three Identifiers, and no other items, were processed.
        assert [i1, i2, i3] == sweep_monitor_fixture.monitor.processed

        # We ran process_batch() three times: once starting at zero,
        # once starting at the ID that ended the first batch, and
        # again starting at the ID that ended the second batch.
        assert [0, i2.id, i3.id] == sweep_monitor_fixture.monitor.batches

        # The cleanup method was called once.
        assert [True] == sweep_monitor_fixture.monitor.cleanup_called

        # The number of records processed reflects what happened over
        # the entire run, not just the final batch.
        assert (
            "Records processed: 3."
            == sweep_monitor_fixture.monitor.timestamp().achievements
        )

    def test_run_starts_at_previous_counter(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        # Two Identifiers.
        i1, i2 = (db.identifier() for i in range(2))

        # The monitor was just run, but it was not able to proceed past
        # i1.
        timestamp = Timestamp.stamp(
            db.session,
            sweep_monitor_fixture.monitor.service_name,
            Timestamp.MONITOR_TYPE,
            sweep_monitor_fixture.monitor.collection,
        )
        timestamp.counter = i1.id

        # Run the monitor.
        sweep_monitor_fixture.monitor.run()

        # The last item in the table was processed. i1 was not
        # processed, because it was processed in a previous run.
        assert [i2] == sweep_monitor_fixture.monitor.processed

        # The monitor's counter has been reset.
        assert 0 == timestamp.counter

    def test_exception_interrupts_run(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        # Four Identifiers.
        i1, i2, i3, i4 = (db.identifier() for i in range(4))

        # This monitor will never be able to process the fourth one.
        class IHateI4(MockSweepMonitor):
            def process_item(self, item):
                if item is i4:
                    raise Exception("HOW DARE YOU")
                super().process_item(item)

        monitor = IHateI4(db.session)

        timestamp = monitor.timestamp()
        original_start = timestamp.start
        monitor.run()

        # The monitor's counter was updated to the ID of the final
        # item in the last batch it was able to process. In this case,
        # this is I2.
        assert i2.id == timestamp.counter

        # The exception that stopped the run was recorded.
        assert "Exception: HOW DARE YOU" in timestamp.exception

        # Even though the run didn't complete, the dates and
        # achievements of the timestamp were updated to reflect the
        # work that _was_ done.
        now = utc_now()
        assert timestamp.start > original_start
        Time.time_eq(now, timestamp.start)
        Time.time_eq(now, timestamp.finish)
        assert timestamp.start < timestamp.finish

        assert "Records processed: 2." == timestamp.achievements

        # I3 was processed, but the batch did not complete, so any
        # changes wouldn't have been written to the database.
        assert [i1, i2, i3] == monitor.processed

        # Running the monitor again will process I3 again, but the same error
        # will happen on i4 and the counter will not be updated.
        monitor.run()
        assert [i1, i2, i3, i3] == monitor.processed
        assert i2.id == timestamp.counter

        # cleanup() is only called when the sweep completes successfully.
        assert [] == monitor.cleanup_called


class TestIdentifierSweepMonitor:
    def test_scope_to_collection(self, db: DatabaseTransactionFixture):
        # Two Collections, each with a LicensePool.
        c1 = db.collection()
        c2 = db.collection()
        e1, p1 = db.edition(with_license_pool=True, collection=c1)
        e2, p2 = db.edition(with_license_pool=True, collection=c2)

        # A Random Identifier not associated with any Collection.
        i3 = db.identifier()

        class Mock(IdentifierSweepMonitor):
            SERVICE_NAME = "Mock"

        # With a Collection, we only process items that are licensed through
        # that collection.
        monitor = Mock(db.session, c1)
        assert [p1.identifier] == monitor.item_query().all()

        # With no Collection, we process all items.
        monitor = Mock(db.session, None)
        assert [p1.identifier, p2.identifier, i3] == monitor.item_query().all()


class TestSubjectSweepMonitor:
    def test_item_query(self, db: DatabaseTransactionFixture):
        class Mock(SubjectSweepMonitor):
            SERVICE_NAME = "Mock"

        s1, ignore = Subject.lookup(db.session, Subject.DDC, "100", None)
        s2, ignore = Subject.lookup(
            db.session, Subject.TAG, None, "100 Years of Solitude"
        )

        # By default, SubjectSweepMonitor handles every Subject
        # in the database.
        everything = Mock(db.session)
        assert [s1, s2] == everything.item_query().all()

        # But you can tell SubjectSweepMonitor to handle only Subjects
        # of a certain type.
        dewey_monitor = Mock(db.session, subject_type=Subject.DDC)
        assert [s1] == dewey_monitor.item_query().all()

        # You can also SubjectSweepMonitor to handle only Subjects
        # whose names or identifiers match a certain string.
        one_hundred_monitor = Mock(db.session, filter_string="100")
        assert [s1, s2] == one_hundred_monitor.item_query().all()

        specific_tag_monitor = Mock(
            db.session, subject_type=Subject.TAG, filter_string="Years"
        )
        assert [s2] == specific_tag_monitor.item_query().all()


class TestCustomListEntrySweepMonitor:
    def test_item_query(self, db: DatabaseTransactionFixture):
        class Mock(CustomListEntrySweepMonitor):
            SERVICE_NAME = "Mock"

        # Three CustomLists, each containing one book.
        list1, [edition1] = db.customlist(num_entries=1)
        list2, [edition2] = db.customlist(num_entries=1)
        list3, [edition3] = db.customlist(num_entries=1)

        [entry1] = list1.entries
        [entry2] = list2.entries
        [entry3] = list3.entries

        # Two Collections, each with one book from one of the lists.
        c1 = db.collection()
        c1.licensepools.extend(edition1.license_pools)

        c2 = db.collection()
        c2.licensepools.extend(edition2.license_pools)

        # If we don't pass in a Collection to
        # CustomListEntrySweepMonitor, we get all three
        # CustomListEntries, in their order of creation.
        monitor = Mock(db.session)
        assert [entry1, entry2, entry3] == monitor.item_query().all()

        # If we pass in a Collection to CustomListEntrySweepMonitor,
        # we get only the CustomListEntry whose work is licensed
        # to that collection.
        monitor = Mock(db.session, collection=c2)
        assert [entry2] == monitor.item_query().all()


class TestEditionSweepMonitor:
    def test_item_query(self, db: DatabaseTransactionFixture):
        class Mock(EditionSweepMonitor):
            SERVICE_NAME = "Mock"

        # Three Editions, two of which have LicensePools.
        e1, p1 = db.edition(with_license_pool=True)
        e2, p2 = db.edition(with_license_pool=True)
        e3 = db.edition(with_license_pool=False)

        # Two Collections, each with one book.
        c1 = db.collection()
        c1.licensepools.extend(e1.license_pools)

        c2 = db.collection()
        c2.licensepools.extend(e2.license_pools)

        # If we don't pass in a Collection to EditionSweepMonitor, we
        # get all three Editions, in their order of creation.
        monitor = Mock(db.session)
        assert [e1, e2, e3] == monitor.item_query().all()

        # If we pass in a Collection to EditionSweepMonitor, we get
        # only the Edition whose work is licensed to that collection.
        monitor = Mock(db.session, collection=c2)
        assert [e2] == monitor.item_query().all()


class TestWorkSweepMonitors:
    """To reduce setup costs, this class tests WorkSweepMonitor,
    PresentationReadyWorkSweepMonitor, and
    NotPresentationReadyWorkSweepMonitor at once.
    """

    def test_item_query(self, db: DatabaseTransactionFixture):
        class Mock(WorkSweepMonitor):
            SERVICE_NAME = "Mock"

        # Three Works with LicensePools. Only one is presentation
        # ready.
        w1, w2, w3 = (db.work(with_license_pool=True) for i in range(3))

        # Another Work that's presentation ready but has no
        # LicensePool.
        w4 = db.work()
        w4.presentation_ready = True

        w2.presentation_ready = False
        w3.presentation_ready = None

        # Two Collections, each with one book.
        c1 = db.collection()
        c1.licensepools.append(w1.license_pools[0])

        c2 = db.collection()
        c2.licensepools.append(w2.license_pools[0])

        # If we don't pass in a Collection to WorkSweepMonitor, we
        # get all four Works, in their order of creation.
        monitor = Mock(db.session)
        assert [w1, w2, w3, w4] == monitor.item_query().all()

        # If we pass in a Collection to EditionSweepMonitor, we get
        # only the Work licensed to that collection.
        monitor = Mock(db.session, collection=c2)
        assert [w2] == monitor.item_query().all()

        # PresentationReadyWorkSweepMonitor is the same, but it excludes
        # works that are not presentation ready.
        class Mock2(PresentationReadyWorkSweepMonitor):
            SERVICE_NAME = "Mock"

        assert [w1, w4] == Mock2(db.session).item_query().all()
        assert [w1] == Mock2(db.session, collection=c1).item_query().all()
        assert [] == Mock2(db.session, collection=c2).item_query().all()

        # NotPresentationReadyWorkSweepMonitor is the same, but it _only_
        # includes works that are not presentation ready.
        class Mock3(NotPresentationReadyWorkSweepMonitor):
            SERVICE_NAME = "Mock"

        assert [w2, w3] == Mock3(db.session).item_query().all()
        assert [] == Mock3(db.session, collection=c1).item_query().all()
        assert [w2] == Mock3(db.session, collection=c2).item_query().all()


class TestPermanentWorkIDRefresh:
    def test_process_item(self, db: DatabaseTransactionFixture):
        """This Monitor calculates an Editions' permanent work ID."""

        class Mock(PermanentWorkIDRefreshMonitor):
            SERVICE_NAME = "Mock"

        edition = db.edition()
        assert None == edition.permanent_work_id
        Mock(db.session).process_item(edition)
        assert edition.permanent_work_id != None


class PresentationReadyMonitorFixture:
    db: DatabaseTransactionFixture
    success: AlwaysSuccessfulCoverageProvider
    failure: NeverSuccessfulCoverageProvider
    work: Work


@pytest.fixture()
def presentation_ready_monitor_fixture(
    db: DatabaseTransactionFixture,
) -> PresentationReadyMonitorFixture:
    data = PresentationReadyMonitorFixture()
    data.db = db
    session = db.session

    # This CoverageProvider will always succeed.
    class MockProvider1(AlwaysSuccessfulCoverageProvider):
        SERVICE_NAME = "Provider 1"
        INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
        DATA_SOURCE_NAME = DataSource.OCLC

    # This CoverageProvider will always fail.
    class MockProvider2(NeverSuccessfulCoverageProvider):
        SERVICE_NAME = "Provider 2"
        INPUT_IDENTIFIER_TYPES = Identifier.GUTENBERG_ID
        DATA_SOURCE_NAME = DataSource.OVERDRIVE

    data.success = MockProvider1(session)
    data.failure = MockProvider2(session)

    data.work = db.work(DataSource.GUTENBERG, with_license_pool=True)
    # Don't fake that the work is presentation ready, as we usually do,
    # because presentation readiness is what we're trying to test.
    data.work.presentation_ready = False
    return data


class TestMakePresentationReadyMonitor:
    def test_process_item_sets_presentation_ready_on_success(
        self, presentation_ready_monitor_fixture: PresentationReadyMonitorFixture
    ):
        data, session = (
            presentation_ready_monitor_fixture,
            presentation_ready_monitor_fixture.db.session,
        )

        # Create a monitor that doesn't need to do anything.
        monitor = MakePresentationReadyMonitor(session, [])
        monitor.process_item(data.work)

        # When it's done doing nothing, it sets the work as
        # presentation-ready.
        assert None == data.work.presentation_ready_exception
        assert True == data.work.presentation_ready

    def test_process_item_sets_exception_on_failure(
        self, presentation_ready_monitor_fixture: PresentationReadyMonitorFixture
    ):
        data, session = (
            presentation_ready_monitor_fixture,
            presentation_ready_monitor_fixture.db.session,
        )

        monitor = MakePresentationReadyMonitor(session, [data.success, data.failure])
        monitor.process_item(data.work)
        assert (
            "Provider(s) failed: %s" % data.failure.SERVICE_NAME
            == data.work.presentation_ready_exception
        )
        assert False == data.work.presentation_ready

    def test_prepare_raises_exception_with_failing_providers(
        self, presentation_ready_monitor_fixture: PresentationReadyMonitorFixture
    ):
        data, session = (
            presentation_ready_monitor_fixture,
            presentation_ready_monitor_fixture.db.session,
        )

        monitor = MakePresentationReadyMonitor(session, [data.success, data.failure])
        with pytest.raises(CoverageProvidersFailed) as excinfo:
            monitor.prepare(data.work)
        assert data.failure.service_name in str(excinfo.value)

    def test_prepare_does_not_call_irrelevant_provider(
        self, presentation_ready_monitor_fixture: PresentationReadyMonitorFixture
    ):
        data, session = (
            presentation_ready_monitor_fixture,
            presentation_ready_monitor_fixture.db.session,
        )

        monitor = MakePresentationReadyMonitor(session, [data.success])
        result = monitor.prepare(data.work)

        # There were no failures.
        assert [] == result

        # The 'success' monitor ran.
        assert [
            data.work.presentation_edition.primary_identifier
        ] == data.success.attempts

        # The 'failure' monitor did not. (If it had, it would have
        # failed.)
        assert [] == data.failure.attempts

        # The work has not been set to presentation ready--that's
        # handled in process_item().
        assert False == data.work.presentation_ready


class TestCustomListEntryWorkUpdateMonitor:
    def test_set_item(self, db: DatabaseTransactionFixture):
        # Create a CustomListEntry.
        list1, [edition1] = db.customlist(num_entries=1)
        [entry] = list1.entries

        # Pretend that its CustomListEntry's work was never set.
        old_work = entry.work
        entry.work = None

        # Running process_item resets it to the same value.
        monitor = CustomListEntryWorkUpdateMonitor(db.session)
        monitor.process_item(entry)
        assert old_work == entry.work


class MockReaperMonitor(ReaperMonitor):
    MODEL_CLASS = Timestamp
    TIMESTAMP_FIELD = "timestamp"


class TestReaperMonitor:
    def test_cutoff(self, db: DatabaseTransactionFixture):
        """Test that cutoff behaves correctly when given different values for
        ReaperMonitor.MAX_AGE.
        """
        m = MockReaperMonitor(db.session)

        # A number here means a number of days.
        for value in [1, 1.5, -1]:
            m.MAX_AGE = value
            expect = utc_now() - datetime.timedelta(days=value)
            Time.time_eq(m.cutoff, expect)

        # But you can pass in a timedelta instead.
        m.MAX_AGE = datetime.timedelta(seconds=99)
        Time.time_eq(m.cutoff, utc_now() - m.MAX_AGE)

    def test_specific_reapers(self, db: DatabaseTransactionFixture):
        assert Credential.expires == CredentialReaper(db.session).timestamp_field
        assert 1 == CredentialReaper.MAX_AGE
        assert (
            Patron.authorization_expires
            == PatronRecordReaper(db.session).timestamp_field
        )
        assert 60 == PatronRecordReaper.MAX_AGE

    def test_run_once(self, db: DatabaseTransactionFixture):
        # Create four Credentials: two expired, two valid.
        expired1 = db.credential()
        expired2 = db.credential()
        now = utc_now()
        expiration_date = now - datetime.timedelta(days=CredentialReaper.MAX_AGE + 1)
        for e in [expired1, expired2]:
            e.expires = expiration_date

        active = db.credential()
        active.expires = now - datetime.timedelta(days=CredentialReaper.MAX_AGE - 1)

        eternal = db.credential()

        m = CredentialReaper(db.session)

        # Set the batch size to 1 to make sure this works even
        # when there are multiple batches.
        m.BATCH_SIZE = 1

        assert "Reaper for Credential.expires" == m.SERVICE_NAME
        result = m.run_once()
        assert "Items deleted: 2" == result.achievements

        # The expired credentials have been reaped; the others
        # are still in the database.
        remaining = set(db.session.query(Credential).all())
        assert {active, eternal} == remaining

    def test_reap_patrons(self, db: DatabaseTransactionFixture):
        m = PatronRecordReaper(db.session)
        expired = db.patron()
        credential = db.credential(patron=expired)
        now = utc_now()
        expired.authorization_expires = now - datetime.timedelta(
            days=PatronRecordReaper.MAX_AGE + 1
        )
        active = db.patron()
        active.authorization_expires = now - datetime.timedelta(
            days=PatronRecordReaper.MAX_AGE - 1
        )
        result = m.run_once()
        assert "Items deleted: 1" == result.achievements
        remaining = db.session.query(Patron).all()
        assert [active] == remaining

        assert [] == db.session.query(Credential).all()


class TestWorkReaper:
    def test_end_to_end(self, db: DatabaseTransactionFixture):
        # Search mock
        class MockSearchIndex:
            removed = []

            def remove_work(self, work):
                self.removed.append(work)

        # First, create three works.

        # This work has a license pool.
        has_license_pool = db.work(with_license_pool=True)

        # This work had a license pool and then lost it.
        had_license_pool = db.work(with_license_pool=True)
        db.session.delete(had_license_pool.license_pools[0])

        # This work never had a license pool.
        never_had_license_pool = db.work(with_license_pool=False)

        # Each work has a presentation edition -- keep track of these
        # for later.
        works = db.session.query(Work)
        presentation_editions = [x.presentation_edition for x in works]

        # If and when Work gets database-level cascading deletes, this
        # is where they will all be triggered, with no chance that an
        # ORM-level delete is doing the work. So let's verify that all
        # of the cascades work.

        # First, set up some related items for each Work.

        # Each work is assigned to a genre.
        genre, ignore = Genre.lookup(db.session, "Science Fiction")
        for work in works:
            work.genres = [genre]

        # Each work is on the same CustomList.
        l, ignore = db.customlist("a list", num_entries=0)
        for work in works:
            l.add_entry(work)

        # Each work has a WorkCoverageRecord.
        for work in works:
            WorkCoverageRecord.add_for(work, operation="some operation")

        # Run the reaper.
        s = MockSearchIndex()
        m = WorkReaper(db.session, search_index_client=s)
        m.run_once()

        # Search index was updated
        assert 2 == len(s.removed)
        assert has_license_pool not in s.removed
        assert had_license_pool in s.removed
        assert never_had_license_pool in s.removed

        # Only the work with a license pool remains.
        assert [has_license_pool] == [x for x in works]

        # The presentation editions are still around, since they might
        # theoretically be used by other parts of the system.
        all_editions = db.session.query(Edition).all()
        for e in presentation_editions:
            assert e in all_editions

        # The surviving work is still assigned to the Genre, and still
        # has WorkCoverageRecords.
        assert [has_license_pool] == genre.works
        surviving_records = db.session.query(WorkCoverageRecord)
        assert surviving_records.count() > 0
        assert all(x.work == has_license_pool for x in surviving_records)

        # The CustomListEntries still exist, but two of them have lost
        # their work.
        assert 2 == len([x for x in l.entries if not x.work])
        assert [has_license_pool] == [x.work for x in l.entries if x.work]


class TestCollectionReaper:
    def test_query(self, db: DatabaseTransactionFixture):
        # This reaper is looking for collections that are marked for
        # deletion.
        collection = db.default_collection()
        reaper = CollectionReaper(db.session)
        assert [] == reaper.query().all()

        collection.marked_for_deletion = True
        assert [collection] == reaper.query().all()

    def test_reaper_delete_calls_collection_delete(
        self, db: DatabaseTransactionFixture
    ):
        # Unlike most ReaperMonitors, CollectionReaper.delete()
        # is overridden to call delete() on the object it was passed,
        # rather than just doing a database delete.
        class MockCollection:
            def delete(self):
                self.was_called = True

        collection = MockCollection()
        reaper = CollectionReaper(db.session)
        reaper.delete(collection)
        assert True == collection.was_called

    def test_run_once(self, db: DatabaseTransactionFixture):
        # End-to-end test
        c1 = db.default_collection()
        c2 = db.collection()
        c2.marked_for_deletion = True
        reaper = CollectionReaper(db.session)
        result = reaper.run_once()

        # The Collection marked for deletion has been deleted; the other
        # one is unaffected.
        assert [c1] == db.session.query(Collection).all()
        assert "Items deleted: 1" == result.achievements


class TestMeasurementReaper:
    def test_query(self, db: DatabaseTransactionFixture):
        # This reaper is looking for measurements that are not current.
        measurement, created = get_one_or_create(
            db.session, Measurement, is_most_recent=True
        )
        reaper = MeasurementReaper(db.session)
        assert [] == reaper.query().all()
        measurement.is_most_recent = False
        assert [measurement] == reaper.query().all()

    def test_run_once(self, db: DatabaseTransactionFixture):
        # End-to-end test
        measurement1, created = get_one_or_create(
            db.session,
            Measurement,
            quantity_measured="answer",
            value=12,
            is_most_recent=True,
        )
        measurement2, created = get_one_or_create(
            db.session,
            Measurement,
            quantity_measured="answer",
            value=42,
            is_most_recent=False,
        )
        reaper = MeasurementReaper(db.session)
        result = reaper.run_once()
        assert [measurement1] == db.session.query(Measurement).all()
        assert "Items deleted: 1" == result.achievements

    def test_disable(self, db: DatabaseTransactionFixture):
        # This reaper can be disabled with a configuration setting
        enabled = ConfigurationSetting.sitewide(
            db.session, Configuration.MEASUREMENT_REAPER
        )
        enabled.value = False
        measurement1, created = get_one_or_create(
            db.session,
            Measurement,
            quantity_measured="answer",
            value=12,
            is_most_recent=True,
        )
        measurement2, created = get_one_or_create(
            db.session,
            Measurement,
            quantity_measured="answer",
            value=42,
            is_most_recent=False,
        )
        reaper = MeasurementReaper(db.session)
        reaper.run()
        assert [measurement1, measurement2] == db.session.query(Measurement).all()
        enabled.value = True
        reaper.run()
        assert [measurement1] == db.session.query(Measurement).all()


class TestScrubberMonitor:
    def test_run_once(self, db: DatabaseTransactionFixture):
        # ScrubberMonitor is basically an abstract class, with
        # subclasses doing nothing but define missing constants. This
        # is an end-to-end test using a specific subclass,
        # CirculationEventLocationScrubber.

        m = CirculationEventLocationScrubber(db.session)
        assert "Scrubber for CirculationEvent.location" == m.SERVICE_NAME

        # CirculationEvents are only scrubbed if they have a location
        # *and* are older than MAX_AGE.
        now = utc_now()
        not_long_ago = m.cutoff + datetime.timedelta(days=1)
        long_ago = m.cutoff - datetime.timedelta(days=1)

        new, ignore = create(db.session, CirculationEvent, start=now, location="loc")
        recent, ignore = create(
            db.session, CirculationEvent, start=not_long_ago, location="loc"
        )
        old, ignore = create(
            db.session, CirculationEvent, start=long_ago, location="loc"
        )
        already_scrubbed, ignore = create(
            db.session, CirculationEvent, start=long_ago, location=None
        )

        # Only the old unscrubbed CirculationEvent is eligible
        # to be scrubbed.
        assert [old] == m.query().all()

        # Other reapers say items were 'deleted'; we say they were
        # 'scrubbed'.
        timestamp = m.run_once()
        assert "Items scrubbed: 1" == timestamp.achievements

        # Only the old unscrubbed CirculationEvent has been scrubbed.
        assert None == old.location
        for untouched in (new, recent):
            assert "loc" == untouched.location

    def test_specific_scrubbers(self, db: DatabaseTransactionFixture):
        # Check that all specific ScrubberMonitors are set up
        # correctly.
        circ = CirculationEventLocationScrubber(db.session)
        assert CirculationEvent.start == circ.timestamp_field
        assert CirculationEvent.location == circ.scrub_field
        assert 365 == circ.MAX_AGE

        patron = PatronNeighborhoodScrubber(db.session)
        assert Patron.last_external_sync == patron.timestamp_field
        assert Patron.cached_neighborhood == patron.scrub_field
        assert Patron.MAX_SYNC_TIME == patron.MAX_AGE
