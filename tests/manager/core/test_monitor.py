import datetime
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time
from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from palace.manager.core.monitor import (
    CollectionMonitor,
    IdentifierSweepMonitor,
    Monitor,
    SweepMonitor,
    TimelineMonitor,
    TimestampData,
)
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.service import container
from palace.manager.sqlalchemy.model.collection import CollectionMissing
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from palace.manager.util.sentinel import SentinelType
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
            PROTOCOL = OverdriveAPI.label()

        # Two collections.
        c1 = db.collection(protocol=OverdriveAPI)
        c2 = db.collection(protocol=BibliothecaAPI)

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
            PROTOCOL = OPDSAPI.label()

        # Here we have three OPDS import Collections...
        o1 = db.collection("o1")
        o2 = db.collection("o2")
        o3 = db.collection("o3")

        # ...and a Bibliotheca collection.
        b1 = db.collection(protocol=BibliothecaAPI)

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

    def test_retries(
        self,
        db: DatabaseTransactionFixture,
        sweep_monitor_fixture: SweepMonitorFixture,
    ):
        identifier = db.identifier()

        class FailOnFirstTwoCallsSucceedOnThird(MockSweepMonitor):
            def __init__(self, _db, **kwargs):
                super().__init__(_db, **kwargs)
                self.process_item_invocation_count = 0

            def process_item(self, item):
                self.process_item_invocation_count += 1
                if self.process_item_invocation_count == 1:
                    raise StaleDataError("stale data")
                elif self.process_item_invocation_count == 2:
                    raise ObjectDeletedError({}, "object deleted")
                elif self.process_item_invocation_count == 3:
                    raise InvalidRequestError("invalid request")
                else:
                    super().process_item(item)

        monitor = FailOnFirstTwoCallsSucceedOnThird(db.session)
        timestamp = monitor.timestamp()
        monitor.run()
        # we expect that process should have been called 3 times total
        assert monitor.process_item_invocation_count == 4
        # there shouldn't be an exception saved since it ultimately succeeded.
        assert timestamp.exception is None
        assert "Records processed: 1." == timestamp.achievements
        assert [identifier] == monitor.processed


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


class TestTimestampData:
    def test_constructor(self):
        # By default, all fields are set to None
        d = TimestampData()
        for i in (
            d.service,
            d.service_type,
            d.collection_id,
            d.start,
            d.finish,
            d.achievements,
            d.counter,
            d.exception,
        ):
            assert i == None

        # Some, but not all, of the fields can be set to real values.
        d = TimestampData(
            start="a", finish="b", achievements="c", counter="d", exception="e"
        )
        assert "a" == d.start
        assert "b" == d.finish
        assert "c" == d.achievements
        assert "d" == d.counter
        assert "e" == d.exception

    def test_is_failure(self):
        # A TimestampData represents failure if its exception is set to
        # any value other than None or SentinelType.ClearValue.
        d = TimestampData()
        assert False == d.is_failure

        d.exception = "oops"
        assert True == d.is_failure

        d.exception = None
        assert False == d.is_failure

        d.exception = SentinelType.ClearValue
        assert False == d.is_failure

    def test_is_complete(self):
        # A TimestampData is complete if it represents a failure
        # (see above) or if its .finish is set to any value other
        # than None or SentinelType.ClearValue

        d = TimestampData()
        assert False == d.is_complete

        d.finish = "done!"
        assert True == d.is_complete

        d.finish = None
        assert False == d.is_complete

        d.finish = SentinelType.ClearValue
        assert False == d.is_complete

        d.exception = "oops"
        assert True == d.is_complete

    @freeze_time()
    def test_finalize_minimal(self, db: DatabaseTransactionFixture):
        # Calling finalize() with only the minimal arguments sets the
        # timestamp values to sensible defaults and leaves everything
        # else alone.

        # This TimestampData starts out with everything set to None.
        d = TimestampData()
        d.finalize("service", "service_type", db.default_collection())

        # finalize() requires values for these arguments, and sets them.
        assert "service" == d.service
        assert "service_type" == d.service_type
        assert db.default_collection().id == d.collection_id

        # The timestamp values are set to sensible defaults.
        assert d.start == d.finish == utc_now()

        # Other fields are still at None.
        for i in d.achievements, d.counter, d.exception:
            assert i is None

    def test_finalize_full(self, db: DatabaseTransactionFixture):
        # You can call finalize() with a complete set of arguments.
        d = TimestampData()
        start = utc_now() - datetime.timedelta(days=1)
        finish = utc_now() - datetime.timedelta(hours=1)
        counter = 100
        d.finalize(
            "service",
            "service_type",
            db.default_collection(),
            start=start,
            finish=finish,
            counter=counter,
            exception="exception",
        )
        assert start == d.start
        assert finish == d.finish
        assert counter == d.counter
        assert "exception" == d.exception

        # If the TimestampData fields are already set to values other
        # than SentinelType.ClearValue, the required fields will be overwritten but
        # the optional fields will be left alone.
        new_collection = db.collection()
        d.finalize(
            "service2",
            "service_type2",
            new_collection,
            start=utc_now(),
            finish=utc_now(),
            counter=15555,
            exception="exception2",
        )
        # These have changed.
        assert "service2" == d.service
        assert "service_type2" == d.service_type
        assert new_collection.id == d.collection_id

        # These have not.
        assert start == d.start
        assert finish == d.finish
        assert counter == d.counter
        assert "exception" == d.exception

    def test_collection(self, db: DatabaseTransactionFixture):
        session = db.session

        d = TimestampData()
        d.finalize("service", "service_type", db.default_collection())
        assert db.default_collection() == d.collection(session)

    @freeze_time()
    def test_apply(self, db: DatabaseTransactionFixture):
        session = db.session

        # You can't apply a TimestampData that hasn't been finalized.
        d = TimestampData()
        with pytest.raises(ValueError) as excinfo:
            d.apply(session)
        assert "Not enough information to write TimestampData to the database." in str(
            excinfo.value
        )

        # Set the basic timestamp information. Optional fields will stay
        # at None.
        collection = db.default_collection()
        d.finalize("service", Timestamp.SCRIPT_TYPE, collection)
        d.apply(session)

        timestamp = Timestamp.lookup(
            session, "service", Timestamp.SCRIPT_TYPE, collection
        )
        assert timestamp.start == timestamp.finish == utc_now()

        # Now set the optional fields as well.
        d.counter = 100
        d.achievements = "yay"
        d.exception = "oops"
        d.apply(session)

        assert 100 == timestamp.counter
        assert "yay" == timestamp.achievements
        assert "oops" == timestamp.exception

        # We can also use apply() to clear out the values for all
        # fields other than the ones that uniquely identify the
        # Timestamp.
        d.start = SentinelType.ClearValue
        d.finish = SentinelType.ClearValue
        d.counter = SentinelType.ClearValue
        d.achievements = SentinelType.ClearValue
        d.exception = SentinelType.ClearValue
        d.apply(session)

        assert None == timestamp.start
        assert None == timestamp.finish
        assert None == timestamp.counter
        assert None == timestamp.achievements
        assert None == timestamp.exception
