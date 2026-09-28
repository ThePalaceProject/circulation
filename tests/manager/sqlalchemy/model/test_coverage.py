import datetime
from unittest.mock import MagicMock

from freezegun import freeze_time

from palace.util.datetime_helpers import datetime_utc, utc_now

from palace.manager.core.monitor import TimestampData
from palace.manager.sqlalchemy.model.coverage import (
    Timestamp,
)
from palace.manager.util.sentinel import SentinelType
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

    def test_stamp(self, db: DatabaseTransactionFixture) -> None:
        service = "service"
        type = Timestamp.SCRIPT_TYPE

        with freeze_time("2010-01-01"):
            # If no date is specified, the value of the timestamp is the time
            # stamp() was called.
            stamp = Timestamp.stamp(db.session, service, type)
            assert stamp.start == stamp.finish == utc_now()
            assert service == stamp.service
            assert type == stamp.service_type
            assert None == stamp.collection
            assert None == stamp.achievements
            assert None == stamp.counter
            assert None == stamp.exception

        with freeze_time("2010-01-02"):
            # Calling stamp() again will update the Timestamp.
            stamp2 = Timestamp.stamp(
                db.session,
                service,
                type,
                achievements="yay",
                counter=100,
                exception="boo",
            )
            assert stamp == stamp2
            assert stamp.start == stamp.finish == utc_now()
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

        # Passing in SentinelType.ClearValue for start, end, or exception will
        # clear an existing Timestamp.
        stamp4 = Timestamp.stamp(
            db.session,
            service,
            type,
            start=SentinelType.ClearValue,
            finish=SentinelType.ClearValue,
            exception=SentinelType.ClearValue,
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

    def test_elapsed(self) -> None:
        stamp = Timestamp()

        # If start is None, elapsed is None.
        assert stamp.start is None
        assert stamp.elapsed is None

        # If start is set but finish is None, elapsed uses the current time
        # as the finish time.
        stamp.start = datetime_utc(2010, 1, 1, 0, 0)
        with freeze_time(datetime_utc(2010, 1, 1, 0, 1)):
            assert stamp.finish is None
            assert stamp.elapsed == datetime.timedelta(minutes=1)

        # If both start and finish are set, elapsed uses those values.
        stamp.finish = datetime_utc(2010, 1, 1, 0, 10)
        assert stamp.elapsed == datetime.timedelta(minutes=10)

    def test_elapsed_seconds(self) -> None:
        stamp = Timestamp()

        # If elapsed is None, elapsed_seconds is None.
        assert stamp.elapsed is None
        assert stamp.elapsed_seconds is None

        # If elapsed is set, elapsed_seconds is the number of seconds
        # in elapsed, as a float.
        stamp.start = datetime_utc(2010, 1, 1, 0, 0)
        stamp.finish = datetime_utc(2010, 1, 1, 0, 1, 5, 6000)

        assert stamp.elapsed_seconds == 65.006

    def test_recording(self) -> None:
        stamp = Timestamp()

        # Set some initial values for start, finish, and exception
        stamp.start = MagicMock()
        stamp.finish = MagicMock()
        stamp.exception = MagicMock()

        # When we enter the recording context, start is set to the
        # current time, and finish and exception are cleared.
        now = utc_now()
        delta = datetime.timedelta(minutes=5)
        with freeze_time(now) as frozen_time:
            with stamp.recording():
                assert stamp.start == now
                assert stamp.finish is None
                assert stamp.exception is None
                frozen_time.tick(delta=delta)

            # When we exit the context, finish is set to the current time.
            assert stamp.start == now
            assert stamp.finish == now + delta

        # If an exception was raised, it is recorded in the exception
        # field.
        now = now + datetime.timedelta(minutes=10)
        with freeze_time(now) as frozen_time:
            try:
                with stamp.recording():
                    assert stamp.start == now
                    assert stamp.finish is None
                    assert stamp.exception is None
                    frozen_time.tick(delta=delta)
                    raise ValueError("testing")
            except ValueError:
                pass

            assert stamp.start == now
            assert stamp.finish == now + delta
            assert stamp.exception == "testing"
