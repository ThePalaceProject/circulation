from __future__ import annotations

import logging
from typing import cast
from unittest.mock import MagicMock

from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from palace.manager.api.overdrive.monitor import (
    NewTitlesOverdriveCollectionMonitor,
    OverdriveCirculationMonitor,
    OverdriveCollectionReaper,
    OverdriveFormatSweep,
    RecentOverdriveCollectionMonitor,
)
from palace.manager.core.metadata_layer import TimestampData
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.time import Time
from tests.mocks.overdrive import MockOverdriveAPI


class TestOverdriveCirculationMonitor:
    def test_run(self, overdrive_api_fixture: OverdriveAPIFixture, time_fixture: Time):
        db = overdrive_api_fixture.db

        # An end-to-end test verifying that this Monitor manages its
        # state across multiple runs.
        #
        # This tests a lot of code that's technically not in Monitor,
        # but when the Monitor API changes, it may require changes to
        # this particular monitor, and it's good to have a test that
        # will fail if that's true.
        class Mock(OverdriveCirculationMonitor):
            def catch_up_from(self, start, cutoff, progress):
                self.catch_up_from_called_with = (start, cutoff, progress)

        monitor = Mock(db.session, overdrive_api_fixture.collection)

        monitor.run()
        start, cutoff, progress = monitor.catch_up_from_called_with
        now = utc_now()

        # The first time this Monitor is called, its 'start time' is
        # the current time, and we ask for an overlap of one minute.
        # This isn't very effective, but we have to start somewhere.
        #
        # (This isn't how the Overdrive collection is initially
        # populated, BTW -- that's NewTitlesOverdriveCollectionMonitor.)
        time_fixture.time_eq(start, now - monitor.OVERLAP)
        time_fixture.time_eq(cutoff, now)
        timestamp = monitor.timestamp()
        assert start == timestamp.start
        assert cutoff == timestamp.finish

        # The second time the Monitor is called, its 'start time'
        # is one minute before the previous cutoff time.
        monitor.run()
        new_start, new_cutoff, new_progress = monitor.catch_up_from_called_with
        now = utc_now()
        assert new_start == cutoff - monitor.OVERLAP
        time_fixture.time_eq(new_cutoff, now)

    def test_catch_up_from(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        # catch_up_from() asks Overdrive about recent changes by
        # calling recently_changed_ids().
        #
        # It mirrors those changes locally by calling
        # update_licensepool().
        #
        # If this is our first time encountering a book, a
        # DISTRIBUTOR_TITLE_ADD analytics event is sent out.
        #
        # The method stops when should_stop() -- called on every book
        # -- returns True.
        class MockAPI:
            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def update_licensepool(self, book_id):
                pool, is_new, is_changed = self.licensepools.pop(0)
                self.update_licensepool_calls.append((book_id, pool))
                return pool, is_new, is_changed

        class MockAnalytics(Analytics):
            def __init__(self):
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        class MockMonitor(OverdriveCirculationMonitor):
            recently_changed_ids_called_with = None
            should_stop_calls = []

            def recently_changed_ids(self, start, cutoff):
                self.recently_changed_ids_called_with = (start, cutoff)
                return [1, 2, None, 3, 4]

            def should_stop(self, start, book, is_changed):
                # We're going to stop after the third valid book,
                # ensuring that we never ask 'Overdrive' for the
                # fourth book.
                self.should_stop_calls.append((start, book, is_changed))
                if book == 3:
                    return True
                return False

        monitor = MockMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,  # type: ignore[arg-type]
            analytics=MockAnalytics(),
        )
        api = cast(MockAPI, monitor.api)

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)

        # The 'Overdrive API' is ready to tell us about four books,
        # but only one of them (the first) represents a change from what
        # we already know.
        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        lp3 = db.licensepool(None)
        lp4 = MagicMock()
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))
        api.licensepools.append(lp4)

        progress = TimestampData()
        start = MagicMock()
        cutoff = MagicMock()
        monitor.catch_up_from(start, cutoff, progress)

        # The monitor called recently_changed_ids with the start and
        # cutoff times. It returned five 'books', one of which was None --
        # simulating a lack of data from Overdrive.
        assert (start, cutoff) == monitor.recently_changed_ids_called_with

        # The monitor ignored the empty book and called
        # update_licensepool on the first three valid 'books'. The
        # mock API delivered the first three LicensePools from the
        # queue.
        assert [(1, lp1), (2, lp2), (3, lp3)] == api.update_licensepool_calls

        # After each book was processed, should_stop was called, using
        # the LicensePool, the start date, plus information about
        # whether the LicensePool was changed (or created) during
        # update_licensepool().
        assert [
            (start, 1, True),
            (start, 2, False),
            (start, 3, True),
        ] == monitor.should_stop_calls

        # should_stop returned True on the third call, and at that
        # point we gave up.

        # The fourth (bogus) LicensePool is still in api.licensepools,
        # because we never asked for it.
        assert [lp4] == api.licensepools

        # A single analytics event was sent out, for the first LicensePool,
        # the one that update_licensepool said was new.
        #
        # No more DISTRIBUTOR events
        assert len(monitor.analytics.events) == 0

        # The incoming TimestampData object was updated with
        # a summary of what happened.
        #
        # We processed four books: 1, 2, None (which was ignored)
        # and 3.
        assert "Books processed: 4." == progress.achievements

    def test_catch_up_from_with_failures_retried(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """Check that book failures are retried."""
        db = overdrive_api_fixture.db

        class MockAPI:
            tries: dict[str, int] = {}

            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def recently_changed_ids(self, start, cutoff):
                return [1, 2, 3]

            def update_licensepool(self, book_id):
                current_count = self.tries.get(str(book_id)) or 0
                current_count = current_count + 1
                self.tries[str(book_id)] = current_count

                if current_count < 1:
                    raise StaleDataError("Ouch!")
                elif current_count < 2:
                    raise ObjectDeletedError({}, "Ouch Deleted!")

                pool, is_new, is_changed = self.licensepools.pop(0)
                self.update_licensepool_calls.append((book_id, pool))
                return pool, is_new, is_changed

        class MockAnalytics(Analytics):
            def __init__(self):
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        monitor = OverdriveCirculationMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,  # type: ignore[arg-type]
            analytics=MockAnalytics(),
        )
        api = cast(MockAPI, monitor.api)

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)

        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        lp3 = db.licensepool(None)
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))

        progress = TimestampData()
        start = MagicMock()
        cutoff = MagicMock()
        monitor.catch_up_from(start, cutoff, progress)

        assert api.tries["1"] == 2
        assert api.tries["2"] == 2
        assert api.tries["3"] == 2
        assert not progress.is_failure

    def test_catch_up_from_with_failures_all(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """If an individual book fails, the import continues, but ends in failure after handling all the books."""
        db = overdrive_api_fixture.db

        class MockAPI:
            tries: dict[str, int] = {}

            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def recently_changed_ids(self, start, cutoff):
                return [1, 2, 3]

            def update_licensepool(self, book_id):
                current_count = self.tries.get(str(book_id)) or 0
                current_count = current_count + 1
                self.tries[str(book_id)] = current_count
                raise Exception("Generic exception that will cause bypass retries")

        class MockAnalytics(Analytics):
            def __init__(self):
                self.events = []

            def collect_event(self, *args):
                self.events.append(args)

        monitor = OverdriveCirculationMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,  # type: ignore[arg-type]
            analytics=MockAnalytics(),
        )

        api = cast(MockAPI, monitor.api)

        # A MockAnalytics object was created and is ready to receive analytics
        # events.
        assert isinstance(monitor.analytics, MockAnalytics)

        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        lp3 = db.licensepool(None)
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))
        api.licensepools.append((lp3, False, True))

        progress = TimestampData()
        start = MagicMock()
        cutoff = MagicMock()
        monitor.catch_up_from(start, cutoff, progress)

        assert api.tries["1"] == 1
        assert api.tries["2"] == 1
        assert api.tries["3"] == 1
        assert progress.is_failure

    def test_retries_for_retryable_errors(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        """If  individual books fail due to retryable conditions, confirm success"""
        db = overdrive_api_fixture.db
        book1 = 1
        book2 = 2

        class MockAPI:
            tries: dict[str, int] = {}

            def __init__(self, *ignore, **kwignore):
                self.licensepools = []
                self.update_licensepool_calls = []

            def recently_changed_ids(self, start, cutoff):
                return [book1, book2]

            def update_licensepool(self, book_id):
                current_count = self.tries.get(str(book_id)) or 0
                current_count = current_count + 1
                self.tries[str(book_id)] = current_count
                if book_id == 1:
                    if current_count == 1:
                        raise StaleDataError("stale data")
                elif book_id == 2:
                    if current_count == 1:
                        raise ObjectDeletedError({}, "object deleted")

                return None, None, False

        monitor = OverdriveCirculationMonitor(
            db.session,
            overdrive_api_fixture.collection,
            api_class=MockAPI,  # type: ignore[arg-type]
        )

        api = cast(MockAPI, monitor.api)

        lp1 = db.licensepool(None)
        lp1.last_checked = utc_now()
        lp2 = db.licensepool(None)
        api.licensepools.append((lp1, True, True))
        api.licensepools.append((lp2, False, False))

        progress = TimestampData()
        start = MagicMock()
        cutoff = MagicMock()
        monitor.catch_up_from(start, cutoff, progress)

        for b in [book1, book2]:
            assert api.tries[str(b)] == 2
        assert not progress.is_failure


class TestNewTitlesOverdriveCollectionMonitor:
    def test_recently_changed_ids(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db

        class MockAPI:
            def __init__(self, *args, **kwargs):
                pass

            def all_ids(self):
                return "all of the ids"

        monitor = NewTitlesOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockAPI
        )
        assert "all of the ids" == monitor.recently_changed_ids(
            MagicMock(), MagicMock()
        )

    def test_should_stop(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        monitor = NewTitlesOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )

        # for this test, we will not count consecutive out of scope dates: if one
        # title is out of scope, we should stop.
        NewTitlesOverdriveCollectionMonitor.MAX_CONSECUTIVE_OUT_OF_SCOPE_DATES = 0

        m = monitor.should_stop

        # If the monitor has never run before, we need to keep going
        # until we run out of books.
        assert False == m(None, MagicMock(), MagicMock())
        assert False == m(monitor.NEVER, MagicMock(), MagicMock())  # type: ignore[arg-type]

        # If information is missing or invalid, we assume that we
        # should keep going.
        start = datetime_utc(2018, 1, 1)
        assert False == m(start, {}, MagicMock())
        assert False == m(start, {"date_added": None}, MagicMock())
        assert False == m(start, {"date_added": "Not a date"}, MagicMock())

        # Here, we're actually comparing real dates, using the date
        # format found in the Overdrive API. A date that's after the
        # `start` date means we should keep going backwards. A date before
        # the `start` date means we should stop.
        assert False == m(
            start, {"date_added": "2019-07-12T11:06:38.157+01:00"}, MagicMock()
        )
        assert True == m(
            start, {"date_added": "2017-07-12T11:06:38.157-04:00"}, MagicMock()
        )

    def test_should_stop_with_consecutive_data_threshold_gt_zero(
        self, overdrive_api_fixture: OverdriveAPIFixture, caplog
    ):
        caplog.set_level(logging.INFO)

        db = overdrive_api_fixture.db
        monitor = NewTitlesOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )

        # for this test, we will count consecutive out of scope date
        NewTitlesOverdriveCollectionMonitor.MAX_CONSECUTIVE_OUT_OF_SCOPE_DATES = 1

        m = monitor.should_stop

        start = datetime_utc(2018, 1, 1)

        # in scope - should continue
        in_scope_properties = {"date_added": "2019-07-12T11:06:38.157+01:00"}
        assert False == m(start, in_scope_properties, MagicMock())

        assert "Date added: 2019-07-12 11:06:38.157000+01:00" in caplog.messages[-1]

        # out of scope but counter threshold not yet exceeded: should continue
        out_of_scope_properties = {"date_added": "2017-07-12T11:06:38.157-04:00"}
        assert not m(start, out_of_scope_properties, MagicMock())

        assert "Date added: 2017-07-12 11:06:38.157000-04:00" in caplog.messages[-1]

        # in scope - should continue, expect reset
        assert not m(start, in_scope_properties, MagicMock())

        assert (
            "We encountered a title that was added within our scope that "
            "followed a title that was out of scope"
        ) in caplog.messages[-1]

        # out of scope but counter threshold not yet exceeded: should continue
        assert not m(start, out_of_scope_properties, MagicMock())

        # second out of scope:  threshold exceeded:  should stop
        assert m(start, out_of_scope_properties, MagicMock())

        assert (
            "Max consecutive out of scope date threshold of 1 breached!"
            in caplog.messages[-1]
        )


class TestNewTitlesOverdriveCollectionMonitor2:
    def test_should_stop(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        monitor = RecentOverdriveCollectionMonitor(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
        assert 0 == monitor.consecutive_unchanged_books
        m = monitor.should_stop

        # This book hasn't been changed, but we're under the limit, so we should
        # keep going.
        assert False == m(MagicMock(), MagicMock(), False)
        assert 1 == monitor.consecutive_unchanged_books

        assert False == m(MagicMock(), MagicMock(), False)
        assert 2 == monitor.consecutive_unchanged_books

        # This book has changed, so our counter gets reset.
        assert False == m(MagicMock(), MagicMock(), True)
        assert 0 == monitor.consecutive_unchanged_books

        # When we're at the limit, and another book comes along that hasn't
        # been changed, _then_ we decide to stop.
        monitor.consecutive_unchanged_books = (
            monitor.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS
        )
        assert True == m(MagicMock(), MagicMock(), False)
        assert (
            monitor.MAXIMUM_CONSECUTIVE_UNCHANGED_BOOKS + 1
            == monitor.consecutive_unchanged_books
        )


class TestReaper:
    def test_instantiate(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveCollectionReaper(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )


class TestOverdriveFormatSweep:
    def test_process_item(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveFormatSweep(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
        mock_api = cast(MockOverdriveAPI, monitor.api)
        mock_api.queue_collection_token()
        # We're not testing that the work actually gets done (that's
        # tested in test_update_formats), only that the monitor
        # implements the expected process_item API without crashing.
        mock_api.queue_response(404)
        edition, pool = db.edition(with_license_pool=True)
        monitor.process_item(pool.identifier)

    def test_process_item_multiple_licence_pools(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        # Make sure that we only call update_formats once when an item
        # is part of multiple licensepools.

        class MockApi(MockOverdriveAPI):
            update_format_calls = 0

            def update_formats(self, licensepool):
                self.update_format_calls += 1

        monitor = OverdriveFormatSweep(
            db.session, overdrive_api_fixture.collection, api_class=MockApi
        )
        mock_api = cast(MockApi, monitor.api)
        mock_api.queue_collection_token()
        mock_api.queue_response(404)

        edition = db.edition()
        collection1 = db.collection(name="Collection 1")
        pool1 = db.licensepool(edition, collection=collection1)

        collection2 = db.collection(name="Collection 2")
        pool2 = db.licensepool(edition, collection=collection2)

        monitor.process_item(pool1.identifier)
        assert 1 == mock_api.update_format_calls
