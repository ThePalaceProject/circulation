import csv
from datetime import date, datetime, timedelta

from palace.manager.api.local_analytics_exporter import LocalAnalyticsExporter
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.work import WorkGenre
from palace.manager.sqlalchemy.util import get_one_or_create
from tests.fixtures.database import DatabaseTransactionFixture


class TestLocalAnalyticsExporter:
    """Tests the local analytics exporter."""

    def test_export(self, db: DatabaseTransactionFixture):
        exporter = LocalAnalyticsExporter()
        c1 = db.collection(name="c1")
        c2 = db.collection(name="c2")
        open_access = True
        w1 = db.work(with_open_access_download=True)
        w2 = db.work(with_open_access_download=True)
        [lp1] = w1.license_pools
        [lp2] = w2.license_pools
        lp1.collection = c1
        lp2.collection = c2
        lp1.open_access = True
        lp2.open_access = False

        edition1 = w1.presentation_edition
        edition1.publisher = "A publisher"
        edition1.imprint = "An imprint"
        edition1.medium = "Book"
        edition2 = w2.presentation_edition
        identifier1 = w1.presentation_edition.primary_identifier
        identifier2 = w2.presentation_edition.primary_identifier
        genres = db.session.query(Genre).order_by(Genre.name).all()
        get_one_or_create(db.session, WorkGenre, work=w1, genre=genres[0], affinity=0.2)
        get_one_or_create(db.session, WorkGenre, work=w1, genre=genres[1], affinity=0.3)
        get_one_or_create(db.session, WorkGenre, work=w1, genre=genres[2], affinity=0.5)

        # We expect the genre with the highest affinity to be put first.
        ordered_genre_string = ",".join(
            [genres[2].name, genres[1].name, genres[0].name]
        )
        get_one_or_create(db.session, WorkGenre, work=w2, genre=genres[1], affinity=0.5)
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        ]
        num = len(types)
        time = datetime.now() - timedelta(minutes=len(types))
        location = "11377"
        # Create a bunch of circulation events of different types,
        # all with the same .location.
        for type in types:
            get_one_or_create(
                db.session,
                CirculationEvent,
                license_pool=lp1,
                type=type,
                start=time,
                end=time,
                location=location,
            )
            time += timedelta(minutes=1)

        # Create a circulation event for a different book,
        # with no .location.
        get_one_or_create(
            db.session,
            CirculationEvent,
            license_pool=lp2,
            type=types[3],
            start=time,
            end=time,
        )

        # Run a query that excludes the last event created.
        today = date.today() - timedelta(days=1)
        output = exporter.export(db.session, today, time)
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row
        assert num == len(rows)

        # We've got one circulation event for each type.
        assert types == [row[1] for row in rows]

        # After the start date and event type, every row has the same
        # data. For the rest of this test we'll be using this block of
        # data to verify that circulation events for w1 look like we'd
        # expect.
        constant = [
            identifier1.identifier,
            identifier1.type,
            edition1.title,
            edition1.author,
            "fiction",
            w1.audience,
            edition1.publisher or "",
            edition1.imprint or "",
            edition1.language,
            w1.target_age_string or "",
            ordered_genre_string,
            location,
            c1.name,
            "",
            "",
            edition1.medium,
            lp1.data_source.name,
            "true",
        ]

        expected_column_count = 20
        for row in rows:
            assert expected_column_count == len(row)
            assert constant == row[2:]

        # Now run a query that includes the last event created.
        output = exporter.export(db.session, today, time + timedelta(minutes=1))
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row
        assert num + 1 == len(rows)
        assert types + [types[3]] == [row[1] for row in rows]

        # All but the last row is the same as in the previous report.
        all_but_last_row = rows[:-1]
        assert types == [row[1] for row in all_but_last_row]
        for row in all_but_last_row:
            assert expected_column_count == len(row)
            assert constant == row[2:]

        # Now let's look at the last row. It's got metadata from a
        # different book, and notably, there is no location.
        no_location = ""
        assert [
            types[3],
            identifier2.identifier,
            identifier2.type,
            edition2.title,
            edition2.author,
            "fiction",
            w2.audience,
            edition2.publisher or "",
            edition2.imprint or "",
            edition2.language,
            w2.target_age_string or "",
            genres[1].name,
            no_location,
            c2.name,
            "",
            "",
            edition1.medium,
            lp2.data_source.name,
            "false",
        ] == rows[-1][1:]

        output = exporter.export(db.session, today, today)
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row
        assert 0 == len(rows)

        # Gather events by library - these events have an associated library id
        # but it was not passed in the exporter
        library_name = "Library1"
        library_short_name = "LIB1"

        library = db.library(name=library_name, short_name=library_short_name)
        library2 = db.library()
        time = datetime.now() - timedelta(minutes=num)
        for type in types:
            get_one_or_create(
                db.session,
                CirculationEvent,
                license_pool=lp1,
                type=type,
                start=time,
                end=time,
                library=library,
                location=location,
            )
            time += timedelta(minutes=1)

        today = date.today() - timedelta(days=1)
        output = exporter.export(db.session, today, time)
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row

        # There have been a total of 11 events so far. No library ID was passed
        # so all events are returned.
        assert 11 == len(rows)

        # Pass in the library ID.
        today = date.today() - timedelta(days=1)
        output = exporter.export(db.session, today, time, library=library)
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row

        # There are five events with a library ID.
        constant_with_library = constant.copy()
        constant_with_library[13] = library_short_name
        constant_with_library[14] = library_name

        assert num == len(rows)
        assert types == [row[1] for row in rows]
        for row in rows:
            assert expected_column_count == len(row)
            assert constant_with_library == row[2:]

        # We are looking for events from a different library but there
        # should be no events associated with this library.
        time = datetime.now() - timedelta(minutes=num)
        today = date.today() - timedelta(days=1)
        output = exporter.export(db.session, today, time, library=library2)
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row

        assert 0 == len(rows)

        # Add example events that will be used to report by location
        user_added_locations = "11377,10018,11378"

        # The CM_HOLD_PLACE event should not be returned since it's not in the
        # list of events to gather when there is a list of locations.
        new_types = [
            CirculationEvent.CM_FULFILL,
            CirculationEvent.CM_CHECKOUT,
            CirculationEvent.OPEN_BOOK,
            CirculationEvent.CM_HOLD_PLACE,
        ]

        # Only information from the first three events should be returned.
        num = len(new_types) - 1
        time = datetime.now() - timedelta(minutes=num)
        for type in new_types:
            get_one_or_create(
                db.session,
                CirculationEvent,
                license_pool=lp1,
                type=type,
                start=time,
                end=time,
            )
            time += timedelta(minutes=1)

        output = exporter.export(
            db.session, today, time + timedelta(minutes=1), user_added_locations
        )
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row

        # No location was associated with each event so none will be returned
        assert 0 == len(rows)

        for type in new_types:
            get_one_or_create(
                db.session,
                CirculationEvent,
                license_pool=lp1,
                type=type,
                start=time,
                end=time,
                location="10001",
            )
            time += timedelta(minutes=1)

        output = exporter.export(
            db.session, today, time + timedelta(minutes=1), user_added_locations
        )
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row

        # Some events have a location but not in the list of locations that was passed
        assert 0 == len(rows)

        for type in new_types:
            get_one_or_create(
                db.session,
                CirculationEvent,
                license_pool=lp1,
                type=type,
                start=time,
                end=time,
                location="11377",
            )
            time += timedelta(minutes=1)

        output = exporter.export(
            db.session, today, time + timedelta(minutes=1), user_added_locations
        )
        reader = csv.reader(
            [row for row in output.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row

        # These events have a location that is in the list of acceptable
        # locations. The CM_HOLD_PLACE event is not in the list of event types
        # to gather information from, so it should not be returned even though
        # it has a location.
        assert num == len(rows)
        # The last event in new_types should not be returned
        assert new_types[:-1] == [row[1] for row in rows]

        # After the start time and event type, the rest of the row is
        # the same content we've come to expect.
        for row in rows:
            assert expected_column_count == len(row)
            assert constant == row[2:]
