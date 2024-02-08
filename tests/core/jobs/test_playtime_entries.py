from __future__ import annotations

import re
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch

import pytest
import pytz
from freezegun import freeze_time

from api.model.time_tracking import PlaytimeTimeEntry
from core.config import Configuration
from core.equivalents_coverage import EquivalentIdentifiersCoverageProvider
from core.jobs.playtime_entries import (
    PlaytimeEntriesEmailReportsScript,
    PlaytimeEntriesSummationScript,
)
from core.model import create
from core.model.collection import Collection
from core.model.identifier import Equivalency, Identifier
from core.model.library import Library
from core.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from core.util.datetime_helpers import datetime_utc, previous_months, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesEmailFixture


def create_playtime_entries(
    db: DatabaseTransactionFixture,
    identifier: Identifier,
    collection: Collection,
    library: Library,
    *entries: PlaytimeTimeEntry,
) -> list[PlaytimeEntry]:
    all_inserted = []
    for entry in entries:
        inserted = PlaytimeEntry(
            tracking_id=entry.id,
            timestamp=entry.during_minute,
            identifier_id=identifier.id,
            library_id=library.id,
            collection_id=collection.id,
            total_seconds_played=entry.seconds_played,
        )
        db.session.add(inserted)
        all_inserted.append(inserted)
    db.session.commit()
    return all_inserted


def date2k(h=0, m=0):
    """Quickly create a datetime object for testing"""
    return datetime(
        year=2000, month=1, day=1, hour=12, minute=0, second=0, tzinfo=pytz.UTC
    ) + timedelta(minutes=m, hours=h)


class TestPlaytimeEntriesSummationScript:
    def test_summation(self, db: DatabaseTransactionFixture):
        P = PlaytimeTimeEntry
        dk = date2k
        identifier = db.identifier()
        identifier2 = db.identifier()
        collection = db.default_collection()
        collection2 = db.collection()
        library = db.default_library()
        entries = create_playtime_entries(
            db,
            identifier,
            collection,
            library,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
            P(id="3", during_minute=dk(m=0), seconds_played=30),
        )
        entries2 = create_playtime_entries(
            db,
            identifier2,
            collection,
            library,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
            P(
                id="3", during_minute=dk(m=1), seconds_played=30
            ),  # One entry for the next minute
        )

        # Different collection, should get grouped separately
        entries3 = create_playtime_entries(
            db,
            identifier2,
            collection2,
            library,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=1), seconds_played=40),
        )

        # This entry should not be considered as it is too recent
        [out_of_scope_entry] = create_playtime_entries(
            db,
            identifier2,
            collection,
            library,
            P(id="5", during_minute=utc_now(), seconds_played=30),
        )

        # An already processed entry should not be considered
        [processed_entry] = create_playtime_entries(
            db,
            identifier2,
            collection,
            library,
            P(id="6", during_minute=dk(m=10), seconds_played=30),
        )
        processed_entry.processed = True

        PlaytimeEntriesSummationScript(db.session).run()

        playtimes = (
            db.session.query(PlaytimeSummary)
            .order_by(
                PlaytimeSummary.identifier_id,
                PlaytimeSummary.collection_id,
                PlaytimeSummary.library_id,
                PlaytimeSummary.timestamp,
            )
            .all()
        )

        assert len(playtimes) == 5

        id1time, id2time1, id2time2, id2col2time, id2col2time1 = playtimes

        assert id1time.identifier == identifier
        assert id1time.total_seconds_played == 120
        assert id1time.collection == collection
        assert id1time.library == library
        assert id1time.timestamp == dk()

        assert id2time1.identifier == identifier2
        assert id2time1.total_seconds_played == 90
        assert id2time1.collection == collection
        assert id2time1.library == library
        assert id2time1.timestamp == dk()

        assert id2time2.identifier == identifier2
        assert id2time2.collection == collection
        assert id2time2.library == library
        assert id2time2.total_seconds_played == 30
        assert id2time2.timestamp == dk(m=1)

        assert id2col2time.identifier == identifier2
        assert id2col2time.collection == collection2
        assert id2col2time.library == library
        assert id2col2time.total_seconds_played == 30
        assert id2col2time.timestamp == dk()

        assert id2col2time1.identifier == identifier2
        assert id2col2time1.collection == collection2
        assert id2col2time1.library == library
        assert id2col2time1.total_seconds_played == 40
        assert id2col2time1.timestamp == dk(m=1)

    def test_reap_processed_entries(self, db: DatabaseTransactionFixture):
        P = PlaytimeTimeEntry
        dk = date2k
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        entries = create_playtime_entries(
            db,
            identifier,
            collection,
            library,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
            P(id="3", during_minute=dk(m=0), seconds_played=30),
            # Processed but not reaped
            P(id="4", during_minute=utc_now() - timedelta(days=10), seconds_played=30),
            # The last will not get processed
            P(id="5", during_minute=utc_now(), seconds_played=30),
        )

        PlaytimeEntriesSummationScript(db.session).run()
        # Nothing reaped yet
        assert db.session.query(PlaytimeEntry).count() == 6
        # Last entry is not processed
        assert [e.processed for e in entries] == [True, True, True, True, True, False]

        # Second run
        PlaytimeEntriesSummationScript(db.session).run()
        # Only 2 should be left
        assert db.session.query(PlaytimeEntry).count() == 2
        assert list(
            db.session.query(PlaytimeEntry)
            .order_by(PlaytimeEntry.id)
            .values(PlaytimeEntry.tracking_id)
        ) == [("4",), ("5",)]


def date1m(days):
    return previous_months(number_of_months=1)[0] + timedelta(days=days)


def playtime(session, identifier, collection, library, timestamp, total_seconds):
    return create(
        session,
        PlaytimeSummary,
        identifier=identifier,
        collection=collection,
        library=library,
        timestamp=timestamp,
        total_seconds_played=total_seconds,
        identifier_str=identifier.urn,
        collection_name=collection.name,
        library_name=library.name,
    )[0]


class TestPlaytimeEntriesEmailReportsScript:
    def test_do_run(
        self,
        db: DatabaseTransactionFixture,
        services_email_fixture: ServicesEmailFixture,
    ):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        edition = db.edition()
        identifier2 = edition.primary_identifier
        collection2 = db.collection()
        library2 = db.library()

        isbn_ids: dict[str, Identifier] = {
            "i1": db.identifier(
                identifier_type=Identifier.ISBN, foreign_id="080442957X"
            ),
            "i2": db.identifier(
                identifier_type=Identifier.ISBN, foreign_id="9788175257665"
            ),
        }
        identifier.equivalencies = [
            Equivalency(
                input_id=identifier.id, output_id=isbn_ids["i1"].id, strength=0.5
            ),
            Equivalency(
                input_id=isbn_ids["i1"].id, output_id=isbn_ids["i2"].id, strength=1
            ),
        ]
        strongest_isbn = isbn_ids["i2"].identifier
        no_isbn = ""

        # We're using the RecursiveEquivalencyCache, so must refresh it.
        EquivalentIdentifiersCoverageProvider(db.session).run()

        playtime(db.session, identifier, collection, library, date1m(3), 1)
        playtime(db.session, identifier, collection, library, date1m(31), 2)
        playtime(
            db.session, identifier, collection, library, date1m(-31), 60
        )  # out of range: prior to the beginning of the default reporting period
        playtime(
            db.session, identifier, collection, library, date1m(95), 60
        )  # out of range: future
        playtime(db.session, identifier2, collection, library, date1m(3), 5)
        playtime(db.session, identifier2, collection, library, date1m(4), 6)

        # Collection2
        playtime(db.session, identifier, collection2, library, date1m(3), 100)
        # library2
        playtime(db.session, identifier, collection, library2, date1m(3), 200)
        # collection2 library2
        playtime(db.session, identifier, collection2, library2, date1m(3), 300)

        reporting_name = "test cm"
        with (
            patch("core.jobs.playtime_entries.csv.writer") as writer,
            patch(
                "core.jobs.playtime_entries.os.environ",
                new={
                    Configuration.REPORTING_EMAIL_ENVIRONMENT_VARIABLE: "reporting@test.email",
                    Configuration.REPORTING_NAME_ENVIRONMENT_VARIABLE: reporting_name,
                },
            ),
        ):
            # Act
            PlaytimeEntriesEmailReportsScript(db.session).run()

        # Assert
        assert (
            writer().writerow.call_count == 6
        )  # 1 header, 5 identifier,collection,library entries

        cutoff = date1m(0).replace(day=1)
        until = utc_now().date().replace(day=1)
        column1 = f"{cutoff} - {until}"
        call_args = writer().writerow.call_args_list
        assert call_args == [
            call(
                [
                    "date",
                    "urn",
                    "isbn",
                    "collection",
                    "library",
                    "title",
                    "total seconds",
                ]
            ),  # Header
            call(
                (
                    column1,
                    identifier.urn,
                    strongest_isbn,
                    collection2.name,
                    library2.name,
                    None,
                    300,
                )
            ),
            call(
                (
                    column1,
                    identifier.urn,
                    strongest_isbn,
                    collection2.name,
                    library.name,
                    None,
                    100,
                )
            ),
            call(
                (
                    column1,
                    identifier.urn,
                    strongest_isbn,
                    collection.name,
                    library2.name,
                    None,
                    200,
                )
            ),
            call(
                (
                    column1,
                    identifier.urn,
                    strongest_isbn,
                    collection.name,
                    library.name,
                    None,
                    1,
                )
            ),  # Identifier without edition
            call(
                (
                    column1,
                    identifier2.urn,
                    no_isbn,
                    collection.name,
                    library.name,
                    edition.title,
                    11,
                )
            ),  # Identifier with edition
        ]

        assert services_email_fixture.mock_emailer.send.call_count == 1
        assert services_email_fixture.mock_emailer.send.call_args == call(
            subject=f"{reporting_name}: Playtime Summaries {cutoff} - {until}",
            sender=services_email_fixture.sender_email,
            receivers=["reporting@test.email"],
            text="",
            html=None,
            attachments={
                f"playtime-summary-{reporting_name.replace(' ', '_')}-{cutoff}-{until}.csv": ""
            },  # Mock objects do not write data
        )

    def test_no_reporting_email(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        _ = playtime(db.session, identifier, collection, library, date1m(20), 1)

        with patch("core.jobs.playtime_entries.os.environ", new={}):
            script = PlaytimeEntriesEmailReportsScript(db.session)
            script._log = MagicMock()
            script.run()

            assert script._log.error.call_count == 1
            assert script._log.warning.call_count == 1
            assert "date,urn,isbn,collection," in script._log.warning.call_args[0][0]

    @pytest.mark.parametrize(
        "id_key, equivalents, default_value, expected_isbn",
        [
            # If the identifier is an ISBN, we will not use an equivalency.
            [
                "i1",
                (("g1", "g2", 1), ("g2", "i1", 1), ("g1", "i2", 0.5)),
                "",
                "080442957X",
            ],
            [
                "i2",
                (("g1", "g2", 1), ("g2", "i1", 0.5), ("g1", "i2", 1)),
                "",
                "9788175257665",
            ],
            ["i1", (("i1", "i2", 200),), "", "080442957X"],
            ["i2", (("i2", "i1", 200),), "", "9788175257665"],
            # If identifier is not an ISBN, but has an equivalency that is, use the strongest match.
            [
                "g2",
                (("g1", "g2", 1), ("g2", "i1", 1), ("g1", "i2", 0.5)),
                "",
                "080442957X",
            ],
            [
                "g2",
                (("g1", "g2", 1), ("g2", "i1", 0.5), ("g1", "i2", 1)),
                "",
                "9788175257665",
            ],
            # If we don't find an equivalent ISBN identifier, then we'll use the default.
            ["g2", (), "default value", "default value"],
            ["g1", (("g1", "g2", 1),), "default value", "default value"],
            # If identifier is None, expect default value.
            [None, (), "default value", "default value"],
        ],
    )
    def test__isbn_for_identifier(
        self,
        db: DatabaseTransactionFixture,
        id_key: str | None,
        equivalents: tuple[tuple[str, str, int | float]],
        default_value: str,
        expected_isbn: str,
    ):
        ids: dict[str, Identifier] = {
            "i1": db.identifier(
                identifier_type=Identifier.ISBN, foreign_id="080442957X"
            ),
            "i2": db.identifier(
                identifier_type=Identifier.ISBN, foreign_id="9788175257665"
            ),
            "g1": db.identifier(identifier_type=Identifier.GUTENBERG_ID),
            "g2": db.identifier(identifier_type=Identifier.GUTENBERG_ID),
        }
        equivalencies = [
            Equivalency(
                input_id=ids[equivalent[0]].id,
                output_id=ids[equivalent[1]].id,
                strength=equivalent[2],
            )
            for equivalent in equivalents
        ]
        test_identifier: Identifier | None = ids[id_key] if id_key is not None else None
        if test_identifier is not None:
            test_identifier.equivalencies = equivalencies

        # We're using the RecursiveEquivalencyCache, so must refresh it.
        EquivalentIdentifiersCoverageProvider(db.session).run()

        # Act
        result = PlaytimeEntriesEmailReportsScript._isbn_for_identifier(
            test_identifier,
            default_value=default_value,
        )
        # Assert
        assert result == expected_isbn

    @pytest.mark.parametrize(
        "current_utc_time, start_arg, expected_start, until_arg, expected_until",
        [
            # Default values from two dates within the same month (next two cases).
            [
                datetime(2020, 1, 1, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            [
                datetime(2020, 1, 31, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            # `start` specified, `until` defaulted.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2019-06-11",
                datetime_utc(2019, 6, 11, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            # `start` defaulted, `until` specified.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                "2019-12-20",
                datetime_utc(2019, 12, 20, 0, 0, 0),
            ],
            # When both dates are specified, the current datetime doesn't matter.
            # Both dates specified, but we test at a specific time here anyway.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "4099-07-03",
                datetime_utc(4099, 7, 3, 0, 0, 0),
                "4150-04-30",
                datetime_utc(4150, 4, 30, 0, 0, 0),
            ],
        ],
    )
    def test_parse_command_line(
        self,
        current_utc_time: datetime,
        start_arg: str | None,
        expected_start: datetime,
        until_arg: str | None,
        expected_until: datetime,
    ):
        start_args = ["--start", start_arg] if start_arg else []
        until_args = ["--until", until_arg] if until_arg else []
        cmd_args = start_args + until_args

        with freeze_time(current_utc_time):
            parsed = PlaytimeEntriesEmailReportsScript.parse_command_line(
                cmd_args=cmd_args
            )
        assert expected_start == parsed.start
        assert expected_until == parsed.until
        assert pytz.UTC == parsed.start.tzinfo
        assert pytz.UTC == parsed.until.tzinfo

    @pytest.mark.parametrize(
        "current_utc_time, start_arg, expected_start, until_arg, expected_until",
        [
            # `start` specified, `until` defaulted.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2020-02-01",
                datetime_utc(2020, 2, 1, 0, 0, 0),
                None,
                datetime_utc(2020, 1, 1, 0, 0, 0),
            ],
            # `start` defaulted, `until` specified.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                None,
                datetime_utc(2019, 12, 1, 0, 0, 0),
                "2019-06-11",
                datetime_utc(2019, 6, 11, 0, 0, 0),
            ],
            # When both dates are specified, the current datetime doesn't matter.
            # Both dates specified, but we test at a specific time here anyway.
            [
                datetime(2020, 1, 31, 0, 0, 0),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "2019-04-30",
                datetime_utc(2019, 4, 30, 0, 0, 0),
                "2018-07-03",
                datetime_utc(2018, 7, 3, 0, 0, 0),
            ],
            # The same dates are specified, but we test at the actual current time.
            [
                utc_now(),
                "4150-04-30",
                datetime_utc(4150, 4, 30, 0, 0, 0),
                "4099-07-03",
                datetime_utc(4099, 7, 3, 0, 0, 0),
            ],
        ],
    )
    def test_parse_command_line_start_not_before_until(
        self,
        capsys,
        current_utc_time: datetime,
        start_arg: str | None,
        expected_start: datetime,
        until_arg: str | None,
        expected_until: datetime,
    ):
        start_args = ["--start", start_arg] if start_arg else []
        until_args = ["--until", until_arg] if until_arg else []
        cmd_args = start_args + until_args

        with freeze_time(current_utc_time), pytest.raises(SystemExit) as excinfo:
            parsed = PlaytimeEntriesEmailReportsScript.parse_command_line(
                cmd_args=cmd_args
            )
        _, err = capsys.readouterr()
        assert 2 == excinfo.value.code
        assert re.search(r"start date \(.*\) must be before until date \(.*\).", err)
