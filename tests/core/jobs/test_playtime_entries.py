from datetime import datetime, timedelta
from typing import List
from unittest.mock import call, patch

import pytz

from api.model.time_tracking import PlaytimeTimeEntry
from core.config import Configuration
from core.jobs.playtime_entries import (
    PlaytimeEntriesEmailReportsScript,
    PlaytimeEntriesSummationScript,
)
from core.model import create
from core.model.identifier import Identifier
from core.model.time_tracking import IdentifierPlaytime, IdentifierPlaytimeEntry
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


def create_playtime_entries(
    db: DatabaseTransactionFixture, identifier: Identifier, *entries: PlaytimeTimeEntry
) -> List[IdentifierPlaytimeEntry]:
    all_inserted = []
    for entry in entries:
        inserted = IdentifierPlaytimeEntry(
            tracking_id=entry.id,
            timestamp=entry.during_minute,
            identifier_id=identifier.id,
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
        entries = create_playtime_entries(
            db,
            identifier,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
            P(id="3", during_minute=dk(m=0), seconds_played=30),
        )
        entries2 = create_playtime_entries(
            db,
            identifier2,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
            P(
                id="3", during_minute=dk(m=1), seconds_played=30
            ),  # One entry for the next minute
        )

        # This entry should not be considered as it is too recent
        [out_of_scope_entry] = create_playtime_entries(
            db,
            identifier2,
            P(id="5", during_minute=utc_now(), seconds_played=30),
        )

        # An already processed entry should not be considered
        [processed_entry] = create_playtime_entries(
            db,
            identifier2,
            P(id="6", during_minute=dk(m=10), seconds_played=30),
        )
        processed_entry.processed = True

        PlaytimeEntriesSummationScript(db.session).run()

        playtimes = (
            db.session.query(IdentifierPlaytime)
            .order_by(IdentifierPlaytime.identifier_id, IdentifierPlaytime.timestamp)
            .all()
        )

        assert len(playtimes) == 3

        id1time, id2time1, id2time2 = playtimes

        assert id1time.identifier == identifier
        assert id1time.total_seconds_played == 120
        assert id1time.timestamp == dk()

        assert id2time1.identifier == identifier2
        assert id2time1.total_seconds_played == 90
        assert id2time1.timestamp == dk()

        assert id2time2.identifier == identifier2
        assert id2time2.total_seconds_played == 30
        assert id2time2.timestamp == dk(m=1)

    def test_reap_processed_entries(self, db: DatabaseTransactionFixture):
        P = PlaytimeTimeEntry
        dk = date2k
        identifier = db.identifier()
        entries = create_playtime_entries(
            db,
            identifier,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
            P(id="3", during_minute=dk(m=0), seconds_played=30),
            # The last 2 will not get processed
            P(id="4", during_minute=utc_now(), seconds_played=30),
            P(id="5", during_minute=utc_now(), seconds_played=30),
        )

        PlaytimeEntriesSummationScript(db.session).run()
        # Nothing reaped yet
        assert db.session.query(IdentifierPlaytimeEntry).count() == 6
        # Last 2 are not processed
        assert [e.processed for e in entries] == [True, True, True, True, False, False]

        # Second run
        PlaytimeEntriesSummationScript(db.session).run()
        # Only 2 should be left
        assert db.session.query(IdentifierPlaytimeEntry).count() == 2


def date3m(days):
    return (utc_now() - timedelta(days=(90 - days))).date()


def playtime(session, identifier, timestamp, total_seconds):
    return create(
        session,
        IdentifierPlaytime,
        identifier=identifier,
        timestamp=timestamp,
        total_seconds_played=total_seconds,
        identifier_str=identifier.urn,
    )[0]


class TestPlaytimeEntriesEmailReportsScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        identifier = db.identifier()
        edition = db.edition()
        identifier2 = edition.primary_identifier

        playtime(db.session, identifier, date3m(3), 1)
        playtime(db.session, identifier, date3m(31), 2)
        playtime(
            db.session, identifier, date3m(-31), 60
        )  # out of range: more than a month prior to the quarter
        playtime(db.session, identifier, date3m(91), 60)  # out of range: future
        playtime(db.session, identifier2, date3m(3), 5)
        playtime(db.session, identifier2, date3m(4), 6)

        with (
            patch("core.jobs.playtime_entries.csv.writer") as writer,
            patch("core.jobs.playtime_entries.EmailManager") as email,
            patch(
                "core.jobs.playtime_entries.os.environ",
                new={
                    Configuration.REPORTING_EMAIL_ENVIRONMENT_VARIABLE: "reporting@test.email"
                },
            ),
        ):
            PlaytimeEntriesEmailReportsScript(db.session).run()

        assert writer().writerow.call_count == 3  # 1 header, 2 identifiers

        cutoff = date3m(0).replace(day=1)
        until = utc_now().date().replace(day=1)
        column1 = f"{cutoff} - {until}"
        call_args = writer().writerow.call_args_list
        assert call_args == [
            call(["date", "urn", "title", "total seconds"]),  # Header
            call((column1, identifier.urn, None, 3)),  # Identifier without edition
            call(
                (column1, identifier2.urn, edition.title, 11)
            ),  # Identifier with edition
        ]

        assert email.send_email.call_count == 1
        assert email.send_email.call_args == call(
            f"Playtime Summaries {cutoff} - {until}",
            receivers=["reporting@test.email"],
            text="",
            attachments={
                f"playtime-summary-{cutoff}-{until}": ""
            },  # Mock objects do not write data
        )
