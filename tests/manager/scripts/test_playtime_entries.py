from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Literal
from unittest.mock import MagicMock, call, patch

import pytest
import pytz
from freezegun import freeze_time
from sqlalchemy.sql.expression import and_, null

from palace.manager.api.model.time_tracking import PlaytimeTimeEntry
from palace.manager.core.config import Configuration
from palace.manager.core.equivalents_coverage import (
    EquivalentIdentifiersCoverageProvider,
)
from palace.manager.scripts.playtime_entries import (
    PlaytimeEntriesEmailReportsScript,
    PlaytimeEntriesSummationScript,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from palace.manager.util.datetime_helpers import datetime_utc, previous_months, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesEmailFixture


def create_playtime_entries(
    db: DatabaseTransactionFixture,
    identifier: Identifier,
    collection: Collection,
    library: Library,
    loan_identifier: str,
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
            identifier_str=identifier.urn,
            collection_name=collection.name,
            library_name=library.name or "",
            loan_identifier=loan_identifier,
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
        collection = db.collection()
        library = db.default_library()

        c2_old_name = "Colletcion 2 with typo"
        c2_new_name = "Collection 2"
        id2_old_value = "original-id"
        id2_new_value = "updated-id"
        id2_old_urn = f"urn:isbn:{id2_old_value}"
        id2_new_urn = f"urn:isbn:{id2_new_value}"
        l2_old_name = "Lirbrary 2 with typo"
        l2_new_name = "Library 2"

        identifier2 = db.identifier(
            identifier_type=Identifier.ISBN, foreign_id=id2_old_value
        )
        collection2 = db.collection(name=c2_old_name)
        library2 = db.library(name=l2_old_name)
        loan_identifier, loan_identifier2, loan_identifier3, loan_identifier4 = (
            f"loan-id:{x}" for x in range(0, 4)
        )
        entries = create_playtime_entries(
            db,
            identifier,
            collection,
            library,
            loan_identifier,
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
            loan_identifier2,
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
            loan_identifier3,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=1), seconds_played=40),
        )

        # Different library, should get grouped separately.
        entries4 = create_playtime_entries(
            db,
            identifier2,
            collection2,
            library2,
            loan_identifier4,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=40),
            P(id="2", during_minute=dk(m=0), seconds_played=30),
        )

        # This entry should not be considered as it is too recent
        [out_of_scope_entry] = create_playtime_entries(
            db,
            identifier2,
            collection,
            library,
            loan_identifier2,
            P(id="5", during_minute=utc_now(), seconds_played=30),
        )

        # An already processed entry should not be considered
        [processed_entry] = create_playtime_entries(
            db,
            identifier2,
            collection,
            library,
            loan_identifier2,
            P(id="6", during_minute=dk(m=10), seconds_played=30),
        )
        processed_entry.processed = True

        # Update identifer2, collection2 and library2.
        # The existing playtime entries should remain unchanged, but the resulting
        # playtime summary records should reflect these changes.
        identifier2.identifier = id2_new_value
        collection2.integration_configuration.name = c2_new_name
        library2.name = l2_new_name

        presummation_entries = db.session.query(PlaytimeEntry).count()

        PlaytimeEntriesSummationScript(db.session).run()

        postsummation_entries = db.session.query(PlaytimeEntry).count()

        # The old "processed_entry" has been deleted, reducing the entry count
        # by 1. Counts for the associated id2, c(1) and l(1) are also reduced by 1.
        assert presummation_entries == 15
        assert postsummation_entries == 14

        # Ensure that the playtime entries have the original identifier 2 urn.
        i2_entries = (
            db.session.query(PlaytimeEntry)
            .where(PlaytimeEntry.identifier_id == identifier2.id)
            .all()
        )
        assert len(i2_entries) == 10
        assert all(e.identifier_str == id2_old_urn for e in i2_entries)

        # Ensure that the playtime entries have the original collection 2 name.
        c2_entries = (
            db.session.query(PlaytimeEntry)
            .where(PlaytimeEntry.collection_id == collection2.id)
            .all()
        )
        assert len(c2_entries) == 5
        assert all(e.collection_name == c2_old_name for e in c2_entries)

        # Ensure that the playtime entries have the original library 2 name.
        l2_entries = (
            db.session.query(PlaytimeEntry)
            .where(PlaytimeEntry.library_id == library2.id)
            .all()
        )
        assert len(l2_entries) == 3
        assert all(e.library_name == l2_old_name for e in l2_entries)

        summaries = (
            db.session.query(PlaytimeSummary)
            .order_by(
                PlaytimeSummary.identifier_id,
                PlaytimeSummary.collection_id,
                PlaytimeSummary.library_id,
                PlaytimeSummary.timestamp,
            )
            .all()
        )

        assert len(summaries) == 6

        id1time, id2time1, id2time2, id2col2time, id2col2time1, id2c2l2time = summaries

        assert id1time.identifier == identifier
        assert id1time.total_seconds_played == 120
        assert id1time.collection == collection
        assert id1time.library == library
        assert id1time.identifier_str == identifier.urn
        assert id1time.collection_name == collection.name
        assert id1time.library_name == library.name
        assert id1time.loan_identifier == loan_identifier
        assert id1time.timestamp == dk()

        assert id2time1.identifier == identifier2
        assert id2time1.total_seconds_played == 90
        assert id2time1.collection == collection
        assert id2time1.library == library
        assert id2time1.identifier_str == id2_new_urn
        assert id2time1.collection_name == collection.name
        assert id2time1.library_name == library.name
        assert id2time1.loan_identifier == loan_identifier2

        assert id2time1.timestamp == dk()

        assert id2time2.identifier == identifier2
        assert id2time2.collection == collection
        assert id2time2.library == library
        assert id2time2.identifier_str == id2_new_urn
        assert id2time2.collection_name == collection.name
        assert id2time2.library_name == library.name
        assert id2time2.loan_identifier == loan_identifier2
        assert id2time2.total_seconds_played == 30
        assert id2time2.timestamp == dk(m=1)

        assert id2col2time.identifier == identifier2
        assert id2col2time.collection == collection2
        assert id2col2time.library == library
        assert id2col2time.identifier_str == id2_new_urn
        assert id2col2time.collection_name == c2_new_name
        assert id2col2time.library_name == library.name
        assert id2col2time.loan_identifier == loan_identifier3
        assert id2col2time.total_seconds_played == 30
        assert id2col2time.timestamp == dk()

        assert id2col2time1.identifier == identifier2
        assert id2col2time1.collection == collection2
        assert id2col2time1.library == library
        assert id2col2time1.identifier_str == id2_new_urn
        assert id2col2time1.collection_name == c2_new_name
        assert id2col2time1.library_name == library.name
        assert id2col2time1.loan_identifier == loan_identifier3
        assert id2col2time1.total_seconds_played == 40
        assert id2col2time1.timestamp == dk(m=1)

        assert id2c2l2time.identifier == identifier2
        assert id2c2l2time.collection == collection2
        assert id2c2l2time.library == library2
        assert id2c2l2time.identifier_str == id2_new_urn
        assert id2c2l2time.collection_name == c2_new_name
        assert id2c2l2time.library_name == l2_new_name
        assert id2c2l2time.loan_identifier == loan_identifier4
        assert id2c2l2time.total_seconds_played == 100
        assert id2c2l2time.timestamp == dk()

    def test_reap_processed_entries(self, db: DatabaseTransactionFixture):
        P = PlaytimeTimeEntry
        dk = date2k
        identifier = db.identifier()
        collection = db.default_collection()
        library = db.default_library()
        loan_identifier = "loan-id"
        entries = create_playtime_entries(
            db,
            identifier,
            collection,
            library,
            loan_identifier,
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

    def test_deleted_related_rows(self, db: DatabaseTransactionFixture):
        def related_rows(table, present: Literal["all"] | Literal["none"]):
            """Query for the presence of related identifier, collection, and library rows."""
            condition = (
                and_(
                    table.identifier_id != null(),
                    table.collection_id != null(),
                    table.library_id != null(),
                )
                if present == "all"
                else and_(
                    table.identifier_id == null(),
                    table.collection_id == null(),
                    table.library_id == null(),
                )
            )
            return db.session.query(table).where(condition)

        summaries_query = db.session.query(PlaytimeSummary).order_by(
            PlaytimeSummary.identifier_str,
            PlaytimeSummary.collection_name,
            PlaytimeSummary.library_name,
        )

        # Set up our identifiers, collections, and libraries.
        id1_value = "080442957X"
        id2_value = "9788175257665"

        c1_name = "Collection 1"
        c2_name = "Collection 2"

        l1_name = "Library 1"
        l2_name = "Library 2"

        id1 = db.identifier(identifier_type=Identifier.ISBN, foreign_id=id1_value)
        id2 = db.identifier(identifier_type=Identifier.ISBN, foreign_id=id2_value)
        id1_urn = id1.urn
        id2_urn = id2.urn

        c1 = db.collection(name=c1_name)
        c2 = db.collection(name=c2_name)
        l1 = db.library(name=l1_name)
        l2 = db.library(name=l2_name)
        loan1_id = "loan1"
        loan2_id = "loan2"

        P = PlaytimeTimeEntry
        dk = date2k

        # Create some client playtime entries.
        book1_round1 = create_playtime_entries(
            db,
            id1,
            c1,
            l1,
            loan1_id,
            P(id="0", during_minute=dk(m=0), seconds_played=30),
            P(id="1", during_minute=dk(m=0), seconds_played=30),
        )
        book2_round1 = create_playtime_entries(
            db,
            id2,
            c2,
            l2,
            loan2_id,
            P(id="2", during_minute=dk(m=0), seconds_played=12),
            P(id="3", during_minute=dk(m=0), seconds_played=17),
        )

        # We should have four entries, all with keys to their associated records.
        assert db.session.query(PlaytimeEntry).count() == 4
        assert related_rows(PlaytimeEntry, present="all").count() == 4
        assert related_rows(PlaytimeEntry, present="none").count() == 0

        # We should have no summary records, at this point.
        assert db.session.query(PlaytimeSummary).count() == 0

        # Summarize those records.
        PlaytimeEntriesSummationScript(db.session).run()

        # Now we should have two summary records.
        assert db.session.query(PlaytimeSummary).count() == 2
        # And they should have associated identifier, collection, and library records.
        assert related_rows(PlaytimeSummary, present="all").count() == 2

        # And those should be the correct identifier, collection, and library records.
        b1sum1, b2sum1 = summaries_query.all()

        assert b1sum1.total_seconds_played == 60
        assert b1sum1.identifier_str == id1_urn
        assert b1sum1.collection_name == c1_name
        assert b1sum1.library_name == l1_name
        assert b1sum1.identifier_id == id1.id
        assert b1sum1.collection_id == c1.id
        assert b1sum1.library_id == l1.id
        assert b1sum1.loan_identifier == loan1_id

        assert b2sum1.total_seconds_played == 29
        assert b2sum1.identifier_str == id2_urn
        assert b2sum1.collection_name == c2_name
        assert b2sum1.library_name == l2_name
        assert b2sum1.identifier_id == id2.id
        assert b2sum1.collection_id == c2.id
        assert b2sum1.library_id == l2.id
        assert b2sum1.loan_identifier == loan2_id

        # Add some new client playtime entries.
        book1_round2 = create_playtime_entries(
            db,
            id1,
            c1,
            l1,
            loan1_id,
            P(id="4", during_minute=dk(m=0), seconds_played=30),
            P(id="5", during_minute=dk(m=0), seconds_played=30),
        )
        book2_round2 = create_playtime_entries(
            db,
            id2,
            c2,
            l2,
            loan2_id,
            P(id="6", during_minute=dk(m=0), seconds_played=22),
            P(id="7", during_minute=dk(m=0), seconds_played=46),
        )

        # Now we should have more entries.
        assert db.session.query(PlaytimeEntry).count() == 8
        assert related_rows(PlaytimeEntry, present="all").count() == 8
        assert related_rows(PlaytimeEntry, present="none").count() == 0

        # Remove our identifiers, collections, and libraries.
        for obj in (id1, id2, c1, c2, l1, l2):
            db.session.delete(obj)
        db.session.flush()

        # Verify that the entry records still exist, but that none have related values.
        assert db.session.query(PlaytimeEntry).count() == 8
        assert related_rows(PlaytimeEntry, present="all").count() == 0
        assert related_rows(PlaytimeEntry, present="none").count() == 8

        # Verify that the existing summary records have not been deleted.
        assert db.session.query(PlaytimeSummary).count() == 2
        # None of them should have all associated records.
        assert related_rows(PlaytimeSummary, present="all").count() == 0
        # And all of them should have none of their associated records.
        assert related_rows(PlaytimeSummary, present="none").count() == 2

        # Run the summarization script again.
        PlaytimeEntriesSummationScript(db.session).run()

        # We should have the same summary records, none of which have links.
        assert db.session.query(PlaytimeSummary).count() == 2
        assert related_rows(PlaytimeSummary, present="all").count() == 0
        assert related_rows(PlaytimeSummary, present="none").count() == 2

        # Verify the times and other details.
        b1sum1, b2sum1 = summaries_query.all()

        assert b1sum1.total_seconds_played == 120
        assert b1sum1.identifier_str == id1_urn
        assert b1sum1.collection_name == c1_name
        assert b1sum1.library_name == l1_name
        assert b1sum1.identifier_id is None
        assert b1sum1.collection_id is None
        assert b1sum1.library_id is None
        assert b1sum1.loan_identifier == loan1_id

        assert b2sum1.total_seconds_played == 97
        assert b2sum1.identifier_str == id2_urn
        assert b2sum1.collection_name == c2_name
        assert b2sum1.library_name == l2_name
        assert b2sum1.identifier_id is None
        assert b2sum1.collection_id is None
        assert b2sum1.library_id is None
        assert b2sum1.loan_identifier == loan2_id


def date1m(days) -> date:
    """Helper to get a `date` value for 1 month ago, adjusted by the given number of days."""
    return previous_months(number_of_months=1)[0] + timedelta(days=days)


def dt1m(days) -> datetime:
    """Helper to get a `datetime` value for 1 month ago, adjusted by the given number of days."""
    return datetime.combine(date1m(days), datetime.min.time(), tzinfo=pytz.UTC)


def playtime(
    session,
    identifier: Identifier,
    collection: Collection,
    library: Library,
    timestamp: datetime,
    total_seconds: int,
    loan_identifier: str,
):
    return PlaytimeSummary.add(
        session,
        ts=timestamp,
        seconds=total_seconds,
        identifier=identifier,
        collection=collection,
        library=library,
        identifier_str=identifier.urn,
        collection_name=collection.name,
        library_name=library.name,
        loan_identifier=loan_identifier,
    )


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

        identifier3 = db.identifier()

        loan_identifiers = [f"loan_id:{x}" for x in range(1, 7)]
        (
            loan_identifier,
            loan_identifier2,
            loan_identifier3,
            loan_identifier4,
            loan_identifier5,
            loan_identifier6,
        ) = loan_identifiers

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
        no_isbn = None

        # We're using the RecursiveEquivalencyCache, so must refresh it.
        EquivalentIdentifiersCoverageProvider(db.session).run()

        # one month + 3 days ago : in scope
        playtime(
            db.session, identifier, collection, library, dt1m(3), 1, loan_identifier
        )
        # one month + 4 days ago : in scope  should be added
        playtime(
            db.session, identifier, collection, library, dt1m(4), 2, loan_identifier
        )
        # out of range: after end of default reporting period
        playtime(
            db.session, identifier, collection, library, dt1m(31), 6, loan_identifier
        )
        playtime(
            db.session, identifier, collection, library, dt1m(-31), 60, loan_identifier
        )  # out of range: prior to the beginning of the default reporting period
        playtime(
            db.session,
            identifier,
            collection,
            library,
            dt1m(95),
            60,
            loan_identifier,
        )  # out of range: future
        playtime(
            db.session, identifier2, collection, library, dt1m(3), 5, loan_identifier2
        )
        playtime(
            db.session, identifier2, collection, library, dt1m(4), 6, loan_identifier2
        )

        # Collection2
        playtime(
            db.session, identifier, collection2, library, dt1m(3), 100, loan_identifier3
        )
        # library2
        playtime(
            db.session, identifier, collection, library2, dt1m(3), 200, loan_identifier4
        )
        # collection2 library2
        playtime(
            db.session,
            identifier,
            collection2,
            library2,
            dt1m(3),
            300,
            loan_identifier5,
        )

        playtime(
            db.session,
            identifier3,
            collection2,
            library2,
            dt1m(10),
            800,
            loan_identifier6,
        )

        # log a summary where a title that was previously unavailable is now available for the same loan
        edition2 = db.edition(title="A test")
        edition2.primary_identifier = identifier3

        playtime(
            db.session,
            identifier3,
            collection2,
            library2,
            dt1m(15),
            13,
            loan_identifier6,
        )

        edition2.title = "Z test"

        playtime(
            db.session,
            identifier3,
            collection2,
            library2,
            dt1m(20),
            4,
            loan_identifier6,
        )

        reporting_name = "test cm"
        with (
            patch("palace.manager.scripts.playtime_entries.csv.writer") as writer,
            patch(
                "palace.manager.scripts.playtime_entries.os.environ",
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
            writer().writerow.call_count == 9
        )  # 1 header, 5 identifier,collection,library entries

        cutoff = date1m(0).replace(day=1)
        until = utc_now().date().replace(day=1)
        column1 = f"{cutoff} - {until}"
        call_args = writer().writerow.call_args_list
        assert call_args == [
            call(
                (  # Header
                    "date",
                    "urn",
                    "isbn",
                    "collection",
                    "library",
                    "title",
                    "total seconds",
                    "loan count",
                )
            ),
            call(
                (
                    column1,
                    identifier.urn,
                    strongest_isbn,
                    collection2.name,
                    library2.name,
                    None,
                    300,
                    1,
                )
            ),
            call(
                (
                    column1,
                    identifier3.urn,
                    None,
                    collection2.name,
                    library2.name,
                    None,
                    800,
                    0,
                )
            ),
            call(
                (
                    column1,
                    identifier3.urn,
                    None,
                    collection2.name,
                    library2.name,
                    "A test",
                    13,
                    0,
                )
            ),
            call(
                (
                    column1,
                    identifier3.urn,
                    None,
                    collection2.name,
                    library2.name,
                    "Z test",
                    4,
                    1,
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
                    1,
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
                    1,
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
                    3,
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
                    1,
                )
            ),  # Identifier with edition
        ]

        # verify the number of unique loans
        assert len(loan_identifiers) == sum([x.args[0][7] for x in call_args[1:]])
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
        loan_id = "loan-id"
        _ = playtime(
            db.session,
            identifier,
            collection,
            library,
            dt1m(20),
            1,
            loan_id,
        )

        with patch("palace.manager.scripts.playtime_entries.os.environ", new={}):
            script = PlaytimeEntriesEmailReportsScript(db.session)
            script._log = MagicMock()
            script.run()

            assert script._log.error.call_count == 1
            assert script._log.warning.call_count == 1
            assert "date,urn,isbn,collection," in script._log.warning.call_args[0][0]

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
