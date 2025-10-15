from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from io import IOBase, StringIO
from typing import Any, Literal
from unittest.mock import DEFAULT, create_autospec

import pytest
import pytz
from sqlalchemy.sql.expression import and_, null

from palace.manager.api.circulation.settings import BaseCirculationApiSettings
from palace.manager.api.model.time_tracking import PlaytimeTimeEntry
from palace.manager.celery.tasks.playtime_entries import (
    REPORT_DATE_FORMAT,
    _fetch_distinct_eligible_data_source_names,
    generate_playtime_report,
    sum_playtime_entries,
)
from palace.manager.core.config import Configuration
from palace.manager.core.equivalents_coverage import (
    EquivalentIdentifiersCoverageProvider,
)
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.service.google_drive.google_drive import GoogleDriveService
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Equivalency, Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.time_tracking import PlaytimeEntry, PlaytimeSummary
from palace.manager.util.datetime_helpers import previous_months, utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


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
            data_source_name=collection.data_source.name,
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


class TestSumPlaytimeEntriesTask:
    def test_summation(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ):
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

        sum_playtime_entries.delay().wait()

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

    def test_reap_processed_entries(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ):
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

        sum_playtime_entries.delay().wait()

        # Nothing reaped yet
        assert db.session.query(PlaytimeEntry).count() == 6
        # Last entry is not processed
        assert [e.processed for e in entries] == [True, True, True, True, True, False]

        # Second run
        sum_playtime_entries.delay().wait()
        # Only 2 should be left
        assert db.session.query(PlaytimeEntry).count() == 2
        assert list(
            db.session.query(PlaytimeEntry)
            .order_by(PlaytimeEntry.id)
            .with_entities(PlaytimeEntry.tracking_id)
        ) == [("4",), ("5",)]

    def test_deleted_related_rows(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ):
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

        ds_name_1 = "datasource 1"
        ds_name_2 = "datasource 2"

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
        sum_playtime_entries.delay().wait()

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
        sum_playtime_entries.delay().wait()

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
        data_source_name=collection.data_source.name,
    )


class TestGeneratePlaytimeReport:
    def test_generate_playtime_reports(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
        monkeypatch: pytest.MonkeyPatch,
    ):
        identifier = db.identifier()
        collection = db.collection(
            "collection b",
            protocol=OPDSForDistributorsAPI,
            settings=db.opds_for_distributors_settings(data_source="ds_b"),
        )

        library = db.default_library()
        edition = db.edition(data_source_name=collection.data_source.name)
        identifier2 = edition.primary_identifier
        collection2 = db.collection(
            "collection a",
            protocol=OPDS2API,
            settings=db.opds2_settings(data_source="ds_a"),
        )

        # a data source with no playtime data - we expect an empty report for ds_c
        collection3 = db.collection(
            "collection c",
            protocol=OPDS2API,
            settings=db.opds2_settings(data_source="ds_c"),
        )

        # A collection and datasource that will be removed after the playtime is recorded, but before the report
        # is generated.
        collection4 = db.collection(
            "collection d",
            protocol=OPDS2API,
            settings=db.opds2_settings(data_source="ds_d"),
        )

        library2 = db.library()

        identifier3 = db.identifier()

        loan_identifiers = [f"loan_id:{x}" for x in range(1, 8)]
        (
            loan_identifier,
            loan_identifier2,
            loan_identifier3,
            loan_identifier4,
            loan_identifier5,
            loan_identifier6,
            loan_identifier7,
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
        no_isbn = ""

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

        # # collection 4:  in scope
        playtime(
            db.session, identifier, collection4, library, dt1m(3), 1, loan_identifier7
        )
        #
        # # now delete the collection and data source for collection 4
        ds4 = collection4.data_source
        db.session.delete(collection4)
        db.session.delete(ds4)

        reporting_name = "test cm"
        parent_folder_id = "palace-test"

        output_data: dict[str, str] = {}

        def mock_create_file(
            file_name: str,
            stream: IOBase,
            content_type: str,
            parent_folder_id: str | None = None,
        ) -> dict[str, Any]:
            nonlocal output_data
            stream.seek(0)
            output_data[file_name] = stream.read().decode("utf-8")
            return DEFAULT

        mock_google_drive_service = create_autospec(GoogleDriveService)
        mock_google_drive_service.create_file.side_effect = mock_create_file
        drive_container = services_fixture.services.google_drive()
        drive_container.config.from_dict({"parent_folder_id": parent_folder_id})
        drive_container.service.override(mock_google_drive_service)
        monkeypatch.setenv(
            Configuration.REPORTING_NAME_ENVIRONMENT_VARIABLE, reporting_name
        )

        # Act
        generate_playtime_report.delay().wait()

        # Assert
        assert len(output_data) == 4
        [
            (ds_a_filename, ds_a_data),
            (ds_b_filename, ds_b_data),
            (ds_c_filename, ds_c_data),
            (ds_d_filename, ds_d_data),
        ] = [(k, list(csv.reader(StringIO(v)))) for k, v in output_data.items()]

        assert len(ds_a_data) == 6
        assert len(ds_b_data) == 4
        assert len(ds_c_data) == 1
        assert len(ds_d_data) == 2

        cutoff = date1m(0).replace(day=1)
        until = utc_now().date().replace(day=1)
        column1 = f"{cutoff.strftime(REPORT_DATE_FORMAT)} - {until.strftime(REPORT_DATE_FORMAT)}"
        headers = [
            "date",
            "urn",
            "isbn",
            "collection",
            "library",
            "title",
            "total seconds",
            "loan count",
        ]

        assert (
            f"{cutoff.strftime(REPORT_DATE_FORMAT)}-{until.strftime(REPORT_DATE_FORMAT)}-playtime-summary-test_cm-ds_a-"
            in ds_a_filename
        )

        assert ds_a_data == [
            headers,
            [
                column1,
                identifier.urn,
                strongest_isbn,
                collection2.name,
                library2.name,
                "",
                "300",
                "1",
            ],
            [
                column1,
                identifier3.urn,
                "",
                collection2.name,
                library2.name,
                "",
                "800",
                "0",
            ],
            [
                column1,
                identifier3.urn,
                "",
                collection2.name,
                library2.name,
                "A test",
                "13",
                "0",
            ],
            [
                column1,
                identifier3.urn,
                "",
                collection2.name,
                library2.name,
                "Z test",
                "4",
                "1",
            ],
            [
                column1,
                identifier.urn,
                strongest_isbn,
                collection2.name,
                library.name,
                "",
                "100",
                "1",
            ],
        ]

        assert (
            f"{cutoff.strftime(REPORT_DATE_FORMAT)}-{until.strftime(REPORT_DATE_FORMAT)}-playtime-summary-test_cm-ds_b-"
            in ds_b_filename
        )

        assert ds_b_data == [
            headers,
            [
                column1,
                identifier.urn,
                strongest_isbn,
                collection.name,
                library2.name,
                "",
                "200",
                "1",
            ],
            [
                column1,
                identifier.urn,
                strongest_isbn,
                collection.name,
                library.name,
                "",
                "3",
                "1",
            ],  # Identifier without edition
            [
                column1,
                identifier2.urn,
                no_isbn,
                collection.name,
                library.name,
                edition.title,
                "11",
                "1",
            ],  # Identifier with edition
        ]

        assert (
            f"{cutoff.strftime(REPORT_DATE_FORMAT)}-{until.strftime(REPORT_DATE_FORMAT)}-playtime-summary-test_cm-ds_c-"
            in ds_c_filename
        )

        assert ds_c_data == [
            headers,
        ]

        assert (
            f"{cutoff.strftime(REPORT_DATE_FORMAT)}-{until.strftime(REPORT_DATE_FORMAT)}-playtime-summary-test_cm-ds_d-"
            in ds_d_filename
        )

        assert ds_d_data == [
            headers,
            [
                column1,
                identifier.urn,
                strongest_isbn,
                "collection d",
                library.name,
                "",
                "1",
                "1",
            ],
        ]

        assert mock_google_drive_service.create_file.call_count == 4

        nested_method = mock_google_drive_service.create_nested_folders_if_not_exist
        assert nested_method.call_count == 4
        assert nested_method.call_args_list[0].kwargs == {
            "parent_folder_id": parent_folder_id,
            "folders": [
                "ds_a",
                "Usage Reports",
                "test cm",
                "2025",
            ],
        }
        assert nested_method.call_args_list[1].kwargs == {
            "parent_folder_id": parent_folder_id,
            "folders": [
                "ds_b",
                "Usage Reports",
                "test cm",
                "2025",
            ],
        }

        assert nested_method.call_args_list[2].kwargs == {
            "parent_folder_id": parent_folder_id,
            "folders": [
                "ds_c",
                "Usage Reports",
                "test cm",
                "2025",
            ],
        }

        assert nested_method.call_args_list[3].kwargs == {
            "parent_folder_id": parent_folder_id,
            "folders": [
                "ds_d",
                "Usage Reports",
                "test cm",
                "2025",
            ],
        }

    @pytest.mark.parametrize(
        "eligible_collections,playtime_summaries,expected_ds_names",
        (
            pytest.param(
                [],
                [],
                [],
                id="no-collections-no-summaries",
            ),
            pytest.param(
                [(OPDS2API, "ds_opds2")],
                [],
                ["ds_opds2"],
                id="eligible-collection-only-opds2",
            ),
            pytest.param(
                [(OPDSForDistributorsAPI, "ds_ofd")],
                [],
                ["ds_ofd"],
                id="eligible-collection-only-opds-for-distributors",
            ),
            pytest.param(
                [
                    (OPDS2API, "ds_opds2"),
                    (OPDSForDistributorsAPI, "ds_ofd"),
                ],
                [],
                ["ds_ofd", "ds_opds2"],
                id="multiple-eligible-collections",
            ),
            pytest.param(
                [
                    (BibliothecaAPI, "ds_bibliotheca"),
                    (OPDS2WithODLApi, "ds_opds2odl"),
                ],
                [],
                [],
                id="ineligible-collections-only",
            ),
            pytest.param(
                [],
                ["ds_from_summary"],
                ["ds_from_summary"],
                id="playtime-summary-only",
            ),
            pytest.param(
                [(OPDS2API, "ds_shared")],
                ["ds_shared"],
                ["ds_shared"],
                id="collection-and-summary-same-ds",
            ),
            pytest.param(
                [(OPDS2API, "ds_collection")],
                ["ds_summary"],
                ["ds_collection", "ds_summary"],
                id="collection-and-summary-different-ds",
            ),
            pytest.param(
                [],
                ["ds_dup", "ds_dup", "ds_dup"],
                ["ds_dup"],
                id="multiple-summaries-same-ds",
            ),
            pytest.param(
                [
                    (OPDS2API, "ds_z"),
                    (OPDSForDistributorsAPI, "ds_a"),
                    (BibliothecaAPI, "ds_ineligible"),
                ],
                ["ds_m", "ds_b"],
                ["ds_a", "ds_b", "ds_m", "ds_z"],
                id="mixed-eligible-ineligible-with-summaries-sorting",
            ),
        ),
    )
    def test_fetch_distinct_eligible_data_source_names(
        self,
        db: DatabaseTransactionFixture,
        eligible_collections: list[tuple[type, str | None]],
        playtime_summaries: list[str],
        expected_ds_names: list[str],
    ):
        """Test fetching distinct eligible data source names from collections and summaries.

        Verifies that:
        - Only collections with eligible protocols are included
        - All PlaytimeSummary data sources are included
        - Results are deduplicated
        - Results are sorted alphabetically
        - Mixed scenarios with both eligible and ineligible collections work correctly
        """
        # Create collections with specified protocols and data sources.
        for protocol, ds_name in eligible_collections:
            settings: BaseCirculationApiSettings
            if protocol == OPDS2API:
                settings = db.opds2_settings(data_source=ds_name)
            elif protocol == OPDS2WithODLApi:
                settings = db.opds2_odl_settings(data_source=ds_name)
            elif protocol == OPDSForDistributorsAPI:
                settings = db.opds_for_distributors_settings(data_source=ds_name)
            elif protocol == BibliothecaAPI:
                settings = db.bibliotheca_settings(data_source=ds_name)
            else:
                raise ValueError(f"Unhandled protocol: {protocol}")

            db.collection(
                name=f"collection_{ds_name or 'none'}",
                protocol=protocol,
                settings=settings,
            )

        if playtime_summaries:
            identifier = db.identifier()
            # We'll delete this collection after creating summaries so that
            # only the summary data sources are counted.
            temp_collection = db.collection(
                protocol=OPDS2API,
                settings=db.opds2_settings(data_source="temp_ds_to_delete"),
            )
            library = db.default_library()

            for idx, ds_name in enumerate(playtime_summaries):
                # Create summaries and assign data sources.
                playtime(
                    db.session,
                    identifier=identifier,
                    collection=temp_collection,
                    library=library,
                    timestamp=dt1m(3),
                    total_seconds=10,
                    loan_identifier=f"loan_{idx}",
                )
                summary = (
                    db.session.query(PlaytimeSummary)
                    .order_by(PlaytimeSummary.id.desc())
                    .first()
                )
                assert summary is not None
                summary.data_source_name = ds_name

            db.session.flush()

            # Delete the temporary collection and its data source so they don't affect the result.
            temp_ds = temp_collection.data_source
            db.session.delete(temp_collection)
            db.session.delete(temp_ds)
            db.session.flush()

        registry = LicenseProvidersRegistry()
        result = _fetch_distinct_eligible_data_source_names(db.session, registry)

        assert result == expected_ds_names

    def test_fetch_distinct_eligible_data_source_names_deleted_collection(
        self,
        db: DatabaseTransactionFixture,
    ):
        """Test that data sources from deleted collections still appear via PlaytimeSummary."""
        collection = db.collection(
            name="collection_to_delete",
            protocol=OPDS2API,
            settings=db.opds2_settings(data_source="ds_deleted"),
        )

        identifier = db.identifier()
        library = db.default_library()
        playtime(
            db.session,
            identifier=identifier,
            collection=collection,
            library=library,
            timestamp=dt1m(3),
            total_seconds=100,
            loan_identifier="loan_1",
        )

        # Verify that the data source is returned before the collection is deleted.
        registry = LicenseProvidersRegistry()
        result = _fetch_distinct_eligible_data_source_names(db.session, registry)
        assert result == ["ds_deleted"]

        # Delete the collection and its data source.
        data_source = collection.data_source
        db.session.delete(collection)
        db.session.delete(data_source)
        db.session.flush()

        # Verify that the data source is returned after the collection is deleted.
        result = _fetch_distinct_eligible_data_source_names(db.session, registry)
        assert result == ["ds_deleted"]
