import csv
import io
import os
import zipfile
from collections.abc import Callable
from datetime import date, timedelta
from typing import IO, BinaryIO
from unittest.mock import MagicMock, create_autospec

import pytest
from pytest import LogCaptureFixture
from sqlalchemy import select
from sqlalchemy.sql import Select

from palace.manager.celery.tasks.generate_inventory_and_hold_reports import (
    generate_csv_report,
    generate_inventory_and_hold_reports,
    generate_report,
    holds_with_no_licenses_report_query,
    inventory_report_query,
    library_report_integrations,
    palace_inventory_activity_report_query,
)
from palace.manager.integration.license.opds.opds1.settings import OPDSImporterSettings
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.opds.odl.info import LicenseStatus
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.classification import Genre, Subject
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePoolStatus
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.sqlalchemy.util import (
    get_one_or_create,
    numericrange_to_string,
    tuple_to_numericrange,
)
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


def test_generate_csv_report(
    db: DatabaseTransactionFixture,
    caplog: LogCaptureFixture,
):
    """Make sure the CSV generation works the way we expect."""
    # Set log level to DEBUG to capture elapsed time logs
    caplog.set_level(LogLevel.debug)

    library = db.library(short_name="test_library")
    query = select(Library.id, Library.short_name)

    csv_file = io.StringIO()
    csv_file.name = "test_report.csv"

    # Call generate_csv_report
    generate_csv_report(
        db=db.session,
        csv_file=csv_file,
        sql_params={},
        query=query,
    )

    # Verify the CSV was actually written
    csv_file.seek(0)
    csv_content = csv_file.read()
    assert "id,short_name" in csv_content
    assert "test_library" in csv_content

    # Verify that the elapsed time log message was written
    assert (
        "generate_csv_report - test_report.csv: Completed. (elapsed time:"
        in caplog.text
    )
    assert "report written to test_report.csv" in caplog.text


def test_only_active_collections_are_included(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
):
    library = db.default_library()
    collection1 = db.default_collection()
    collection2 = db.default_inactive_collection()

    # The library has two collections, one of which is inactive.
    assert set(library.associated_collections) == {collection1, collection2}
    assert library.active_collections == [collection1]
    assert collection1.is_active is True
    assert collection2.is_active is False

    # Only OPDS integrations are eligible for inventory and holds reports,
    # so we verify that our collections meet that criteria.
    assert collection1.protocol.lower().startswith(("opds", "odl"))
    assert collection2.protocol.lower().startswith(("opds", "odl"))

    eligible_integrations = library_report_integrations(
        library,
        db.session,
        services_fixture.services.integration_registry.license_providers(),
    )

    assert len(eligible_integrations) == 1
    assert eligible_integrations == [collection1.integration_configuration]


@pytest.mark.parametrize(
    "query_function, expected_column_names",
    (
        (
            inventory_report_query,
            (
                "item_status",
                "license_status",
                "title",
                "author",
                "identifier",
                "isbn",
                "language",
                "publisher",
                "published_date",
                "format",
                "audience",
                "genres",
                "age_ranges",
                "bisac_subjects",
                "visible",
                "visibility_status",
                "data_source",
                "collection_name",
                "license_expiration",
                "days_remaining_on_license",
                "initial_loans",
                "remaining_loans",
                "allowed_concurrent_users",
            ),
        ),
        (
            palace_inventory_activity_report_query,
            (
                "title",
                "author",
                "identifier",
                "isbn",
                "language",
                "publisher",
                "format",
                "audience",
                "genres",
                "data_source",
                "collection_name",
                "total_library_allowed_concurrent_users",
                "library_active_loan_count",
                "shared_active_loan_count",
                "library_active_hold_count",
                "shared_active_hold_count",
                "library_hold_ratio",
            ),
        ),
        (
            holds_with_no_licenses_report_query,
            (
                "title",
                "author",
                "identifier",
                "isbn",
                "language",
                "publisher",
                "format",
                "audience",
                "genres",
                "data_source",
                "collection_name",
                "library_active_hold_count",
                "shared_active_hold_count",
            ),
        ),
    ),
)
def test_report_columns(
    query_function: Callable[[], Select],
    expected_column_names: tuple[str, ...],
):
    """Verify column order and count for each of the query functions."""
    actual_inventory_columns = tuple(c.name for c in query_function().selected_columns)
    assert actual_inventory_columns == expected_column_names


def test_generate_report(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
    caplog: LogCaptureFixture,
):
    email = "test@email.com"

    # A non-existent collection should log an error
    caplog.set_level(LogLevel.info)
    send_email_mock = create_autospec(
        services_fixture.services.email.container.send_email
    )

    mock_s3 = MagicMock()

    generate_report(
        db.session,
        library_id=1,
        email_address=email,
        send_email=send_email_mock,
        registry=services_fixture.services.integration_registry.license_providers(),
        s3_service=mock_s3,
    )
    assert (
        f"Cannot generate inventory and holds report for library (id=1): library not found."
        in caplog.text
    )

    # create some test data that we expect to be picked up in the inventory report
    library = db.library(short_name="test_library")
    data_source = "BiblioBoard"
    collection_name = "BiblioBoard Test Collection"
    collection = create_test_opds_collection(collection_name, data_source, db, library)
    library2 = db.library(short_name="test_library2")
    # add another library
    collection.associated_libraries.append(library2)

    # Configure test data we expect will not be picked up.
    create_test_opds_collection(
        "Another Test Collection", "AnotherOpdsDataSource", db, library, False
    )

    od_collection_not_to_include = db.collection(
        protocol=OverdriveAPI,
        name="Overdrive Test Collection",
    )

    od_collection_not_to_include.associated_libraries = [library]

    ds = collection.data_source
    assert ds is not None

    # Add a book for testing.
    title = "展翅高飞 : Whistling Wings"
    author = "Laura Goering"
    language = "eng"
    publisher = "My Publisher"
    published_date = date(2019, 5, 17)
    checkouts_left = 10
    terms_concurrency = 5
    edition = db.edition(data_source_name=ds.name)
    edition.language = language
    edition.publisher = publisher
    edition.published = published_date
    edition.title = title
    edition.medium = edition.BOOK_MEDIUM
    edition.author = author

    # Grab identifier and give it an ISBN equivalent.
    identifier = edition.primary_identifier
    identifier_value = identifier.identifier
    isbn_identifier = db.identifier(identifier_type=Identifier.ISBN)
    isbn = isbn_identifier.identifier
    identifier.equivalent_to(edition.data_source, isbn_identifier, strength=1)
    assert identifier.type != Identifier.ISBN
    assert len(isbn) > 0

    work = db.work(
        language="eng",
        fiction=True,
        with_license_pool=False,
        data_source_name=ds.name,
        presentation_edition=edition,
        collection=collection,
        genre="genre_z",
    )

    genre, ignore = Genre.lookup(db.session, "genre_a", autocreate=True)
    work.genres.append(genre)
    work.audience = "young adult"
    work.target_age = tuple_to_numericrange((12, 14))

    bisac_subject_one = db.subject(Subject.BISAC, "BISAC_ONE")
    bisac_subject_one.name = "BISAC Subject One"
    bisac_subject_one.target_age = tuple_to_numericrange((7, 9))
    bisac_subject_two = db.subject(Subject.BISAC, "BISAC_TWO")
    bisac_subject_two.name = "BISAC Subject Two"
    bisac_subject_two.target_age = tuple_to_numericrange((10, 12))
    non_bisac_subject = db.subject(Subject.LCSH, "NON_BISAC")
    non_bisac_subject.name = "Non-BISAC Subject"
    non_bisac_subject.target_age = tuple_to_numericrange((13, 15))

    db.classification(identifier, bisac_subject_one, ds)
    db.classification(identifier, bisac_subject_two, ds)
    db.classification(identifier, non_bisac_subject, ds)

    expected_age_range_one = numericrange_to_string(bisac_subject_one.target_age)
    expected_age_range_two = numericrange_to_string(bisac_subject_two.target_age)
    expected_non_bisac_age_range = numericrange_to_string(non_bisac_subject.target_age)

    licensepool = db.licensepool(
        edition=edition,
        open_access=False,
        data_source_name=ds.name,
        set_edition_as_presentation=True,
        collection=collection,
        work=work,
    )

    # Add a second book with no copies
    title2 = "Test Book 2"
    author2 = "Tom Pen"
    published_date2 = date(2020, 10, 5)
    edition2 = db.edition(data_source_name=ds.name)
    edition2.language = language
    edition2.publisher = publisher
    edition2.published = published_date2
    edition2.title = title2
    edition2.medium = edition.BOOK_MEDIUM
    edition2.author = author2

    # Grab identifier and give it an ISBN equivalent.
    identifier2 = edition2.primary_identifier
    identifier2_value = identifier2.identifier
    isbn_identifier2 = db.identifier(identifier_type=Identifier.ISBN)
    isbn2 = isbn_identifier2.identifier
    identifier2.equivalent_to(edition2.data_source, isbn_identifier2, strength=1)
    assert identifier2.type != Identifier.ISBN

    work2 = db.work(
        language="eng",
        fiction=True,
        with_license_pool=False,
        data_source_name=ds.name,
        presentation_edition=edition2,
        collection=collection,
        genre="genre_z",
    )
    work2.target_age = tuple_to_numericrange((6, 8))

    licensepool_no_licenses_owned = db.licensepool(
        edition=edition2,
        open_access=False,
        data_source_name=ds.name,
        set_edition_as_presentation=True,
        collection=collection,
        work=work2,
    )

    licensepool_no_licenses_owned.licenses_owned = 0

    days_remaining = 10
    expiration = utc_now() + timedelta(days=days_remaining)
    initial_loans = 20
    db.license(
        pool=licensepool,
        status=LicenseStatus.available,
        checkouts_left=checkouts_left,
        terms_concurrency=terms_concurrency,
        expires=expiration,
        terms_checkouts=initial_loans,
    )
    db.license(
        pool=licensepool,
        status=LicenseStatus.unavailable,
        checkouts_left=1,
        terms_concurrency=1,
        expires=utc_now(),
        terms_checkouts=5,
    )

    db.license(
        pool=licensepool_no_licenses_owned,
        status=LicenseStatus.available,
        terms_concurrency=1,
        terms_checkouts=None,
    )
    patron1 = db.patron(library=library)
    patron2 = db.patron(library=library)
    patron3 = db.patron(library=library)
    patron4 = db.patron(library=library)
    patron5 = db.patron(library=library)
    # this one should be counted because the end is in the future.
    hold1, _ = get_one_or_create(
        db.session,
        Hold,
        patron=patron1,
        license_pool=licensepool,
        position=1,
        start=utc_now(),
        end=utc_now() + timedelta(days=1),
    )

    # this one should be counted because the end is None
    hold2, _ = get_one_or_create(
        db.session,
        Hold,
        patron=patron2,
        license_pool=licensepool,
        start=utc_now(),
        end=None,
    )

    # this hold should be counted b/c the position is > 0
    hold3, _ = get_one_or_create(
        db.session,
        Hold,
        patron=patron3,
        license_pool=licensepool,
        start=utc_now() - timedelta(days=1),
        end=utc_now() - timedelta(minutes=1),
        position=1,
    )

    # this hold should not be counted because the end is neither in the future nor unset and the position is zero
    hold4, _ = get_one_or_create(
        db.session,
        Hold,
        patron=patron4,
        license_pool=licensepool,
        start=utc_now(),
        end=utc_now() - timedelta(minutes=1),
        position=0,
    )

    # this hold should be counted because the end is in the future.
    hold5, _ = get_one_or_create(
        db.session,
        Hold,
        patron=patron1,
        license_pool=licensepool_no_licenses_owned,
        position=1,
        start=utc_now(),
        end=utc_now() + timedelta(days=1),
    )

    shared_patrons_in_hold_queue = 4
    licensepool.patrons_in_hold_queue = shared_patrons_in_hold_queue

    # Add a third book that doesn't have any holds, so we can verify that it's not in the holds report.
    no_holds_work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
    no_holds_work.target_age = None
    no_holds_identifier_value = (
        no_holds_work.presentation_edition.primary_identifier.identifier
    )

    # The identifier value should be different from the one we used for the hold.
    assert no_holds_identifier_value != identifier_value
    assert library.id

    reports_zip = "test_zip"

    def store_stream_mock(
        key: str,
        stream: BinaryIO,
        content_type: str | None = None,
    ):

        with open(reports_zip, "wb") as file:
            file.write(stream.read())

    mock_s3.store_stream = store_stream_mock

    generate_report(
        db.session,
        library.id,
        email_address=email,
        send_email=send_email_mock,
        registry=services_fixture.services.integration_registry.license_providers(),
        s3_service=mock_s3,
    )

    mock_s3.generate_url.assert_called_once()
    send_email_mock.assert_called_once()
    kwargs = send_email_mock.call_args.kwargs
    assert kwargs["receivers"] == [email]
    assert "Inventory and Holds Reports" in kwargs["subject"]
    assert "This report will be available for download for 30 days." in kwargs["text"]
    try:
        with zipfile.ZipFile(reports_zip, mode="r") as archive:
            entry_list = archive.namelist()
            assert len(entry_list) == 3
            with (
                archive.open(entry_list[0]) as inventory_activity_report_zip_entry,
                archive.open(entry_list[1]) as inventory_report_zip_entry,
                archive.open(entry_list[2]) as holds_with_no_licenses_report_zip_entry,
            ):
                # >> Report: inventory report.
                assert inventory_report_zip_entry
                assert "test_library" in inventory_report_zip_entry.name
                inventory_report_csv = zip_csv_entry_to_dict(inventory_report_zip_entry)

                # The inventory report should have one row per license per pool.
                # Book 1: 2 licenses. Book 2: 1 license. Book 3: just the pool.
                assert len(inventory_report_csv) == 4

                # Find all rows for each book.
                book1_available_row = next(
                    r
                    for r in inventory_report_csv
                    if r["identifier"] == identifier_value
                    and r["license_status"] == str(LicenseStatus.available)
                )
                book1_unavailable_row = next(
                    r
                    for r in inventory_report_csv
                    if r["identifier"] == identifier_value
                    and r["license_status"] == str(LicenseStatus.unavailable)
                )
                book2_row = next(
                    r
                    for r in inventory_report_csv
                    if r["identifier"] == identifier2_value
                )
                book3_no_holds_row = next(
                    r
                    for r in inventory_report_csv
                    if r["identifier"] == no_holds_identifier_value
                )

                # >> Book 1 - Available License Row
                assert book1_available_row["item_status"] == str(
                    LicensePoolStatus.ACTIVE
                )
                assert book1_available_row["license_status"] == str(
                    LicenseStatus.available
                )
                assert book1_available_row["title"] == title
                assert book1_available_row["author"] == author
                assert book1_available_row["identifier"] == identifier_value
                assert book1_available_row["isbn"] == isbn
                assert book1_available_row["language"] == language
                assert book1_available_row["publisher"] == publisher
                assert book1_available_row["published_date"] == "2019-05-17"
                assert book1_available_row["audience"] == "young adult"
                assert book1_available_row["genres"] == "genre_a,genre_z"
                assert "BISAC Subject One" in book1_available_row["bisac_subjects"]
                assert "BISAC Subject Two" in book1_available_row["bisac_subjects"]
                assert "Non-BISAC Subject" not in book1_available_row["bisac_subjects"]
                assert "|" in book1_available_row["bisac_subjects"]
                assert sorted(book1_available_row["bisac_subjects"].split("|")) == [
                    "BISAC Subject One",
                    "BISAC Subject Two",
                ]
                assert expected_age_range_one in book1_available_row["age_ranges"]
                assert expected_age_range_two in book1_available_row["age_ranges"]
                assert "|" in book1_available_row["age_ranges"]
                assert sorted(book1_available_row["age_ranges"].split("|")) == sorted(
                    [expected_age_range_one, expected_age_range_two]
                )
                assert (
                    expected_non_bisac_age_range
                    not in book1_available_row["age_ranges"]
                )
                assert book1_available_row["format"] == edition.BOOK_MEDIUM
                assert book1_available_row["data_source"] == data_source
                assert book1_available_row["collection_name"] == collection_name
                assert float(book1_available_row["days_remaining_on_license"]) == float(
                    days_remaining
                )
                assert book1_available_row["initial_loans"] == str(initial_loans)
                assert book1_available_row["remaining_loans"] == str(checkouts_left)
                assert book1_available_row["allowed_concurrent_users"] == str(
                    terms_concurrency
                )
                assert (
                    expiration.strftime("%Y-%m-%d %H:%M:%S.%f")
                    in book1_available_row["license_expiration"]
                )

                # >> Book 1 - Unavailable License Row
                assert book1_unavailable_row["item_status"] == str(
                    LicensePoolStatus.ACTIVE
                )
                assert book1_unavailable_row["license_status"] == str(
                    LicenseStatus.unavailable
                )
                assert book1_unavailable_row["title"] == title
                assert book1_unavailable_row["author"] == author
                assert book1_unavailable_row["identifier"] == identifier_value
                assert book1_unavailable_row["isbn"] == isbn
                assert book1_unavailable_row["language"] == language
                assert book1_unavailable_row["publisher"] == publisher
                assert book1_unavailable_row["published_date"] == "2019-05-17"
                assert book1_unavailable_row["audience"] == "young adult"
                assert book1_unavailable_row["genres"] == "genre_a,genre_z"
                assert book1_unavailable_row["format"] == edition.BOOK_MEDIUM
                assert book1_unavailable_row["data_source"] == data_source
                assert book1_unavailable_row["collection_name"] == collection_name
                # Unavailable license has checkouts_left=1, terms_concurrency=1, terms_checkouts=5
                assert book1_unavailable_row["initial_loans"] == "5"
                assert book1_unavailable_row["remaining_loans"] == "1"
                assert book1_unavailable_row["allowed_concurrent_users"] == "1"
                # expires=utc_now() so days_remaining should be <= 0
                assert float(book1_unavailable_row["days_remaining_on_license"]) <= 0

                # >> Book 2 - No Licenses Owned (but has 1 available license)
                assert book2_row["item_status"] == str(LicensePoolStatus.ACTIVE)
                assert book2_row["license_status"] == str(LicenseStatus.available)
                assert book2_row["title"] == title2
                assert book2_row["author"] == author2
                assert book2_row["identifier"] == identifier2_value
                assert book2_row["isbn"] == isbn2
                assert book2_row["language"] == language
                assert book2_row["publisher"] == publisher
                assert book2_row["published_date"] == "2020-10-05"
                assert book2_row["audience"] == "Adult"  # Default audience
                assert book2_row["genres"] == "genre_z"
                assert book2_row["format"] == edition.BOOK_MEDIUM
                assert book2_row["data_source"] == data_source
                assert book2_row["collection_name"] == collection_name
                # This license has terms_concurrency=1, no expiration, no checkouts_left specified
                assert book2_row["allowed_concurrent_users"] == "1"
                # No expiration means license_expiration and days_remaining should be empty or None
                assert book2_row["license_expiration"] in ("", "None")
                assert book2_row["days_remaining_on_license"] in ("", "None")
                # terms_checkouts was not set, so initial_loans should be empty/None
                assert book2_row["initial_loans"] in ("", "None")
                assert book2_row["remaining_loans"] in ("", "None")

                # >> Book 3 - No Holds Book.
                # This has a licensepool but no licenses, so license fields are empty.
                assert book3_no_holds_row["item_status"] == str(
                    LicensePoolStatus.ACTIVE
                )
                assert book3_no_holds_row is not None
                assert book3_no_holds_row["identifier"] == no_holds_identifier_value
                assert book3_no_holds_row["data_source"] == data_source
                assert book3_no_holds_row["collection_name"] == collection_name
                # We didn't set a language, so we get the default.
                assert book3_no_holds_row["language"] == "eng"
                # No licenses exist, so license-specific fields should be empty.
                assert book3_no_holds_row["license_status"] == ""
                assert book3_no_holds_row["license_expiration"] == ""
                assert book3_no_holds_row["days_remaining_on_license"] == ""
                assert book3_no_holds_row["published_date"] == ""
                assert book3_no_holds_row["initial_loans"] == ""
                assert book3_no_holds_row["remaining_loans"] == ""
                assert book3_no_holds_row["allowed_concurrent_users"] == ""

                # >> Report: inventory activity report.
                assert inventory_activity_report_zip_entry
                assert "test_library" in inventory_activity_report_zip_entry.name
                inventory_activity_report_csv = zip_csv_entry_to_dict(
                    inventory_activity_report_zip_entry
                )

                # All books (with or without holds), one row per book.
                assert len(inventory_activity_report_csv) == 3
                assert {r["identifier"] for r in inventory_activity_report_csv} == {
                    identifier_value,
                    identifier2_value,
                    no_holds_identifier_value,
                }
                row = next(
                    r
                    for r in inventory_activity_report_csv
                    if r["identifier"] == identifier_value
                )
                no_holds_row = next(
                    (
                        r
                        for r in inventory_activity_report_csv
                        if r["identifier"] == no_holds_identifier_value
                    ),
                    None,
                )
                no_licenses_owned_row = next(
                    (
                        r
                        for r in inventory_activity_report_csv
                        if r["identifier"] == identifier2_value
                    ),
                    None,
                )

                # Ensure that our test book is described properly in the activity report.
                assert row["title"] == title
                assert row["author"] == author
                assert row["identifier"] == identifier_value
                assert row["isbn"] == isbn
                assert row["language"] == language
                assert row["publisher"] == publisher
                assert row["audience"] == "young adult"
                assert row["genres"] == "genre_a,genre_z"
                assert row["format"] == edition.BOOK_MEDIUM
                assert row["data_source"] == data_source
                assert row["collection_name"] == collection_name
                # Activity report specific fields for book with holds
                assert int(row["total_library_allowed_concurrent_users"]) == 1
                assert int(row["library_active_loan_count"]) == 0
                # Collection is shared (library2 was added), licenses_reserved defaults to 0
                assert int(row["shared_active_loan_count"]) == 0
                assert int(row["library_active_hold_count"]) == 3
                assert (
                    int(row["shared_active_hold_count"]) == shared_patrons_in_hold_queue
                )
                # Hold ratio: 3 active holds / 1 available license = 3
                assert float(row["library_hold_ratio"]) == 3.0

                # Even the book with no holds should be included in the activity report.
                assert no_holds_row is not None
                assert no_holds_row["title"] is not None
                assert no_holds_row["identifier"] == no_holds_identifier_value
                # Activity report fields for book with no holds
                assert int(no_holds_row["library_active_hold_count"]) == 0
                assert int(no_holds_row["library_active_loan_count"]) == 0
                # Since there are no holds and licenses_available > 0, ratio should be 0
                assert float(no_holds_row["library_hold_ratio"]) == 0

                # Test the book with no owned licenses.
                assert no_licenses_owned_row is not None
                assert no_licenses_owned_row["title"] == title2
                assert int(no_licenses_owned_row["library_active_hold_count"]) == 1
                assert int(no_licenses_owned_row["library_active_loan_count"]) == 0
                assert (
                    int(no_licenses_owned_row["total_library_allowed_concurrent_users"])
                    == 0
                )
                # When `total_library_allowed_concurrent_users` <= 0, ratio should be -1
                assert int(no_licenses_owned_row["library_hold_ratio"]) == -1

                # >> Report: holds with no licenses.
                assert holds_with_no_licenses_report_zip_entry
                assert "test_library" in holds_with_no_licenses_report_zip_entry.name
                assert holds_with_no_licenses_report_zip_entry
                holds_with_no_licenses_report_csv = zip_csv_entry_to_dict(
                    holds_with_no_licenses_report_zip_entry
                )

                # Only our single book with no licenses should be in the holds report.
                assert len(holds_with_no_licenses_report_csv) == 1

                row = next(
                    r
                    for r in holds_with_no_licenses_report_csv
                    if r["identifier"] == identifier2_value
                )
                no_holds_row = next(
                    (
                        r
                        for r in holds_with_no_licenses_report_csv
                        if r["identifier"] == no_holds_identifier_value
                    ),
                    None,
                )
                assert no_holds_row is None

                # Ensure that our test book is described properly in the holds report.
                assert row["title"] == title2
                assert row["author"] == author2
                assert row["identifier"] == identifier2_value
                assert row["isbn"] == isbn2
                assert row["language"] == language
                assert row["publisher"] == publisher
                assert row["audience"] == "Adult"
                assert row["genres"] == "genre_z"
                assert row["format"] == edition.BOOK_MEDIUM
                assert row["data_source"] == data_source
                assert row["collection_name"] == collection_name
                assert int(row["shared_active_hold_count"]) == 0
                assert int(row["library_active_hold_count"]) == 1
    finally:
        os.remove(reports_zip)


def test_inventory_report_visibility_columns(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
):
    library = db.library(short_name="test_library")
    collection = create_test_opds_collection(
        "Visibility Collection", "VisibilitySource", db, library
    )
    ds = collection.data_source
    assert ds is not None

    filtered_audiences = ["young adult"]
    filtered_genres = ["filtered_genre"]

    visible_work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
    visible_work.audience = "adult"
    genre_ok, _ = Genre.lookup(db.session, "genre_ok", autocreate=True)
    visible_work.genres.append(genre_ok)

    audience_filtered_work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
    audience_filtered_work.audience = "young adult"
    audience_filtered_work.genres.append(genre_ok)

    genre_filtered_work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
    genre_filtered_work.audience = "adult"
    genre_filtered, _ = Genre.lookup(db.session, "filtered_genre", autocreate=True)
    genre_filtered_work.genres.append(genre_filtered)

    suppressed_work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
    suppressed_work.audience = "young adult"
    suppressed_work.genres.append(genre_filtered)
    suppressed_work.suppressed_for.append(library)

    integration_ids = [collection.integration_configuration.id]
    sql_params = {
        "library_id": library.id,
        "integration_ids": tuple(integration_ids),
        "filtered_audiences": filtered_audiences,
        "filtered_audiences_enabled": True,
        "filtered_genres": filtered_genres,
        "filtered_genres_enabled": True,
    }

    csv_file = io.StringIO()
    csv_file.name = "test_visibility_report.csv"

    generate_csv_report(
        db=db.session,
        csv_file=csv_file,
        sql_params=sql_params,
        query=inventory_report_query(),
    )

    csv_file.seek(0)
    rows = list(csv.DictReader(csv_file))

    def row_for(work):
        identifier_value = work.presentation_edition.primary_identifier.identifier
        return next(r for r in rows if r["identifier"] == identifier_value)

    visible_row = row_for(visible_work)
    assert visible_row["visible"] == "true"
    assert visible_row["visibility_status"] == ""

    audience_filtered_row = row_for(audience_filtered_work)
    assert audience_filtered_row["visible"] == "false"
    assert audience_filtered_row["visibility_status"] == "filtered"

    genre_filtered_row = row_for(genre_filtered_work)
    assert genre_filtered_row["visible"] == "false"
    assert genre_filtered_row["visibility_status"] == "filtered"

    suppressed_row = row_for(suppressed_work)
    assert suppressed_row["visible"] == "false"
    assert suppressed_row["visibility_status"] == "manually suppressed"


@pytest.mark.parametrize(
    "status,licenses_owned,licenses_available,license_exception",
    [
        pytest.param(
            LicensePoolStatus.ACTIVE,
            5,
            3,
            None,
            id="active-with-licenses",
        ),
        pytest.param(
            LicensePoolStatus.PRE_ORDER,
            5,
            0,
            None,
            id="pre-order",
        ),
        pytest.param(
            LicensePoolStatus.EXHAUSTED,
            0,
            0,
            None,
            id="exhausted-no-licenses",
        ),
        pytest.param(
            LicensePoolStatus.REMOVED,
            3,
            0,
            "Copyright violation",
            id="removed-copyright-violation",
        ),
        pytest.param(
            LicensePoolStatus.ACTIVE,
            0,
            0,
            None,
            id="active-but-no-licenses",
        ),
    ],
)
def test_inventory_report_item_status(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
    status: LicensePoolStatus,
    licenses_owned: int,
    licenses_available: int,
    license_exception: str | None,
):
    """Verify that item_status in inventory report reflects LicensePool status correctly."""
    library = db.library(short_name="test_lib")
    collection = create_test_opds_collection(
        "Test Collection", "TestSource", db, library
    )
    ds = collection.data_source
    assert ds is not None

    # Create a license pool with the specified status
    work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
    pool = work.active_license_pool()
    assert pool is not None
    pool.status = status
    pool.licenses_owned = licenses_owned
    pool.licenses_available = licenses_available
    if license_exception:
        pool.license_exception = license_exception

    # Generate the inventory report
    integration_ids = [collection.integration_configuration.id]
    sql_params = {"library_id": library.id, "integration_ids": tuple(integration_ids)}

    csv_file = io.StringIO()
    csv_file.name = "test_item_status_report.csv"

    generate_csv_report(
        db=db.session,
        csv_file=csv_file,
        sql_params=sql_params,
        query=inventory_report_query(),
    )

    csv_file.seek(0)
    reader = csv.DictReader(csv_file)
    [item_row] = list(reader)

    # Check the status
    assert item_row["item_status"] == str(status)


def zip_csv_entry_to_dict(zip_entry: IO[bytes]):
    wrapper = io.TextIOWrapper(zip_entry, encoding="UTF-8")
    csv_dict = list(csv.DictReader(wrapper))
    return csv_dict


def create_test_opds_collection(
    collection_name: str,
    data_source: str,
    db: DatabaseTransactionFixture,
    library: Library,
    include_in_inventory_report: bool = True,
):
    settings = OPDSImporterSettings(
        include_in_inventory_report=include_in_inventory_report,
        external_account_id="http://opds.com",
        data_source=data_source,
    )
    collection = db.collection(name=collection_name, settings=settings)
    collection.associated_libraries = [library]
    return collection


def test_generate_inventory_and_hold_reports_task(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
    celery_fixture: CeleryFixture,
):

    mock_s3_service = MagicMock()
    mock_s3_service.generate_url.return_value = "http://test"
    services_fixture.services.storage.public.override(mock_s3_service)

    library = db.library(short_name="test_library")
    # there must be at least one opds collection associated with the library for this to work
    create_test_opds_collection("c1", "d1", db, library)
    generate_inventory_and_hold_reports.delay(library.id, "test@email").wait()
    services_fixture.emailer.send.assert_called_once()

    mock_s3_service.store_stream.assert_called_once()
    mock_s3_service.generate_url.assert_called_once()

    assert (
        "Inventory and Holds Reports"
        in services_fixture.emailer.send.call_args.kwargs["subject"]
    )
    assert services_fixture.emailer.send.call_args.kwargs["receivers"] == ["test@email"]
    assert (
        "Download Report here -> http://test"
        in services_fixture.emailer.send.call_args.kwargs["text"]
    )
    assert (
        "This report will be available for download for 30 days."
        in services_fixture.emailer.send.call_args.kwargs["text"]
    )
