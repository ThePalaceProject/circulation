import csv
import io
import os
import zipfile
from collections.abc import Callable
from datetime import timedelta
from typing import IO, BinaryIO
from unittest.mock import MagicMock, create_autospec

import pytest
from pytest import LogCaptureFixture
from sqlalchemy.sql import Select

from palace.manager.celery.tasks.generate_inventory_and_hold_reports import (
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
from palace.manager.sqlalchemy.model.classification import Genre
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


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
                "status",
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
                "license_expiration",
                "days_remaining_on_license",
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
    identifier_value = "urn:identifier-1"
    checkouts_left = 10
    terms_concurrency = 5
    edition = db.edition(data_source_name=ds.name)
    edition.language = language
    edition.publisher = publisher
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
    identifier2_value = "urn:identifier-2"
    edition2 = db.edition(data_source_name=ds.name)
    edition2.language = language
    edition2.publisher = publisher
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
    db.license(
        pool=licensepool,
        status=LicenseStatus.available,
        checkouts_left=checkouts_left,
        terms_concurrency=terms_concurrency,
        expires=expiration,
    )
    db.license(
        pool=licensepool,
        status=LicenseStatus.unavailable,
        checkouts_left=1,
        terms_concurrency=1,
        expires=utc_now(),
    )

    db.license(
        pool=licensepool_no_licenses_owned,
        status=LicenseStatus.available,
        terms_concurrency=1,
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

    # Add a book that doesn't have any holds, so we can verify that it's not in the holds report.
    no_holds_work = db.work(
        data_source_name=ds.name, collection=collection, with_license_pool=True
    )
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

                # The inventory report should have two rows, since we have three books.
                assert len(inventory_report_csv) == 3
                # One row should be our well-described test book...
                row = next(
                    r
                    for r in inventory_report_csv
                    if r["identifier"] == identifier_value
                )
                # ... and the other should be our poorly-described book with no holds.
                _ = next(
                    r
                    for r in inventory_report_csv
                    if r["identifier"] == no_holds_identifier_value
                )

                # Ensure that our test book is described properly in the inventory report.
                assert row["status"] == str(LicenseStatus.available)
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
                assert float(row["days_remaining_on_license"]) == float(days_remaining)
                assert row["remaining_loans"] == str(checkouts_left)
                assert row["allowed_concurrent_users"] == str(terms_concurrency)
                assert (
                    expiration.strftime("%Y-%m-%d %H:%M:%S.%f")
                    in row["license_expiration"]
                )

                # >> Report: inventory activity report.
                assert inventory_activity_report_zip_entry
                assert "test_library" in inventory_activity_report_zip_entry.name
                inventory_activity_report_csv = zip_csv_entry_to_dict(
                    inventory_activity_report_zip_entry
                )

                # The activity report includes all books (with or without holds).
                assert len(inventory_activity_report_csv) == 3
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
                # Even the book with no holds should be included in the activity report.
                assert no_holds_row is not None

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

                # Test the book with no holds
                assert no_holds_row is not None
                assert no_holds_row["title"] is not None
                assert no_holds_row["identifier"] == no_holds_identifier_value
                # Activity report fields for book with no holds
                assert int(no_holds_row["library_active_hold_count"]) == 0
                assert int(no_holds_row["library_active_loan_count"]) == 0
                # Since there are no holds and licenses_available > 0, ratio should be 0
                assert float(no_holds_row["library_hold_ratio"]) == 0

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
