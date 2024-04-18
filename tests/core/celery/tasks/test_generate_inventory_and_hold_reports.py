import csv
import os
from datetime import timedelta
from unittest.mock import create_autospec

from pytest import LogCaptureFixture
from sqlalchemy.orm import sessionmaker

from api.overdrive import OverdriveSettings
from core.celery.tasks.generate_inventory_and_hold_reports import (
    GenerateInventoryAndHoldsReportsJob,
    generate_inventory_and_hold_reports,
)
from core.model import Genre, Hold, Library, get_one_or_create
from core.model.licensing import LicenseStatus
from core.opds_import import OPDSImporterSettings
from core.service.logging.configuration import LogLevel
from core.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


def test_job_run(
    db: DatabaseTransactionFixture,
    mock_session_maker: sessionmaker,
    services_fixture: ServicesFixture,
    caplog: LogCaptureFixture,
):
    email = "test@email.com"

    # A non-existent collection should log an error
    caplog.set_level(LogLevel.info)
    send_email_mock = create_autospec(
        services_fixture.services.email.container.send_email
    )
    GenerateInventoryAndHoldsReportsJob(
        mock_session_maker,
        library_id=1,
        email_address=email,
        send_email=send_email_mock,
    ).run()
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
    collection.libraries.append(library2)

    # Configure test data we expect will not be picked up.
    no_inventory_report_settings = OPDSImporterSettings(
        include_in_inventory_report=False,
        external_account_id="http://opds.com",
        data_source="AnotherOpdsDataSource",
    )
    collection_not_to_include = db.collection(
        name="Another Test Collection", settings=no_inventory_report_settings.dict()
    )
    collection_not_to_include.libraries = [library]

    od_settings = OverdriveSettings(
        overdrive_website_id="overdrive_id",
        overdrive_client_key="client_key",
        overdrive_client_secret="secret",
        external_account_id="http://www.overdrive/id",
    )
    od_collection_not_to_include = db.collection(
        name="Overdrive Test Collection",
        data_source_name="Overdrive",
        settings=od_settings.dict(),
    )

    od_collection_not_to_include.libraries = [library]

    ds = collection.data_source
    assert ds
    title = "展翅高飞 : Whistling Wings"
    author = "Laura Goering"
    language = "eng"
    publisher = "My Publisher"
    checkouts_left = 10
    terms_concurrency = 5
    edition = db.edition(data_source_name=ds.name)
    edition.language = language
    edition.publisher = publisher
    edition.title = title
    edition.medium = edition.BOOK_MEDIUM
    edition.author = author
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
    )

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

    patron1 = db.patron(library=library)
    patron2 = db.patron(library=library)
    patron3 = db.patron(library=library)
    patron4 = db.patron(library=library)

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

    shared_patrons_in_hold_queue = 4
    licensepool.patrons_in_hold_queue = shared_patrons_in_hold_queue

    assert library.id

    # for testing, don't delete the files associated with the attachments so we can read them after the script
    # runs
    job = GenerateInventoryAndHoldsReportsJob(
        mock_session_maker,
        library.id,
        email_address=email,
        send_email=send_email_mock,
        delete_attachments=False,
    )

    job.run()
    send_email_mock.assert_called_once()
    kwargs = send_email_mock.call_args.kwargs
    assert kwargs["receivers"] == [email]
    assert "Inventory and Holds Reports" in kwargs["subject"]
    attachments: dict = kwargs["attachments"]

    assert len(attachments) == 2
    inventory_report_key = [x for x in attachments.keys() if "inventory" in x][0]
    assert inventory_report_key
    assert "test_library" in inventory_report_key
    inventory_report_value = attachments[inventory_report_key]
    assert inventory_report_value
    inventory_report_csv = list(csv.DictReader(open(inventory_report_value)))

    assert len(inventory_report_csv) == 1
    for row in inventory_report_csv:
        assert row["title"] == title
        assert row["author"] == author
        assert row["identifier"]
        assert row["language"] == language
        assert row["publisher"] == publisher
        assert row["audience"] == "young adult"
        assert row["genres"] == "genre_a,genre_z"
        assert row["format"] == edition.BOOK_MEDIUM
        assert row["data_source"] == data_source
        assert row["collection_name"] == collection_name
        assert float(row["days_remaining_on_license"]) == float(days_remaining)
        assert row["shared_active_loan_count"] == "0"
        assert row["library_active_loan_count"] == "0"
        assert row["remaining_loans"] == str(checkouts_left)
        assert row["allowed_concurrent_users"] == str(terms_concurrency)
        assert expiration.strftime("%Y-%m-%d %H:%M:%S.%f") in row["license_expiration"]

    holds_report_key = [x for x in attachments.keys() if "holds" in x][0]
    assert holds_report_key
    assert "test_library" in holds_report_key
    holds_report_value = attachments[holds_report_key]
    assert holds_report_value
    holds_report_csv = list(csv.DictReader(open(holds_report_value)))
    assert len(holds_report_csv) == 1

    for row in holds_report_csv:
        assert row["title"] == title
        assert row["author"] == author
        assert row["identifier"]
        assert row["language"] == language
        assert row["publisher"] == publisher
        assert row["audience"] == "young adult"
        assert row["genres"] == "genre_a,genre_z"
        assert row["format"] == edition.BOOK_MEDIUM
        assert row["data_source"] == data_source
        assert row["collection_name"] == collection_name
        assert int(row["shared_active_hold_count"]) == shared_patrons_in_hold_queue
        assert int(row["library_active_hold_count"]) == 3

    # clean up files
    for f in attachments.values():
        os.remove(f)


def create_test_opds_collection(
    collection_name: str,
    data_source: str,
    db: DatabaseTransactionFixture,
    library: Library,
):
    settings = OPDSImporterSettings(
        include_in_inventory_report=True,
        external_account_id="http://opds.com",
        data_source=data_source,
    )
    collection = db.collection(name=collection_name, settings=settings.dict())
    collection.libraries = [library]
    return collection


def test_generate_inventory_and_hold_reports_task(
    db: DatabaseTransactionFixture,
    services_fixture: ServicesFixture,
    celery_fixture: CeleryFixture,
):
    library = db.library(short_name="test_library")
    # there must be at least one opds collection associated with the library for this to work
    create_test_opds_collection("c1", "d1", db, library)
    send_email_mock = create_autospec(
        services_fixture.services.email.container.send_email
    )
    services_fixture.services.email.container.send_email = send_email_mock
    generate_inventory_and_hold_reports.delay(library.id, "test@email").wait()
    send_email_mock.assert_called_once()
