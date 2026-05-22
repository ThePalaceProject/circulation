"""Unit tests for BibliothecaPurchaseImporter."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

from palace.util.datetime_helpers import datetime_utc, utc_now

from palace.manager.celery.tasks import apply
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.bibliotheca_purchase_importer import (
    DEFAULT_PURCHASE_START_TIME,
    PURCHASE_SERVICE_NAME,
    BibliothecaPurchaseImporter,
    DayImportResult,
)
from palace.manager.sqlalchemy.model.coverage import Timestamp
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI


def _make_importer(
    db: DatabaseTransactionFixture,
    api: MockBibliothecaAPI | None = None,
) -> tuple[BibliothecaPurchaseImporter, MockBibliothecaAPI]:
    """Return an importer and its bound API for the default test collection."""
    collection = MockBibliothecaAPI.mock_collection(db.session, db.default_library())
    mock_api = api or MockBibliothecaAPI(db.session, collection)
    importer = BibliothecaPurchaseImporter(db.session, collection, api=mock_api)
    return importer, mock_api


def _fake_marc_record(bibliotheca_id: str = "d5rf89") -> MagicMock:
    """Return a minimal mock pymarc Record with a single 001 control field."""
    field = MagicMock()
    field.tag = "001"
    field.value.return_value = bibliotheca_id
    record = MagicMock()
    record.fields = [field]
    return record


class TestBibliothecaPurchaseImporterGetStart:
    def test_no_prior_timestamp_returns_default_start(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With no stored Timestamp, get_start returns DEFAULT_PURCHASE_START_TIME."""
        importer, _ = _make_importer(db)
        start = importer.get_start()
        assert start == DEFAULT_PURCHASE_START_TIME

    def test_prior_timestamp_returns_its_finish(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With a stored Timestamp, get_start returns timestamp.finish."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection
        finish = utc_now() - timedelta(days=30)
        Timestamp.stamp(
            db.session,
            service=PURCHASE_SERVICE_NAME,
            service_type=Timestamp.MONITOR_TYPE,
            collection=collection,
            finish=finish,
        )

        start = importer.get_start()
        assert abs((start - finish).total_seconds()) < 1


class TestBibliothecaPurchaseImporterImportDay:
    def test_returns_day_result_with_correct_bounds(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """import_day returns a DayImportResult with the correct window."""
        importer, _ = _make_importer(db)
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        with patch.object(BibliothecaAPI, "marc_request", return_value=iter([])):
            result = importer.import_day(current_day, cutoff)

        assert isinstance(result, DayImportResult)
        assert result.day_start == current_day
        expected_end = current_day + timedelta(days=1)
        assert abs((result.day_end - expected_end).total_seconds()) < 1
        assert result.records_handled == 0

    def test_day_end_capped_at_cutoff(self, db: DatabaseTransactionFixture) -> None:
        """When current_day + 1 day > cutoff, day_end is capped at cutoff."""
        importer, _ = _make_importer(db)
        cutoff = utc_now()
        # Start just a few hours before cutoff — less than a full day.
        current_day = cutoff - timedelta(hours=3)

        with patch.object(BibliothecaAPI, "marc_request", return_value=iter([])):
            result = importer.import_day(current_day, cutoff)

        assert abs((result.day_end - cutoff).total_seconds()) < 1

    def test_stamps_timestamp_after_processing(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """import_day updates the Timestamp so the next day starts correctly."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        with patch.object(BibliothecaAPI, "marc_request", return_value=iter([])):
            result = importer.import_day(current_day, cutoff)

        ts = Timestamp.lookup(
            db.session, PURCHASE_SERVICE_NAME, Timestamp.MONITOR_TYPE, collection
        )
        assert ts is not None
        assert ts.finish is not None
        assert abs((ts.finish - result.day_end).total_seconds()) < 1

    def test_counts_records_handled(self, db: DatabaseTransactionFixture) -> None:
        """records_handled in the result matches the number of records processed."""
        importer, _ = _make_importer(db)
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        fake_records = [_fake_marc_record(f"item{i}") for i in range(3)]

        with (
            patch.object(
                BibliothecaAPI, "marc_request", return_value=iter(fake_records)
            ),
            patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]),
        ):
            result = importer.import_day(current_day, cutoff)

        assert result.records_handled == 3

    def test_paginates_marc_request(self, db: DatabaseTransactionFixture) -> None:
        """_purchases calls marc_request again when the page is full (50 records)."""
        importer, _ = _make_importer(db)
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        full_page = [_fake_marc_record(f"item{i}") for i in range(50)]
        partial_page = [_fake_marc_record("item_last")]

        call_count = 0

        def mock_marc_request(
            start: datetime,
            end: datetime,
            offset: int,
            limit: int,
        ):  # type: ignore[no-untyped-def]
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return iter(full_page)
            return iter(partial_page)

        with (
            patch.object(BibliothecaAPI, "marc_request", side_effect=mock_marc_request),
            patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]),
        ):
            result = importer.import_day(current_day, cutoff)

        assert call_count == 2
        assert result.records_handled == 51


class TestBibliothecaPurchaseImporterProcessRecord:
    """Tests for _process_record logic, exercised via import_day."""

    def test_creates_license_pool_for_valid_record(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """_process_record creates a LicensePool for a record with a valid 001 field."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        with (
            patch.object(
                BibliothecaAPI,
                "marc_request",
                return_value=iter([_fake_marc_record("d5rf89")]),
            ),
            patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]),
        ):
            importer.import_day(current_day, cutoff)

        pools = [
            lp for lp in collection.licensepools if lp.identifier.identifier == "d5rf89"
        ]
        assert len(pools) == 1

    def test_skips_record_with_no_control_number(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """_process_record logs an error and skips records with no 001 field."""
        importer, _ = _make_importer(db)
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        # Record with no 001 field.
        bad_record = MagicMock()
        bad_record.fields = []
        bad_record.as_json.return_value = "{}"

        with patch.object(
            BibliothecaAPI, "marc_request", return_value=iter([bad_record])
        ):
            result = importer.import_day(current_day, cutoff)

        # The record was "handled" (iterated) but produced no LicensePool.
        assert result.records_handled == 1

    def test_queues_bibliographic_apply_when_needed(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """bibliographic_apply is queued when the bibliographic data has changed."""
        importer, _ = _make_importer(db)
        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = True
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        with (
            patch.object(
                BibliothecaAPI,
                "marc_request",
                return_value=iter([_fake_marc_record()]),
            ),
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            importer.import_day(current_day, cutoff)

        mock_apply.delay.assert_called_once()

    def test_skips_bibliographic_apply_when_not_needed(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """bibliographic_apply is not queued when the data is already up to date."""
        importer, _ = _make_importer(db)
        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = False
        current_day = datetime_utc(2024, 1, 15)
        cutoff = datetime_utc(2024, 1, 20)

        with (
            patch.object(
                BibliothecaAPI,
                "marc_request",
                return_value=iter([_fake_marc_record()]),
            ),
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            importer.import_day(current_day, cutoff)

        mock_apply.delay.assert_not_called()
