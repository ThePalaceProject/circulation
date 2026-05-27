"""Unit tests for BibliothecaEventImporter."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.tasks import apply
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.bibliotheca_importer import (
    DEFAULT_SLICE_SIZE,
    EVENT_IMPORT_OVERLAP,
    EVENT_IMPORT_SERVICE_NAME,
    BibliothecaEventImporter,
    SliceImportResult,
)
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI


def _make_importer(
    db: DatabaseTransactionFixture,
    api: MockBibliothecaAPI | None = None,
) -> tuple[BibliothecaEventImporter, MockBibliothecaAPI]:
    """Return an importer and its bound API for the default test collection."""
    collection = MockBibliothecaAPI.mock_collection(db.session, db.default_library())
    mock_api = api or MockBibliothecaAPI(db.session, collection)
    importer = BibliothecaEventImporter(db.session, collection, api=mock_api)
    return importer, mock_api


class TestBibliothecaEventImporterGetStart:
    def test_no_prior_timestamp_returns_cutoff_minus_overlap(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With no stored Timestamp, get_start falls back to cutoff - OVERLAP."""
        importer, _ = _make_importer(db)
        cutoff = utc_now()
        start = importer.get_start(cutoff)
        assert abs((start - (cutoff - EVENT_IMPORT_OVERLAP)).total_seconds()) < 1

    def test_prior_timestamp_returns_finish_minus_overlap(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """With a stored Timestamp, get_start returns timestamp.finish - OVERLAP."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection
        finish = utc_now() - timedelta(hours=1)
        Timestamp.stamp(
            db.session,
            service=EVENT_IMPORT_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection,
            finish=finish,
        )

        cutoff = utc_now()
        start = importer.get_start(cutoff)
        assert abs((start - (finish - EVENT_IMPORT_OVERLAP)).total_seconds()) < 1

    def test_timestamp_with_null_finish_falls_back_to_cutoff(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """A Timestamp whose finish is None is treated the same as no Timestamp."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection
        Timestamp.stamp(
            db.session,
            service=EVENT_IMPORT_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection,
            finish=None,
        )

        cutoff = utc_now()
        start = importer.get_start(cutoff)
        assert abs((start - (cutoff - EVENT_IMPORT_OVERLAP)).total_seconds()) < 1


class TestBibliothecaEventImporterImportTimeSlice:
    def test_returns_slice_result_with_correct_bounds(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """import_time_slice returns a SliceImportResult with the correct window."""
        importer, mock_api = _make_importer(db)
        mock_api.queue_response(200, content=b"<Events></Events>")

        start = utc_now() - timedelta(minutes=30)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with patch.object(BibliothecaAPI, "get_events_between", return_value=iter([])):
            result = importer.import_time_slice(start, cutoff)

        assert isinstance(result, SliceImportResult)
        assert result.slice_start == start
        assert result.events_handled == 0
        # slice_end is capped at min(start + DEFAULT_SLICE_SIZE, cutoff)
        expected_end = min(start + DEFAULT_SLICE_SIZE, cutoff)
        assert abs((result.slice_end - expected_end).total_seconds()) < 1

    def test_slice_capped_at_cutoff(self, db: DatabaseTransactionFixture) -> None:
        """When start + slice_size > cutoff, slice_end is capped at cutoff."""
        importer, _ = _make_importer(db)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP
        # Start only 2 minutes before cutoff — less than the 5-minute slice size.
        start = cutoff - timedelta(minutes=2)

        with patch.object(BibliothecaAPI, "get_events_between", return_value=iter([])):
            result = importer.import_time_slice(start, cutoff)

        assert abs((result.slice_end - cutoff).total_seconds()) < 1

    def test_custom_slice_size_respected(self, db: DatabaseTransactionFixture) -> None:
        """A caller-supplied slice_size overrides the 5-minute default."""
        importer, _ = _make_importer(db)
        start = utc_now() - timedelta(hours=2)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP
        custom_size = timedelta(minutes=30)

        with patch.object(BibliothecaAPI, "get_events_between", return_value=iter([])):
            result = importer.import_time_slice(start, cutoff, slice_size=custom_size)

        expected_end = min(start + custom_size, cutoff)
        assert abs((result.slice_end - expected_end).total_seconds()) < 1

    def test_stamps_timestamp_after_processing(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """import_time_slice updates the Timestamp so the next slice starts correctly."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection
        start = utc_now() - timedelta(minutes=30)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with patch.object(BibliothecaAPI, "get_events_between", return_value=iter([])):
            result = importer.import_time_slice(start, cutoff)

        ts = Timestamp.lookup(
            db.session,
            EVENT_IMPORT_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection,
        )
        assert ts is not None
        assert ts.finish is not None
        assert abs((ts.finish - result.slice_end).total_seconds()) < 1

    def test_counts_events_handled(self, db: DatabaseTransactionFixture) -> None:
        """events_handled in the result matches the number of events processed."""
        importer, mock_api = _make_importer(db)
        event_time = datetime(2016, 4, 28, 11, 4, 6, tzinfo=timezone.utc)
        fake_events = [
            (
                f"item{i}",
                "9781101190623",
                None,
                event_time,
                None,
                CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
            )
            for i in range(3)
        ]

        start = utc_now() - timedelta(minutes=30)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with (
            patch.object(
                BibliothecaAPI, "get_events_between", return_value=iter(fake_events)
            ),
            patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]),
        ):
            result = importer.import_time_slice(start, cutoff)

        assert result.events_handled == 3


class TestBibliothecaEventImporterHandleEvent:
    """Tests for the _handle_event logic, exercised via import_time_slice."""

    def _fake_event(
        self,
        bibliotheca_id: str = "d5rf89",
        isbn: str = "9781101190623",
        event_type: str = CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
    ) -> tuple:
        event_time = datetime(2016, 4, 28, 11, 4, 6, tzinfo=timezone.utc)
        return (bibliotheca_id, isbn, None, event_time, None, event_type)

    def test_creates_license_pool_and_isbn_equivalency(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """_handle_event creates a LicensePool and links the Bibliotheca ID to the ISBN."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection

        start = utc_now() - timedelta(minutes=10)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with (
            patch.object(
                BibliothecaAPI,
                "get_events_between",
                return_value=iter([self._fake_event()]),
            ),
            patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]),
        ):
            importer.import_time_slice(start, cutoff)

        pools = [
            lp for lp in collection.licensepools if lp.identifier.identifier == "d5rf89"
        ]
        assert len(pools) == 1

        isbn_id = (
            db.session.query(Identifier)
            .filter_by(type=Identifier.ISBN, identifier="9781101190623")
            .one_or_none()
        )
        assert isbn_id is not None
        equivalencies = [
            eq for eq in pools[0].identifier.equivalencies if eq.output == isbn_id
        ]
        assert len(equivalencies) == 1
        assert equivalencies[0].strength == 1

    def test_updates_availability(self, db: DatabaseTransactionFixture) -> None:
        """A DISTRIBUTOR_LICENSE_ADD event increments licenses_owned and available."""
        importer, mock_api = _make_importer(db)
        collection = mock_api.collection

        start = utc_now() - timedelta(minutes=10)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with (
            patch.object(
                BibliothecaAPI,
                "get_events_between",
                return_value=iter([self._fake_event()]),
            ),
            patch.object(BibliothecaAPI, "bibliographic_lookup", return_value=[]),
        ):
            importer.import_time_slice(start, cutoff)

        pool = next(
            lp for lp in collection.licensepools if lp.identifier.identifier == "d5rf89"
        )
        assert pool.licenses_owned == 1
        assert pool.licenses_available == 1

    def test_queues_bibliographic_apply_when_needed(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """bibliographic_apply is queued when the bibliographic data has changed."""
        importer, mock_api = _make_importer(db)
        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = True

        start = utc_now() - timedelta(minutes=10)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with (
            patch.object(
                BibliothecaAPI,
                "get_events_between",
                return_value=iter([self._fake_event()]),
            ),
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            importer.import_time_slice(start, cutoff)

        mock_apply.delay.assert_called_once()

    def test_skips_bibliographic_apply_when_not_needed(
        self, db: DatabaseTransactionFixture
    ) -> None:
        """bibliographic_apply is not queued when the data is already up to date."""
        importer, mock_api = _make_importer(db)
        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = False

        start = utc_now() - timedelta(minutes=10)
        cutoff = utc_now() - EVENT_IMPORT_OVERLAP

        with (
            patch.object(
                BibliothecaAPI,
                "get_events_between",
                return_value=iter([self._fake_event()]),
            ),
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            importer.import_time_slice(start, cutoff)

        mock_apply.delay.assert_not_called()
