"""Tests for Bibliotheca Celery tasks."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch
from uuid import uuid4

import pytest

from palace.util.datetime_helpers import datetime_utc, utc_now
from palace.util.log import LogLevel

from palace.manager.api.circulation.exceptions import RemoteInitiatedServerError
from palace.manager.celery.importer import import_workflow_lock
from palace.manager.celery.tasks import apply, bibliotheca
from palace.manager.celery.tasks.bibliotheca import (
    _circulation_update_workflow_lock,
    _purchase_record_workflow_lock,
)
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.bibliotheca_circulation_updater import (
    CIRCULATION_UPDATE_BATCH_SIZE,
    CIRCULATION_UPDATE_SERVICE_NAME,
    BatchUpdateResult,
)
from palace.manager.integration.license.bibliotheca_importer import (
    EVENT_IMPORT_SERVICE_NAME,
)
from palace.manager.integration.license.bibliotheca_purchase_record_importer import (
    _MARC_PAGE_SIZE,
    DEFAULT_PURCHASE_RECORD_START_TIME,
    PURCHASE_RECORD_SERVICE_NAME,
    DayImportResult,
)
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI
from tests.mocks.mock import MockRequestsResponse


class BibliothecaTaskFixture:
    """Common setup for Bibliotheca Celery task tests."""

    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )

    def mock_api(self) -> MockBibliothecaAPI:
        """Return a fresh MockBibliothecaAPI bound to this fixture's collection."""
        return MockBibliothecaAPI(self.db.session, self.collection)

    def stamp_event_import(
        self, finish: datetime | None = None, collection: Collection | None = None
    ) -> Timestamp:
        """Create (or update) the event-import Timestamp for the collection."""
        return Timestamp.stamp(
            self.db.session,
            service=EVENT_IMPORT_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection or self.collection,
            finish=finish or utc_now(),
        )

    def get_event_import_timestamp(
        self, collection: Collection | None = None
    ) -> Timestamp | None:
        return Timestamp.lookup(
            self.db.session,
            EVENT_IMPORT_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection or self.collection,
        )


@pytest.fixture
def bibliotheca_task_fixture(
    db: DatabaseTransactionFixture,
) -> BibliothecaTaskFixture:
    return BibliothecaTaskFixture(db)


class TestBibliothecaImportAllCollections:
    def test_queues_import_collection_for_each_bibliotheca_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """import_all_collections queues import_collection for every Bibliotheca
        collection and ignores collections using other protocols."""
        db.default_collection()  # non-Bibliotheca, should be ignored
        c1 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 1"
        )
        c2 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 2"
        )

        with patch.object(bibliotheca, "import_collection") as mock_task:
            bibliotheca.import_all_collections.delay().wait()

        mock_task.delay.assert_has_calls(
            [call(collection_id=c1.id), call(collection_id=c2.id)],
            any_order=True,
        )
        assert mock_task.delay.call_count == 2

    def test_no_bibliotheca_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """import_all_collections is a no-op when there are no Bibliotheca collections."""
        db.default_collection()  # non-Bibliotheca

        with patch.object(bibliotheca, "import_collection") as mock_task:
            bibliotheca.import_all_collections.delay().wait()

        mock_task.delay.assert_not_called()


class TestBibliothecaImportCollection:
    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_creates_timestamp_on_first_run(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """On the first run (no prior Timestamp), the task creates one."""
        mock_api_cls.return_value.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        ts = bibliotheca_task_fixture.get_event_import_timestamp()
        assert ts is not None
        assert ts.finish is not None

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_starts_from_stored_timestamp(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The task starts from ``timestamp.finish - OVERLAP`` when a prior
        Timestamp exists."""
        mock_api = mock_api_cls.return_value
        mock_api.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        one_hour_ago = utc_now() - timedelta(hours=1)
        bibliotheca_task_fixture.stamp_event_import(finish=one_hour_ago)

        # Stop after the first slice so the replace chain doesn't keep running.
        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        # get_events_between should have been called with a start time ≈
        # one_hour_ago - OVERLAP (5 min), using the first call_args only.
        call_args = mock_api.get_events_between.call_args_list[0]
        assert call_args is not None
        slice_start, slice_end = call_args.args
        expected_start = one_hour_ago - timedelta(minutes=5)
        assert abs((slice_start - expected_start).total_seconds()) < 2

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_explicit_start_used_directly(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When an explicit ``start`` is supplied (the chained-slice path), it is
        passed directly to the API without consulting the stored Timestamp."""
        mock_api = mock_api_cls.return_value
        mock_api.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        explicit_start = utc_now() - timedelta(hours=2)

        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(
                    collection_id=collection.id,
                    start=explicit_start,
                ).wait()

        slice_start, _ = mock_api.get_events_between.call_args.args
        assert abs((slice_start - explicit_start).total_seconds()) < 1

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_already_up_to_date(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When the stored Timestamp is current (finish ≈ now), no API call is made."""
        collection = bibliotheca_task_fixture.collection
        # Stamp far enough in the future that start = finish - OVERLAP is still >= cutoff.
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() + timedelta(minutes=10)
        )

        bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        mock_api_cls.return_value.get_events_between.assert_not_called()

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_replaces_when_more_slices_remain(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When start is more than one slice behind cutoff, task.replace() is raised
        with the next slice's start."""
        mock_api_cls.return_value.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        # Stamp an old timestamp so several slices are needed.
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(hours=1)
        )

        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(
                    collection_id=collection.id,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        # The next start should be approximately 5 minutes after the slice started.
        assert replace_sig.kwargs["start"] is not None

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_no_replace_on_last_slice(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When the entire backlog fits in one slice, task.replace() is not called."""
        mock_api_cls.return_value.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        # Timestamp just 3 minutes old — fits in one 5-minute slice.
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(minutes=3)
        )

        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        mock_replace.assert_not_called()
        mock_api_cls.return_value.get_events_between.assert_called_once()

    def test_skips_when_lock_held(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the workflow lock is already held (another run is in progress), the
        task logs a warning and returns without making any API calls."""
        collection = bibliotheca_task_fixture.collection

        existing_lock_value = str(uuid4())
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, existing_lock_value
        )
        workflow_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI"
        ) as mock_api_cls:
            bibliotheca.import_collection.delay(collection_id=collection.id).wait()
            mock_api_cls.return_value.get_events_between.assert_not_called()

        assert "skipped" in caplog.text
        assert "already in progress" in caplog.text

        workflow_lock.release()

    def test_lock_not_released_on_autoretry(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """A retryable failure holds the workflow lock and each retry re-runs the import.

        The workflow lock is keyed on ``task.request.id``, which Celery preserves across
        retries, so every retry re-acquires the same lock and re-runs the slice rather
        than skipping as if another run were in progress. The lock stays held so no
        concurrent run can start.
        """
        collection = bibliotheca_task_fixture.collection
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(minutes=10)
        )

        mock_response = MockRequestsResponse(500, content="Internal Server Error")

        with patch(
            "palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI"
        ) as mock_api_cls:
            mock_api_cls.return_value.get_events_between.side_effect = (
                BadResponseException("http://test.com", "Bad response", mock_response)
            )

            with celery_fixture.patch_retry_backoff():
                bibliotheca.import_collection.delay(collection_id=collection.id).get(
                    propagate=False
                )

            # The slice was re-run on every retry (1 initial attempt + max_retries=4),
            # not skipped as an "already in progress" run.
            assert mock_api_cls.return_value.get_events_between.call_count == 5

        # Lock should still be held after retries exhaust — it will expire via
        # the 2-hour Redis TTL, preventing a concurrent run from starting.
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

    def test_remote_initiated_server_error_retried_and_expected(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """bibliographic_lookup (called per event by the importer) raises
        RemoteInitiatedServerError when Bibliotheca returns an empty response body —
        a transient condition that must be retried, not surfaced as an unhandled task
        exception.

        RemoteInitiatedServerError is a sibling of RemoteIntegrationException (both
        derive from IntegrationException), not a subclass, so it has to be listed
        explicitly in autoretry_for and throws. This guards against either being dropped.
        """
        task = bibliotheca.import_collection
        assert RemoteInitiatedServerError in task.autoretry_for
        assert RemoteInitiatedServerError in task.throws

        collection = bibliotheca_task_fixture.collection
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(minutes=10)
        )

        fake_event = (
            "d5rf89",
            "9781101190623",
            None,
            datetime(2016, 4, 28, 11, 4, 6, tzinfo=timezone.utc),
            None,
            CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        )

        with (
            # Return a list (re-iterable across retries) of one event so the importer
            # reaches _handle_event -> bibliographic_lookup, which raises.
            patch.object(
                BibliothecaAPI, "get_events_between", return_value=[fake_event]
            ),
            patch.object(
                BibliothecaAPI,
                "bibliographic_lookup",
                side_effect=RemoteInitiatedServerError(
                    "boom", BibliothecaAPI.SERVICE_NAME
                ),
            ) as mock_lookup,
        ):
            with celery_fixture.patch_retry_backoff():
                bibliotheca.import_collection.delay(collection_id=collection.id).get(
                    propagate=False
                )

            # Retried on every attempt (1 initial + max_retries=4) rather than
            # failing immediately as an unhandled exception.
            assert mock_lookup.call_count == 5

    def test_events_processed_end_to_end(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """Smoke test: the full Celery task path processes an event and advances the
        Timestamp.  Detailed event-handling assertions live in test_bibliotheca_importer.
        """
        collection = bibliotheca_task_fixture.collection

        ten_minutes_ago = utc_now() - timedelta(minutes=10)
        bibliotheca_task_fixture.stamp_event_import(finish=ten_minutes_ago)

        event_time = datetime(2016, 4, 28, 11, 4, 6, tzinfo=timezone.utc)
        fake_event = (
            "d5rf89",
            "9781101190623",
            None,
            event_time,
            None,
            CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        )

        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = True

        with (
            patch.object(
                BibliothecaAPI, "get_events_between", return_value=iter([fake_event])
            ),
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply"),
        ):
            bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        # A LicensePool was created — the task reached the importer.
        pools = [
            lp for lp in collection.licensepools if lp.identifier.identifier == "d5rf89"
        ]
        assert len(pools) == 1

        # Timestamp was advanced past the slice.
        ts = bibliotheca_task_fixture.get_event_import_timestamp()
        assert ts is not None
        assert ts.finish is not None
        assert ts.finish > ten_minutes_ago

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_timestamp_updated_after_each_slice(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The Timestamp.finish is advanced by one slice after each task invocation,
        enabling crash recovery without re-processing old events."""
        mock_api_cls.return_value.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        one_hour_ago = utc_now() - timedelta(hours=1)
        bibliotheca_task_fixture.stamp_event_import(finish=one_hour_ago)

        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(
                    collection_id=collection.id,
                ).wait()

        # After processing one slice the timestamp finish should have advanced
        # roughly 5 minutes beyond the start we derived.
        ts = bibliotheca_task_fixture.get_event_import_timestamp()
        assert ts is not None
        assert ts.finish is not None
        expected_start = one_hour_ago - timedelta(minutes=5)
        expected_finish = expected_start + timedelta(minutes=5)
        assert abs((ts.finish - expected_finish).total_seconds()) < 5

    def test_multiple_collections_each_get_own_lock(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """Each collection uses an independent workflow lock; two collections can
        run concurrently without interfering."""
        c1 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Col 1"
        )
        c2 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Col 2"
        )

        # Hold the lock for collection 1 only.
        lock_c1 = import_workflow_lock(redis_fixture.client, c1.id, str(uuid4()))
        lock_c1.acquire()

        with patch(
            "palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI"
        ) as mock_api_cls2:
            mock_api_cls2.return_value.get_events_between.return_value = iter([])
            # Collection 2 should process normally.
            bibliotheca.import_collection.delay(collection_id=c2.id).wait()
            # Collection 1 should be skipped (lock held).
            bibliotheca.import_collection.delay(collection_id=c1.id).wait()

            assert mock_api_cls2.return_value.get_events_between.call_count == 1

        lock_c1.release()


class BibliothecaPurchaseRecordTaskFixture:
    """Common setup for Bibliotheca purchase record importer Celery task tests."""

    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )

    def stamp_purchase_record(
        self, finish: datetime | None = None, collection: Collection | None = None
    ) -> Timestamp:
        """Create (or update) the purchase record Timestamp for the collection."""
        return Timestamp.stamp(
            self.db.session,
            service=PURCHASE_RECORD_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection or self.collection,
            finish=finish or utc_now(),
        )

    def get_purchase_record_timestamp(
        self, collection: Collection | None = None
    ) -> Timestamp | None:
        return Timestamp.lookup(
            self.db.session,
            PURCHASE_RECORD_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection or self.collection,
        )


@pytest.fixture
def bibliotheca_purchase_record_task_fixture(
    db: DatabaseTransactionFixture,
) -> BibliothecaPurchaseRecordTaskFixture:
    return BibliothecaPurchaseRecordTaskFixture(db)


class TestImportPurchaseRecordsForAllCollections:
    def test_queues_purchase_record_collection_for_each_bibliotheca_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """import_purchase_records_for_all_collections queues import_purchase_records_by_collection for every Bibliotheca
        collection and ignores collections using other protocols."""
        db.default_collection()  # non-Bibliotheca, should be ignored
        c1 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 1"
        )
        c2 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 2"
        )

        with patch.object(
            bibliotheca, "import_purchase_records_by_collection"
        ) as mock_task:
            bibliotheca.import_purchase_records_for_all_collections.delay().wait()

        mock_task.delay.assert_has_calls(
            [
                call(collection_id=c1.id, current_day=None, reset_timestamp=False),
                call(collection_id=c2.id, current_day=None, reset_timestamp=False),
            ],
            any_order=True,
        )
        assert mock_task.delay.call_count == 2

    def test_force_reimport_passes_start_date_and_reset_to_each_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """force_reimport=True passes current_day=DEFAULT_PURCHASE_RECORD_START_TIME and
        reset_timestamp=True to each per-collection task."""
        c1 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 1"
        )
        c2 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 2"
        )

        with patch.object(
            bibliotheca, "import_purchase_records_by_collection"
        ) as mock_task:
            bibliotheca.import_purchase_records_for_all_collections.delay(
                force_reimport=True
            ).wait()

        mock_task.delay.assert_has_calls(
            [
                call(
                    collection_id=c1.id,
                    current_day=DEFAULT_PURCHASE_RECORD_START_TIME,
                    reset_timestamp=True,
                ),
                call(
                    collection_id=c2.id,
                    current_day=DEFAULT_PURCHASE_RECORD_START_TIME,
                    reset_timestamp=True,
                ),
            ],
            any_order=True,
        )
        assert mock_task.delay.call_count == 2

    def test_no_bibliotheca_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """import_purchase_records_for_all_collections is a no-op when there are no Bibliotheca collections."""
        db.default_collection()  # non-Bibliotheca

        with patch.object(
            bibliotheca, "import_purchase_records_by_collection"
        ) as mock_task:
            bibliotheca.import_purchase_records_for_all_collections.delay().wait()

        mock_task.delay.assert_not_called()


class TestImportPurchaseRecordsByCollection:
    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_first_run_starts_from_default_start_time(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """On the first run (no prior Timestamp), the task starts from 2014-01-01."""
        mock_api = mock_api_cls.return_value
        mock_api.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        # Stamp a timestamp so we can check the start passed to marc_request.
        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id
                ).wait()

        call_args = mock_api.marc_request.call_args_list[0]
        slice_start, _ = call_args.args[:2]
        expected_start = datetime_utc(2014, 1, 1)
        assert abs((slice_start - expected_start).total_seconds()) < 1

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_starts_from_stored_timestamp(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The task starts from timestamp.finish when a prior Timestamp exists."""
        mock_api = mock_api_cls.return_value
        mock_api.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        stored_finish = datetime_utc(2024, 3, 10)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stored_finish
        )

        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id
                ).wait()

        call_args = mock_api.marc_request.call_args_list[0]
        slice_start, _ = call_args.args[:2]
        assert abs((slice_start - stored_finish).total_seconds()) < 1

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_already_up_to_date(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When the stored Timestamp is current (finish >= now), no API call is made."""
        collection = bibliotheca_purchase_record_task_fixture.collection
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=utc_now() + timedelta(minutes=10)
        )

        bibliotheca.import_purchase_records_by_collection.delay(
            collection_id=collection.id
        ).wait()

        mock_api_cls.return_value.marc_request.assert_not_called()

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_replaces_when_more_days_remain(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When current_day + 1 day is still behind cutoff, task.replace() is raised
        with the next day."""
        mock_api_cls.return_value.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        # Stamp a timestamp several days in the past so multiple days are needed.
        stored_finish = utc_now() - timedelta(days=5)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stored_finish
        )

        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        expected_next_day = stored_finish + timedelta(days=1)
        assert (
            abs((replace_sig.kwargs["current_day"] - expected_next_day).total_seconds())
            < 5
        )
        assert replace_sig.kwargs["offset"] == 1

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaPurchaseRecordImporter.import_day"
    )
    def test_replaces_with_next_offset_when_page_full(
        self,
        mock_import_day: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When import_day signals a full page (next_offset set), task.replace() is
        called with the same current_day and the advanced offset rather than
        advancing to the next day."""
        collection = bibliotheca_purchase_record_task_fixture.collection

        stored_finish = utc_now() - timedelta(days=5)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stored_finish
        )

        next_day = stored_finish + timedelta(days=1)
        mock_import_day.return_value = DayImportResult(
            records_fetched=_MARC_PAGE_SIZE,
            day_start=stored_finish,
            day_end=next_day,
            next_offset=1 + _MARC_PAGE_SIZE,
        )

        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                    offset=1,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        # Same day, offset advanced by _MARC_PAGE_SIZE.
        assert (
            abs((replace_sig.kwargs["current_day"] - stored_finish).total_seconds()) < 5
        )
        assert replace_sig.kwargs["offset"] == 1 + _MARC_PAGE_SIZE

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_no_replace_on_last_day(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When current_day + 1 day >= cutoff, task.replace() is not called."""
        mock_api_cls.return_value.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        # Timestamp just a few hours old — fits inside one day.
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=utc_now() - timedelta(hours=3)
        )

        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            bibliotheca.import_purchase_records_by_collection.delay(
                collection_id=collection.id
            ).wait()

        mock_replace.assert_not_called()

    def test_skips_when_lock_held(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the purchase record workflow lock is already held, the task logs a warning
        and returns without making any API calls."""
        collection = bibliotheca_purchase_record_task_fixture.collection

        existing_lock = _purchase_record_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        existing_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
        ) as mock_api_cls:
            bibliotheca.import_purchase_records_by_collection.delay(
                collection_id=collection.id
            ).wait()
            mock_api_cls.return_value.marc_request.assert_not_called()

        assert "skipped" in caplog.text
        assert "already in progress" in caplog.text

        existing_lock.release()

    def test_lock_not_released_on_autoretry(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """A retryable failure holds the workflow lock and each retry re-runs the import.

        The workflow lock is keyed on ``task.request.id``, which Celery preserves across
        retries, so every retry re-acquires the same lock and re-runs the day rather than
        skipping as if another run were in progress. The lock stays held so no concurrent
        run can start.
        """
        collection = bibliotheca_purchase_record_task_fixture.collection
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=utc_now() - timedelta(days=5)
        )

        mock_response = MockRequestsResponse(500, content="Internal Server Error")

        with patch(
            "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
        ) as mock_api_cls:
            mock_api_cls.return_value.marc_request.side_effect = BadResponseException(
                "http://test.com", "Bad response", mock_response
            )

            with celery_fixture.patch_retry_backoff():
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id
                ).get(propagate=False)

            # The day was re-run on every retry (1 initial attempt + max_retries=4),
            # not skipped as an "already in progress" run.
            assert mock_api_cls.return_value.marc_request.call_count == 5

        # Lock should still be held after retries exhaust — it will expire via
        # the 2-hour Redis TTL, preventing a concurrent run from starting.
        workflow_lock = _purchase_record_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

    def test_remote_initiated_server_error_retried_and_expected(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """marc_request raises RemoteInitiatedServerError for transient Bibliotheca-side
        failures (non-200 body, malformed/"Unknown error" document). It must be retried
        and declared expected rather than surfacing as an unhandled task exception.

        RemoteInitiatedServerError is a sibling of RemoteIntegrationException (both derive
        from IntegrationException), not a subclass, so it has to be listed explicitly in
        autoretry_for and throws. This guards against either being dropped.
        """
        task = bibliotheca.import_purchase_records_by_collection
        assert RemoteInitiatedServerError in task.autoretry_for
        assert RemoteInitiatedServerError in task.throws

        collection = bibliotheca_purchase_record_task_fixture.collection
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=utc_now() - timedelta(days=5)
        )

        with patch(
            "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
        ) as mock_api_cls:
            mock_api_cls.return_value.marc_request.side_effect = (
                RemoteInitiatedServerError("boom", BibliothecaAPI.SERVICE_NAME)
            )

            with celery_fixture.patch_retry_backoff():
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id
                ).get(propagate=False)

            # Retried on every attempt (1 initial + max_retries=4) rather than
            # failing immediately as an unhandled exception.
            assert mock_api_cls.return_value.marc_request.call_count == 5

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_timestamp_updated_after_each_day(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The Timestamp.finish is advanced by one day after each task invocation,
        enabling crash recovery without re-processing old records."""
        mock_api_cls.return_value.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        stored_finish = datetime_utc(2024, 3, 10)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stored_finish
        )

        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                ).wait()

        ts = bibliotheca_purchase_record_task_fixture.get_purchase_record_timestamp()
        assert ts is not None
        assert ts.finish is not None
        expected_finish = stored_finish + timedelta(days=1)
        assert abs((ts.finish - expected_finish).total_seconds()) < 5

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_force_reimport_clears_timestamp_before_import(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When reset_timestamp=True, Timestamp.finish is cleared before get_start() is
        called, so the import starts from DEFAULT_PURCHASE_RECORD_START_TIME rather than
        the stale stored finish date."""
        mock_api_cls.return_value.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        # Stamp a finish date far in the past that would be returned by get_start() if
        # the timestamp were not cleared.
        stale_finish = datetime_utc(2024, 3, 10)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stale_finish
        )

        # Stop after the first day (mock replace so we can inspect what was stamped).
        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                    reset_timestamp=True,
                ).wait()

        # After clearing, get_start() returns DEFAULT_PURCHASE_RECORD_START_TIME
        # (2014-01-01), so import_day stamps finish = 2014-01-02 (one day later).
        # If the clear did not happen, finish would be 2024-03-11 instead.
        ts = bibliotheca_purchase_record_task_fixture.get_purchase_record_timestamp()
        assert ts is not None
        expected_finish = DEFAULT_PURCHASE_RECORD_START_TIME + timedelta(days=1)
        assert ts.finish == expected_finish

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_reset_timestamp_not_forwarded_to_replacement(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """reset_timestamp=True is not forwarded to replacement tasks; it only
        applies to the first invocation."""
        mock_api_cls.return_value.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=utc_now() - timedelta(days=5)
        )

        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                    reset_timestamp=True,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        assert replace_sig.kwargs.get("reset_timestamp") is not True

    def test_chain_runs_through_multiple_days_end_to_end(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The replace chain actually processes multiple days without mocking task.replace().

        Three days of backlog with empty pages forces at least 3 marc_request calls
        and the Timestamp advancing at least 3 days past the stored finish, verifying
        that the chain continuation logic (not just the first invocation) is wired
        up correctly end-to-end.
        """
        collection = bibliotheca_purchase_record_task_fixture.collection
        db = bibliotheca_purchase_record_task_fixture.db

        stored_finish = utc_now() - timedelta(days=3)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stored_finish
        )

        with patch(
            "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
        ) as mock_api_cls:
            # Return a fresh empty iterator on every call so each day's page
            # returns 0 records (day_complete=True, no pagination within a day).
            mock_api_cls.return_value.marc_request.side_effect = lambda *a, **kw: iter(
                []
            )

            # Start the chain. wait() only blocks until the *first* task finishes;
            # replacement tasks are queued and picked up by the worker asynchronously.
            bibliotheca.import_purchase_records_by_collection.delay(
                collection_id=collection.id
            ).wait()

            # Poll until the Timestamp has advanced at least 3 days past the stored
            # finish, or we hit the 10-second timeout.
            deadline = utc_now() + timedelta(seconds=10)
            while utc_now() < deadline:
                db.session.expire_all()
                ts = (
                    bibliotheca_purchase_record_task_fixture.get_purchase_record_timestamp()
                )
                if (
                    ts is not None
                    and ts.finish is not None
                    and ts.finish >= stored_finish + timedelta(days=3)
                ):
                    break
                time.sleep(0.1)

        ts = bibliotheca_purchase_record_task_fixture.get_purchase_record_timestamp()
        assert ts is not None
        assert ts.finish is not None
        assert ts.finish >= stored_finish + timedelta(days=3)
        # At least one marc_request call per day of backlog.
        assert mock_api_cls.return_value.marc_request.call_count >= 3

    def test_stops_chain_gracefully_when_collection_deleted(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the collection is deleted between chain invocations, the task logs a
        warning and returns without raising, stopping the chain cleanly."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        collection_id = collection.id

        # Delete the collection so load_from_id will raise ModelNotFoundError.
        db.session.delete(collection)
        db.session.commit()

        caplog.set_level(LogLevel.warning)

        bibliotheca.import_purchase_records_by_collection.delay(
            collection_id=collection_id,
        ).wait()

        assert "not found" in caplog.text
        assert "deleted" in caplog.text
        assert str(collection_id) in caplog.text

    def test_stops_chain_gracefully_when_collection_marked_for_deletion(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the collection is marked for deletion, the task logs a warning and
        returns without making any API calls, stopping the chain cleanly."""
        collection = bibliotheca_purchase_record_task_fixture.collection
        collection.marked_for_deletion = True

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
        ) as mock_api_cls:
            bibliotheca.import_purchase_records_by_collection.delay(
                collection_id=collection.id,
            ).wait()
            mock_api_cls.return_value.marc_request.assert_not_called()

        assert "marked for deletion" in caplog.text
        assert collection.name in caplog.text

    def test_purchase_record_lock_independent_from_import_lock(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The purchase record workflow lock uses a different Redis key than the event
        import workflow lock, so the two can run concurrently per collection."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )

        # Hold the event import lock.
        event_lock = import_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        event_lock.acquire()

        # The purchase lock for the same collection should still be acquirable.
        purchase_record_lock = _purchase_record_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        acquired = purchase_record_lock.acquire()
        assert acquired

        purchase_record_lock.release()
        event_lock.release()


class BibliothecaCirculationUpdateTaskFixture:
    """Common setup for Bibliotheca circulation update Celery task tests."""

    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )

    def stamp_circulation_update(
        self, counter: int | None = None, collection: Collection | None = None
    ) -> Timestamp:
        """Create (or update) the circulation update Timestamp for the collection."""
        return Timestamp.stamp(
            self.db.session,
            service=CIRCULATION_UPDATE_SERVICE_NAME,
            service_type=Timestamp.TASK_TYPE,
            collection=collection or self.collection,
            counter=counter,
        )

    def get_circulation_update_timestamp(
        self, collection: Collection | None = None
    ) -> Timestamp | None:
        return Timestamp.lookup(
            self.db.session,
            CIRCULATION_UPDATE_SERVICE_NAME,
            Timestamp.TASK_TYPE,
            collection or self.collection,
        )


@pytest.fixture
def bibliotheca_circulation_update_task_fixture(
    db: DatabaseTransactionFixture,
) -> BibliothecaCirculationUpdateTaskFixture:
    return BibliothecaCirculationUpdateTaskFixture(db)


class TestCirculationUpdateAllCollections:
    def test_queues_circulation_update_collection_for_each_bibliotheca_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """circulation_update_all_collections queues circulation_update_collection for every
        Bibliotheca collection and ignores collections using other protocols."""
        db.default_collection()  # non-Bibliotheca, should be ignored
        c1 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 1"
        )
        c2 = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library(), name="Bibliotheca 2"
        )

        with patch.object(bibliotheca, "circulation_update_collection") as mock_task:
            bibliotheca.circulation_update_all_collections.delay().wait()

        mock_task.delay.assert_has_calls(
            [call(collection_id=c1.id), call(collection_id=c2.id)],
            any_order=True,
        )
        assert mock_task.delay.call_count == 2

    def test_no_bibliotheca_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """circulation_update_all_collections is a no-op when there are no Bibliotheca collections."""
        db.default_collection()  # non-Bibliotheca

        with patch.object(bibliotheca, "circulation_update_collection") as mock_task:
            bibliotheca.circulation_update_all_collections.delay().wait()

        mock_task.delay.assert_not_called()


class TestCirculationUpdateCollection:
    def test_first_invocation_reads_offset_from_timestamp(
        self,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """On the first invocation (offset=None), the offset is read from the stored Timestamp.counter."""
        collection = bibliotheca_circulation_update_task_fixture.collection

        # Set a counter so we can verify it was used as the offset.
        bibliotheca_circulation_update_task_fixture.stamp_circulation_update(counter=99)

        mock_result = BatchUpdateResult(records_handled=0, next_offset=None)

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
        ) as mock_updater_cls:
            mock_updater = mock_updater_cls.return_value
            mock_updater.get_offset.return_value = 99
            mock_updater.update_batch.return_value = mock_result

            bibliotheca.circulation_update_collection.delay(
                collection_id=collection.id
            ).wait()

        # Verify that update_batch was called with the offset from the stored timestamp.
        mock_updater.update_batch.assert_called_once_with(99)

    @patch(
        "palace.manager.integration.license.bibliotheca_circulation_updater.BibliothecaAPI"
    )
    def test_already_complete_no_replace(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When the sweep is complete (empty batch), no replace is issued."""
        mock_api_cls.return_value.bibliographic_lookup.return_value = iter([])
        collection = bibliotheca_circulation_update_task_fixture.collection

        # Offset 0, no identifiers in collection → partial batch → no replace.
        with patch.object(
            bibliotheca.circulation_update_collection, "replace"
        ) as mock_replace:
            bibliotheca.circulation_update_collection.delay(
                collection_id=collection.id
            ).wait()

        mock_replace.assert_not_called()

    @patch(
        "palace.manager.integration.license.bibliotheca_circulation_updater.BibliothecaAPI"
    )
    def test_full_batch_issues_replace_with_next_offset(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When update_batch returns a full batch (next_offset set), task.replace() is
        raised with the next offset."""
        collection = bibliotheca_circulation_update_task_fixture.collection

        mock_result = BatchUpdateResult(records_handled=25, next_offset=100)

        with (
            patch(
                "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
            ) as mock_updater_cls,
            patch.object(
                bibliotheca.circulation_update_collection, "replace"
            ) as mock_replace,
        ):
            mock_updater = mock_updater_cls.return_value
            mock_updater.get_offset.return_value = 0
            mock_updater.update_batch.return_value = mock_result
            mock_replace.side_effect = Exception("replaced")

            with pytest.raises(Exception, match="replaced"):
                bibliotheca.circulation_update_collection.delay(
                    collection_id=collection.id,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        assert replace_sig.kwargs["offset"] == 100

    @patch(
        "palace.manager.integration.license.bibliotheca_circulation_updater.BibliothecaAPI"
    )
    def test_partial_batch_no_replace(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When update_batch signals completion (next_offset=None), no replace is issued."""
        collection = bibliotheca_circulation_update_task_fixture.collection

        mock_result = BatchUpdateResult(records_handled=3, next_offset=None)

        with (
            patch(
                "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
            ) as mock_updater_cls,
            patch.object(
                bibliotheca.circulation_update_collection, "replace"
            ) as mock_replace,
        ):
            mock_updater = mock_updater_cls.return_value
            mock_updater.get_offset.return_value = 0
            mock_updater.update_batch.return_value = mock_result

            bibliotheca.circulation_update_collection.delay(
                collection_id=collection.id,
            ).wait()

        mock_replace.assert_not_called()

    @patch(
        "palace.manager.integration.license.bibliotheca_circulation_updater.BibliothecaAPI"
    )
    def test_replace_carries_collection_id(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The replace signature carries the collection_id forward via signature_with."""
        collection = bibliotheca_circulation_update_task_fixture.collection

        mock_result = BatchUpdateResult(records_handled=25, next_offset=100)

        with (
            patch(
                "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
            ) as mock_updater_cls,
            patch.object(
                bibliotheca.circulation_update_collection, "replace"
            ) as mock_replace,
        ):
            mock_updater = mock_updater_cls.return_value
            mock_updater.get_offset.return_value = 0
            mock_updater.update_batch.return_value = mock_result
            mock_replace.side_effect = Exception("replaced")

            with pytest.raises(Exception, match="replaced"):
                bibliotheca.circulation_update_collection.delay(
                    collection_id=collection.id,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        assert replace_sig.kwargs["collection_id"] == collection.id
        assert replace_sig.kwargs["offset"] == 100

    def test_skips_when_lock_held(
        self,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the workflow lock is already held by another run, the task logs a warning
        and returns without processing."""
        collection = bibliotheca_circulation_update_task_fixture.collection

        existing_lock = _circulation_update_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        existing_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
        ) as mock_updater_cls:
            bibliotheca.circulation_update_collection.delay(
                collection_id=collection.id
            ).wait()
            mock_updater_cls.return_value.update_batch.assert_not_called()

        assert "skipped" in caplog.text
        assert "already in progress" in caplog.text

        existing_lock.release()

    def test_lock_not_released_on_autoretry(
        self,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """A retryable failure holds the workflow lock and each retry re-runs the batch.

        The workflow lock is keyed on ``task.request.id``, which Celery preserves across
        retries, so every retry re-acquires the same lock and re-runs the batch rather
        than skipping as if another run were in progress. The lock stays held so no
        concurrent run can start.
        """
        collection = bibliotheca_circulation_update_task_fixture.collection

        mock_response = MockRequestsResponse(500, content="Internal Server Error")

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
        ) as mock_updater_cls:
            mock_updater = mock_updater_cls.return_value
            mock_updater.get_offset.return_value = 0
            mock_updater.update_batch.side_effect = BadResponseException(
                "http://test.com", "Bad response", mock_response
            )

            with celery_fixture.patch_retry_backoff():
                bibliotheca.circulation_update_collection.delay(
                    collection_id=collection.id
                ).get(propagate=False)

            # The batch was re-run on every retry (1 initial attempt + max_retries=4),
            # not skipped as an "already in progress" run.
            assert mock_updater.update_batch.call_count == 5

        # Lock should still be held after retries exhaust — it will expire via
        # the 2-hour Redis TTL, preventing a concurrent run from starting.
        workflow_lock = _circulation_update_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

    def test_remote_initiated_server_error_retried_and_expected(
        self,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """bibliographic_lookup raises RemoteInitiatedServerError when Bibliotheca
        returns an empty response body — a transient Bibliotheca-side condition. It
        must be retried and declared expected rather than zeroing out the batch's
        availability or surfacing as an unhandled task exception.

        RemoteInitiatedServerError is a sibling of RemoteIntegrationException (both
        derive from IntegrationException), not a subclass, so it has to be listed
        explicitly in autoretry_for and throws. This guards against either being dropped.
        """
        task = bibliotheca.circulation_update_collection
        assert RemoteInitiatedServerError in task.autoretry_for
        assert RemoteInitiatedServerError in task.throws

        collection = bibliotheca_circulation_update_task_fixture.collection

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
        ) as mock_updater_cls:
            mock_updater = mock_updater_cls.return_value
            mock_updater.get_offset.return_value = 0
            mock_updater.update_batch.side_effect = RemoteInitiatedServerError(
                "boom", BibliothecaAPI.SERVICE_NAME
            )

            with celery_fixture.patch_retry_backoff():
                bibliotheca.circulation_update_collection.delay(
                    collection_id=collection.id
                ).get(propagate=False)

            # Retried on every attempt (1 initial + max_retries=4) rather than
            # failing immediately as an unhandled exception.
            assert mock_updater.update_batch.call_count == 5

    def test_circulation_update_lock_independent_from_event_import_and_purchase_record_locks(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The circulation update workflow lock uses a different Redis key than both the
        event import and purchase record locks, so all three can run concurrently per collection.
        """
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )

        # Hold the event import lock.
        event_lock = import_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        event_lock.acquire()

        # Hold the purchase record lock.
        purchase_record_lock = _purchase_record_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        purchase_record_lock.acquire()

        # The circulation update lock should still be acquirable.
        circulation_lock = _circulation_update_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        acquired = circulation_lock.acquire()
        assert acquired

        circulation_lock.release()
        purchase_record_lock.release()
        event_lock.release()

    def test_stops_chain_gracefully_when_collection_deleted(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the collection is deleted between chain invocations, the task logs a
        warning and returns without raising, stopping the chain cleanly."""
        collection = MockBibliothecaAPI.mock_collection(
            db.session, db.default_library()
        )
        collection_id = collection.id

        db.session.delete(collection)
        db.session.commit()

        caplog.set_level(LogLevel.warning)

        bibliotheca.circulation_update_collection.delay(
            collection_id=collection_id,
        ).wait()

        assert "not found" in caplog.text
        assert "deleted" in caplog.text
        assert str(collection_id) in caplog.text

    def test_stops_chain_gracefully_when_collection_marked_for_deletion(
        self,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the collection is marked for deletion, the task logs a warning and
        returns without making any API calls, stopping the chain cleanly."""
        collection = bibliotheca_circulation_update_task_fixture.collection
        collection.marked_for_deletion = True

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaCirculationUpdater"
        ) as mock_updater_cls:
            bibliotheca.circulation_update_collection.delay(
                collection_id=collection.id,
            ).wait()
            mock_updater_cls.return_value.update_batch.assert_not_called()

        assert "marked for deletion" in caplog.text
        assert collection.name in caplog.text

    @patch(
        "palace.manager.integration.license.bibliotheca_circulation_updater.BibliothecaAPI"
    )
    def test_full_sweep_chains_through_every_batch_and_releases_lock(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_circulation_update_task_fixture: BibliothecaCirculationUpdateTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """End-to-end: a multi-batch sweep chains through every batch via a real
        ``task.replace()`` until completion, then releases the workflow lock.

        This complements the unit-level lock coverage
        (``test_lock_not_released_on_autoretry``,
        ``RedisLock`` ``test_lock_not_released_on_ignored_exception``, and
        ``workflow_lock_guard`` ``test_continuation_reacquires_own_lock``): here a real
        chain runs the full sweep, proving the batches advance to completion under a
        single lock that is held across the ``replace()`` hand-off and released at the end.
        """
        fixture = bibliotheca_circulation_update_task_fixture
        collection = fixture.collection
        db = fixture.db

        # Bibliotheca recognises nothing, so each batch simply iterates its identifiers
        # without queuing applies — we only care about chaining and lock lifecycle here.
        mock_api_cls.return_value.bibliographic_lookup.return_value = []

        # One more identifier than a single batch, so the sweep needs two batches:
        # batch 1 = CIRCULATION_UPDATE_BATCH_SIZE (full -> replace), batch 2 = 1
        # (partial -> complete).
        data_source = MockBibliothecaAPI(db.session, collection).data_source
        for i in range(CIRCULATION_UPDATE_BATCH_SIZE + 1):
            identifier = db.identifier(
                identifier_type=Identifier.BIBLIOTHECA_ID, foreign_id=f"chain{i:04d}"
            )
            LicensePool.for_foreign_id(
                db.session,
                data_source,
                Identifier.BIBLIOTHECA_ID,
                identifier.identifier,
                collection=collection,
            )
        db.session.commit()

        # Run the chain for real — replace is NOT mocked, so the worker processes every
        # batch in sequence under one preserved task id.
        bibliotheca.circulation_update_collection.delay(
            collection_id=collection.id
        ).wait()

        # bibliographic_lookup was called once per batch: the chain ran both batches.
        assert mock_api_cls.return_value.bibliographic_lookup.call_count == 2

        # The sweep completed: counter reset to 0 and finish stamped.
        ts = fixture.get_circulation_update_timestamp()
        assert ts is not None
        assert ts.counter == 0
        assert ts.finish is not None

        # The workflow lock was held across the replace hand-off and released on normal
        # completion, so no lock remains to block the next scheduled sweep.
        lock = _circulation_update_workflow_lock(
            redis_fixture.client, collection.id, random_value="probe"
        )
        assert not lock.locked()
