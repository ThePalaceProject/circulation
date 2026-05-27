"""Tests for Bibliotheca Celery tasks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, call, patch
from uuid import UUID, uuid4

import pytest

from palace.util.datetime_helpers import datetime_utc, utc_now
from palace.util.log import LogLevel

from palace.manager.celery.importer import import_workflow_lock
from palace.manager.celery.tasks import apply, bibliotheca
from palace.manager.celery.tasks.bibliotheca import _purchase_record_workflow_lock
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
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
        lock_value = str(uuid4())

        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(
                    collection_id=collection.id,
                    start=explicit_start,
                    lock_value=lock_value,
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
        with the next slice's start and the same lock_value."""
        mock_api_cls.return_value.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        # Stamp an old timestamp so several slices are needed.
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(hours=1)
        )

        lock_value = str(uuid4())
        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(
                    collection_id=collection.id,
                    lock_value=lock_value,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        assert replace_sig.kwargs["lock_value"] == lock_value
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

    @patch("palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI")
    def test_lock_value_passed_through_on_replace(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The lock_value generated on the first slice is forwarded unchanged to the
        next slice so the workflow lock remains held across task.replace() calls."""
        mock_api_cls.return_value.get_events_between.return_value = iter([])
        collection = bibliotheca_task_fixture.collection

        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(hours=1)
        )

        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                # First invocation: no lock_value (will be generated internally).
                bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        replace_sig = mock_replace.call_args[0][0]
        lock_value = replace_sig.kwargs["lock_value"]
        assert lock_value is not None
        UUID(lock_value)  # raises ValueError if not a valid UUID

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

    def test_continues_with_warning_when_lock_expires_mid_chain(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the workflow lock expires between slices (is_first_slice=False and lock
        not acquired), the task logs a warning but still processes the slice rather than
        silently returning."""
        collection = bibliotheca_task_fixture.collection
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(minutes=3)
        )

        # Simulate the lock having expired and been re-acquired by a competing run.
        # A free lock would be acquired successfully; we need another holder so that our
        # task's lock_value (a different UUID) fails to acquire.
        competing_lock = import_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        competing_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.integration.license.bibliotheca_importer.BibliothecaAPI"
        ) as mock_api_cls:
            mock_api_cls.return_value.get_events_between.return_value = iter([])
            # Pass a lock_value that does not match the competing lock so acquire fails,
            # but is_first_slice is False (lock_value is not None).
            bibliotheca.import_collection.delay(
                collection_id=collection.id,
                lock_value=str(uuid4()),
            ).wait()
            # Despite the lock not being acquired, the slice was still processed.
            mock_api_cls.return_value.get_events_between.assert_called_once()

        assert "workflow lock expired between slices" in caplog.text

        competing_lock.release()

    def test_lock_not_released_on_autoretry(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When a retryable exception is raised the workflow lock is held so that
        a concurrent run cannot start on the same collection while retries are
        in progress."""
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

        # Lock should still be held after retries exhaust — it will expire via
        # the 2-hour Redis TTL, preventing a concurrent run from starting.
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

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

        lock_value = str(uuid4())
        with patch.object(bibliotheca.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_collection.delay(
                    collection_id=collection.id,
                    lock_value=lock_value,
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
                call(collection_id=c1.id, current_day=None),
                call(collection_id=c2.id, current_day=None),
            ],
            any_order=True,
        )
        assert mock_task.delay.call_count == 2

    def test_force_reimport_passes_start_date_to_each_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """force_reimport=True passes current_day=DEFAULT_PURCHASE_RECORD_START_TIME to each per-collection task."""
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
                ),
                call(
                    collection_id=c2.id,
                    current_day=DEFAULT_PURCHASE_RECORD_START_TIME,
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
        with the next day and the same lock_value."""
        mock_api_cls.return_value.marc_request.return_value = iter([])
        collection = bibliotheca_purchase_record_task_fixture.collection

        # Stamp a timestamp several days in the past so multiple days are needed.
        stored_finish = utc_now() - timedelta(days=5)
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=stored_finish
        )

        lock_value = str(uuid4())
        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                    lock_value=lock_value,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        assert replace_sig.kwargs["lock_value"] == lock_value
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
            records_handled=_MARC_PAGE_SIZE,
            day_start=stored_finish,
            day_end=next_day,
            next_offset=1 + _MARC_PAGE_SIZE,
        )

        lock_value = str(uuid4())
        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                    offset=1,
                    lock_value=lock_value,
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        # Same day, offset advanced by _MARC_PAGE_SIZE.
        assert (
            abs((replace_sig.kwargs["current_day"] - stored_finish).total_seconds()) < 5
        )
        assert replace_sig.kwargs["offset"] == 1 + _MARC_PAGE_SIZE
        assert replace_sig.kwargs["lock_value"] == lock_value

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

    @patch(
        "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
    )
    def test_lock_value_passed_through_on_replace(
        self,
        mock_api_cls: MagicMock,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """The lock_value generated on the first day is forwarded unchanged to the
        next day so the workflow lock remains held across task.replace() calls."""
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
                    collection_id=collection.id
                ).wait()

        replace_sig = mock_replace.call_args[0][0]
        lock_value = replace_sig.kwargs["lock_value"]
        assert lock_value is not None
        UUID(lock_value)  # raises ValueError if not a valid UUID

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

    def test_continues_with_warning_when_lock_expires_mid_chain(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """When the purchase record workflow lock expires mid-chain (is_first_invocation=False
        and lock not acquired), the task logs a warning but still processes the day."""
        collection = bibliotheca_purchase_record_task_fixture.collection
        bibliotheca_purchase_record_task_fixture.stamp_purchase_record(
            finish=utc_now() - timedelta(hours=3)
        )

        competing_lock = _purchase_record_workflow_lock(
            redis_fixture.client, collection.id, str(uuid4())
        )
        competing_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.integration.license.bibliotheca_purchase_record_importer.BibliothecaAPI"
        ) as mock_api_cls:
            mock_api_cls.return_value.marc_request.return_value = iter([])
            bibliotheca.import_purchase_records_by_collection.delay(
                collection_id=collection.id,
                lock_value=str(uuid4()),
            ).wait()
            mock_api_cls.return_value.marc_request.assert_called_once()

        assert "workflow lock expired between invocations" in caplog.text

        competing_lock.release()

    def test_lock_not_released_on_autoretry(
        self,
        bibliotheca_purchase_record_task_fixture: BibliothecaPurchaseRecordTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When a retryable exception is raised the purchase record workflow lock is held so
        that a concurrent run cannot start on the same collection while retries are
        in progress."""
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

        workflow_lock = _purchase_record_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

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

        lock_value = str(uuid4())
        with patch.object(
            bibliotheca.import_purchase_records_by_collection, "replace"
        ) as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                bibliotheca.import_purchase_records_by_collection.delay(
                    collection_id=collection.id,
                    lock_value=lock_value,
                ).wait()

        ts = bibliotheca_purchase_record_task_fixture.get_purchase_record_timestamp()
        assert ts is not None
        assert ts.finish is not None
        expected_finish = stored_finish + timedelta(days=1)
        assert abs((ts.finish - expected_finish).total_seconds()) < 5

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
