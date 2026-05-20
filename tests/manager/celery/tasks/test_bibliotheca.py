"""Tests for Bibliotheca Celery tasks."""

from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch
from uuid import uuid4

import pytest

from palace.util.datetime_helpers import utc_now
from palace.util.log import LogLevel

from palace.manager.celery.tasks import apply, bibliotheca
from palace.manager.celery.tasks.bibliotheca import (
    EVENT_IMPORT_SERVICE_NAME,
    _event_import_workflow_lock,
)
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import BibliothecaFilesFixture
from tests.fixtures.redis import RedisFixture
from tests.mocks.bibliotheca import MockBibliothecaAPI
from tests.mocks.mock import MockRequestsResponse


class BibliothecaTaskFixture:
    """Common setup for Bibliotheca Celery task tests."""

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        files: BibliothecaFilesFixture,
    ) -> None:
        self.db = db
        self.files = files
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
            service_type=Timestamp.MONITOR_TYPE,
            collection=collection or self.collection,
            finish=finish or utc_now(),
        )

    def get_event_import_timestamp(
        self, collection: Collection | None = None
    ) -> Timestamp | None:
        return Timestamp.lookup(
            self.db.session,
            EVENT_IMPORT_SERVICE_NAME,
            Timestamp.MONITOR_TYPE,
            collection or self.collection,
        )


@pytest.fixture
def bibliotheca_task_fixture(
    db: DatabaseTransactionFixture,
    bibliotheca_files_fixture: BibliothecaFilesFixture,
) -> BibliothecaTaskFixture:
    return BibliothecaTaskFixture(db, bibliotheca_files_fixture)


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
    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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

    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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

    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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

    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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

    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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

    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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
        assert isinstance(lock_value, str)

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
        workflow_lock = _event_import_workflow_lock(
            redis_fixture.client, collection.id, existing_lock_value
        )
        workflow_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaAPI"
        ) as mock_api_cls:
            bibliotheca.import_collection.delay(collection_id=collection.id).wait()
            mock_api_cls.return_value.get_events_between.assert_not_called()

        assert "skipped" in caplog.text
        assert "already in progress" in caplog.text

        workflow_lock.release()

    def test_lock_released_on_autoretry(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """When a retryable exception is raised the workflow lock is released so that
        subsequent retries (and the next beat tick) can re-acquire it and resume from
        the last committed Timestamp position."""
        collection = bibliotheca_task_fixture.collection
        bibliotheca_task_fixture.stamp_event_import(
            finish=utc_now() - timedelta(minutes=10)
        )

        mock_response = MockRequestsResponse(500, content="Internal Server Error")

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaAPI"
        ) as mock_api_cls:
            mock_api_cls.return_value.get_events_between.side_effect = (
                BadResponseException("http://test.com", "Bad response", mock_response)
            )

            with celery_fixture.patch_retry_backoff():
                bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        # Lock should be free after retries exhaust so the next run is not blocked.
        workflow_lock = _event_import_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert not workflow_lock.locked()

    def test_events_processed_and_timestamp_updated(
        self,
        bibliotheca_task_fixture: BibliothecaTaskFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """Integration: events from the API are processed and create LicensePools;
        the Timestamp is updated; and a bibliographic_apply task is queued when the
        content hash indicates the metadata has changed."""
        from datetime import timezone

        from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent

        collection = bibliotheca_task_fixture.collection

        # Stamp a timestamp 10 minutes ago so there is one slice to process.
        ten_minutes_ago = utc_now() - timedelta(minutes=10)
        bibliotheca_task_fixture.stamp_event_import(finish=ten_minutes_ago)

        # Fake one PURCHASE event for item 'd5rf89'; PURCHASE → DISTRIBUTOR_LICENSE_ADD.
        event_time = datetime(2016, 4, 28, 11, 4, 6, tzinfo=timezone.utc)
        fake_event = (
            "d5rf89",
            "9781101190623",
            None,
            event_time,
            None,
            CirculationEvent.DISTRIBUTOR_LICENSE_ADD,
        )

        # Return a fake BibliographicData that always reports needs_apply=True so
        # the apply task is queued without making a real HTTP call.
        mock_bib = MagicMock()
        mock_bib.needs_apply.return_value = True

        with (
            # Patch at the class level so the real BibliothecaAPI constructor runs.
            patch.object(
                BibliothecaAPI, "get_events_between", return_value=iter([fake_event])
            ),
            patch.object(
                BibliothecaAPI, "bibliographic_lookup", return_value=[mock_bib]
            ),
            patch.object(apply, "bibliographic_apply") as mock_apply,
        ):
            bibliotheca.import_collection.delay(collection_id=collection.id).wait()

        # A LicensePool should exist for the event's item identifier.
        pools = [
            lp for lp in collection.licensepools if lp.identifier.identifier == "d5rf89"
        ]
        assert len(pools) == 1

        # bibliographic_apply should have been queued once (for the single event).
        mock_apply.delay.assert_called_once()

        # Timestamp should have been updated.
        ts = bibliotheca_task_fixture.get_event_import_timestamp()
        assert ts is not None
        assert ts.finish is not None
        assert ts.finish > ten_minutes_ago

    @patch("palace.manager.celery.tasks.bibliotheca.BibliothecaAPI")
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
        lock_c1 = _event_import_workflow_lock(redis_fixture.client, c1.id, str(uuid4()))
        lock_c1.acquire()

        with patch(
            "palace.manager.celery.tasks.bibliotheca.BibliothecaAPI"
        ) as mock_api_cls2:
            mock_api_cls2.return_value.get_events_between.return_value = iter([])
            # Collection 2 should process normally.
            bibliotheca.import_collection.delay(collection_id=c2.id).wait()
            # Collection 1 should be skipped (lock held).
            bibliotheca.import_collection.delay(collection_id=c1.id).wait()

            assert mock_api_cls2.return_value.get_events_between.call_count == 1

        lock_c1.release()
