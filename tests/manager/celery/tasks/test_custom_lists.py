"""Tests for the custom list Celery tasks."""

from __future__ import annotations

import datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from opensearchpy import RequestError
from opensearchpy.exceptions import TransportError
from sqlalchemy.exc import SQLAlchemyError

from palace.manager.celery.tasks import custom_lists
from palace.manager.celery.tasks.custom_lists import (
    _entry_update_lock,
    _sweep_lock,
)
from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_auto_updating_list(
    db: DatabaseTransactionFixture,
    status: str = CustomList.INIT,
    query: str | None = '{"query": {"key": "genre", "op": "eq", "value": "Fantasy"}}',
    last_update: datetime.datetime | None = None,
) -> CustomList:
    """Create an auto-updating CustomList with sensible defaults."""
    custom_list, _ = db.customlist(num_entries=0)
    custom_list.auto_update_enabled = True
    custom_list.auto_update_status = status
    custom_list.auto_update_query = query
    if last_update is not None:
        custom_list.auto_update_last_update = last_update
    return custom_list


# ---------------------------------------------------------------------------
# Stage 0 — Sweep orchestrator
# ---------------------------------------------------------------------------


class TestUpdateCustomListEntriesSweep:
    def test_no_auto_updating_lists_finalizes_directly(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """When there are no auto-updating lists, the sweep finalizes directly."""
        with (
            patch.object(
                custom_lists, "finalize_custom_list_entries_sweep"
            ) as mock_finalize,
            patch.object(custom_lists, "update_custom_list_entries") as mock_entries,
        ):
            custom_lists.update_custom_list_entries_sweep.delay().wait()

            mock_entries.si.assert_not_called()
            mock_finalize.delay.assert_called_once()
            # The sweep lock_value must be forwarded so finalize can release it.
            _, kwargs = mock_finalize.delay.call_args
            assert kwargs.get("lock_value") is not None

    def test_fans_out_per_list_tasks(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """Creates one update_custom_list_entries task per auto-updating list."""
        cl1 = _make_auto_updating_list(db)
        cl2 = _make_auto_updating_list(db)
        # A disabled list — should NOT appear in the fan-out.
        disabled_list, _ = db.customlist(num_entries=0)
        disabled_list.auto_update_enabled = False

        with (
            patch.object(custom_lists, "update_custom_list_entries") as mock_entries,
            patch.object(
                custom_lists, "finalize_custom_list_entries_sweep"
            ) as mock_finalize,
        ):
            # Use a mock chord so we can inspect what was assembled.
            with patch("palace.manager.celery.tasks.custom_lists.chord") as mock_chord:
                with patch(
                    "palace.manager.celery.tasks.custom_lists.group"
                ) as mock_group:
                    custom_lists.update_custom_list_entries_sweep.delay().wait()

            # si() called for each enabled list
            si_ids = {c.args[0] for c in mock_entries.si.call_args_list}
            assert cl1.id in si_ids
            assert cl2.id in si_ids
            assert disabled_list.id not in si_ids

    def test_sweep_lock_value_forwarded_to_finalize(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """The sweep lock_value is forwarded to finalize_custom_list_entries_sweep so
        the lock is released there."""
        cl1 = _make_auto_updating_list(db)

        with (
            patch.object(custom_lists, "update_custom_list_entries"),
            patch.object(custom_lists, "finalize_custom_list_entries_sweep"),
            patch("palace.manager.celery.tasks.custom_lists.chord") as mock_chord,
            patch("palace.manager.celery.tasks.custom_lists.group"),
        ):
            custom_lists.update_custom_list_entries_sweep.delay().wait()

        # The chord callback signature must carry lock_value.
        chord_callback = mock_chord.call_args[0][1]
        assert chord_callback.kwargs.get("lock_value") is not None

    def test_sweep_lock_prevents_concurrent_sweeps(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """If the sweep lock is held a second sweep is skipped."""
        lock_value = str(uuid4())
        sweep_lock = _sweep_lock(redis_fixture.client, lock_value)
        sweep_lock.acquire()

        with (
            patch.object(
                custom_lists, "finalize_custom_list_entries_sweep"
            ) as mock_finalize,
            patch.object(custom_lists, "update_custom_list_entries") as mock_entries,
        ):
            custom_lists.update_custom_list_entries_sweep.delay().wait()

            mock_entries.si.assert_not_called()
            mock_finalize.delay.assert_not_called()

        sweep_lock.release()


# ---------------------------------------------------------------------------
# Stage 1 — Per-list entry update
# ---------------------------------------------------------------------------


class TestUpdateCustomListEntries:
    def test_init_mode_runs_full_query(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """In INIT mode, populate_query_pages is called with no time filter."""
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.return_value = (3, None)
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

        mock_queries.populate_query_pages.assert_called_once()
        _, kwargs = mock_queries.populate_query_pages.call_args
        # json_query must be None for INIT (no time filter injected)
        assert kwargs.get("json_query") is None
        assert kwargs.get("update_metadata") is False

    def test_repopulate_mode_clears_entries_first(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """In REPOPULATE mode, all existing entries are deleted before re-populating."""
        custom_list = _make_auto_updating_list(db, status=CustomList.REPOPULATE)
        # Add a few entries so there is something to delete.
        work1 = db.work()
        work2 = db.work()
        custom_list.add_entry(work1.presentation_edition)
        custom_list.add_entry(work2.presentation_edition)
        db.session.flush()
        assert (
            db.session.query(CustomListEntry)
            .filter(CustomListEntry.list_id == custom_list.id)
            .count()
            == 2
        )

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.return_value = (0, None)
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

        # All prior entries should have been bulk-deleted.
        remaining = (
            db.session.query(CustomListEntry)
            .filter(CustomListEntry.list_id == custom_list.id)
            .count()
        )
        assert remaining == 0

    def test_updated_mode_injects_time_filter(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """In UPDATED mode, a licensepools.availability_time >= filter is injected."""
        last_update = datetime.datetime(2024, 1, 1, 12, 0, 0)
        custom_list = _make_auto_updating_list(
            db, status=CustomList.UPDATED, last_update=last_update
        )

        captured_query: dict = {}

        def capture_call(*args, **kwargs):
            captured_query.update(kwargs.get("json_query") or {})
            return (1, None)

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.side_effect = capture_call
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

        # The time filter must be present in the injected query.
        and_clauses = captured_query["query"]["and"]
        time_filter = and_clauses[0]
        assert time_filter["key"] == "licensepools.availability_time"
        assert time_filter["op"] == "gte"
        assert time_filter["value"] == pytest.approx(last_update.timestamp())

    def test_final_batch_updates_metadata(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """When populate_query_pages returns no next cursor, metadata is written."""
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.return_value = (5, None)
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

        db.session.refresh(custom_list)
        assert custom_list.auto_update_status == CustomList.UPDATED
        assert custom_list.auto_update_last_update is not None

    def test_continuation_uses_signature_with(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """When more pages remain, task.replace() uses signature_with to carry args forward."""
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)
        next_key = ["sort_key_value_1", 42]

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.return_value = (5, next_key)
            with patch.object(
                custom_lists.update_custom_list_entries, "replace"
            ) as mock_replace:
                mock_replace.side_effect = Exception("replace called")
                with pytest.raises(Exception, match="replace called"):
                    custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

            replace_sig = mock_replace.call_args[0][0]
            assert replace_sig.kwargs["pagination_key"] == next_key
            assert replace_sig.kwargs["custom_list_id"] == custom_list.id
            # lock_value is no longer an explicit parameter — workflow_lock_guard
            # uses task.request.id as the stable lock identity across pages.
            assert "lock_value" not in replace_sig.kwargs

    def test_continuation_uses_task_id_as_lock_identity(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """Continuation invocations re-acquire the same lock via the stable task ID."""
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)
        next_key = ["sort_key_value"]

        # Simulate a continuation by passing a pagination_key (first-invocation
        # detection uses pagination_key is None, so any non-None value here
        # triggers continuation logic).
        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.return_value = (5, next_key)
            with patch.object(
                custom_lists.update_custom_list_entries, "replace"
            ) as mock_replace:
                mock_replace.side_effect = Exception("replace called")
                with pytest.raises(Exception, match="replace called"):
                    custom_lists.update_custom_list_entries.delay(
                        custom_list.id,
                        pagination_key=["previous_key"],
                    ).wait()

            replace_sig = mock_replace.call_args[0][0]
            # pagination_key updated, custom_list_id carried forward
            assert replace_sig.kwargs["pagination_key"] == next_key
            assert replace_sig.kwargs["custom_list_id"] == custom_list.id

    def test_entry_lock_blocks_duplicate_update(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """If another update holds the per-list lock, the task skips cleanly."""
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)
        lock_value = str(uuid4())
        lock = _entry_update_lock(redis_fixture.client, custom_list.id, lock_value)
        lock.acquire()

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()
            mock_queries.populate_query_pages.assert_not_called()

        lock.release()

    def test_lock_released_after_final_batch(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """After the final batch completes successfully, the lock is released."""
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.return_value = (1, None)
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

        # Lock should be released; a fresh lock with any value can now be acquired.
        fresh_lock = _entry_update_lock(redis_fixture.client, custom_list.id, "any")
        assert not fresh_lock.locked()

    def test_skips_disabled_list(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """If auto_update_enabled is False, the task exits immediately."""
        custom_list, _ = db.customlist(num_entries=0)
        custom_list.auto_update_enabled = False
        custom_list.auto_update_query = "{}"

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()
            mock_queries.populate_query_pages.assert_not_called()

    def test_missing_list_logs_and_continues(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """A nonexistent list ID causes the task to skip without raising."""
        # Use an ID that doesn't exist in the DB.
        nonexistent_id = 999_999_999
        # Should not raise.
        custom_lists.update_custom_list_entries.delay(nonexistent_id).wait()

    def test_malformed_query_logged_and_swallowed(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """A RequestError (this list's query is malformed) is logged and skipped.

        Because this task is a chord header, a propagated error would abort the
        whole sweep chord and hold the sweep lock until its TTL. A malformed
        query is a list-specific config problem, so the task must instead succeed
        (the chord proceeds and the per-list lock is released).
        """
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.side_effect = RequestError(
                400, "parsing_exception", {"error": "malformed query"}
            )
            # Should not raise -- the error is caught, logged, and swallowed.
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

        # The per-list lock must be released so a later sweep can run.
        lock = _entry_update_lock(redis_fixture.client, custom_list.id, str(uuid4()))
        assert lock.acquire() is not False

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(SQLAlchemyError("database is down"), id="database"),
            pytest.param(
                TransportError(503, "service_unavailable"), id="opensearch-transport"
            ),
        ],
    )
    def test_infrastructure_error_surfaces(
        self,
        error: Exception,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """Infrastructure failures propagate so the Celery error alarm fires.

        A database error or an OpenSearch connection/transport failure is not
        list-specific; swallowing it would let a persistent outage fail silently
        on every sweep instead of triggering the "unhandled Celery error"
        CloudWatch alarm. These must NOT be caught.
        """
        custom_list = _make_auto_updating_list(db, status=CustomList.INIT)

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            mock_queries.populate_query_pages.side_effect = error
            with pytest.raises(type(error)):
                custom_lists.update_custom_list_entries.delay(custom_list.id).wait()

    def test_updated_mode_no_query_skips(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """UPDATED mode with no auto_update_query skips populate_query_pages."""
        custom_list = _make_auto_updating_list(
            db, status=CustomList.UPDATED, query=None
        )

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()
            mock_queries.populate_query_pages.assert_not_called()

    def test_updated_mode_bad_json_skips(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
        services_fixture: ServicesFixture,
    ):
        """UPDATED mode with invalid JSON in auto_update_query skips gracefully."""
        custom_list = _make_auto_updating_list(
            db, status=CustomList.UPDATED, query="not-valid-json"
        )

        with patch(
            "palace.manager.celery.tasks.custom_lists.CustomListQueries"
        ) as mock_queries:
            custom_lists.update_custom_list_entries.delay(custom_list.id).wait()
            mock_queries.populate_query_pages.assert_not_called()


# ---------------------------------------------------------------------------
# Stage 2 — Finalize
# ---------------------------------------------------------------------------


class TestFinalizeCustomListEntriesSweep:
    def test_releases_sweep_lock_when_lock_value_provided(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """finalize_custom_list_entries_sweep releases the sweep lock."""
        lock_value = str(uuid4())
        sweep_lock = _sweep_lock(redis_fixture.client, lock_value)
        sweep_lock.acquire()
        assert sweep_lock.locked()

        custom_lists.finalize_custom_list_entries_sweep.delay(
            lock_value=lock_value
        ).wait()

        assert not sweep_lock.locked()

    def test_no_lock_released_when_standalone(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """Without a lock_value the task is a no-op (no lock to release)."""
        # Should not raise.
        custom_lists.finalize_custom_list_entries_sweep.delay().wait()
