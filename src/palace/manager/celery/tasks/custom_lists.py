"""
Celery tasks for maintaining custom list entries and lane sizes.

Pipeline (orchestrated via chords):

1. ``update_custom_list_entries_sweep``  — queries all auto-updating lists,
   fans them out into parallel ``update_custom_list_entries`` tasks, and wires
   ``update_lane_sizes_sweep`` as the chord callback.
2. ``update_custom_list_entries``        — per-list: populates entries via
   OpenSearch and reconciles the cached ``size``.  Uses ``task.replace()`` to
   spread pagination over multiple short task invocations.
3. ``update_lane_sizes_sweep``           — queries all lanes, fans them out into
   parallel ``update_lane_size`` tasks, and wires ``finalize_lane_size_update``
   as the chord callback.
4. ``update_lane_size``                  — per-lane: counts matching works in
   OpenSearch and stores the result.
5. ``finalize_lane_size_update``         — fires ``site_configuration_has_changed``
   once after all lane sizes are settled.

The standalone ``update_custom_list_size`` task is kept for the CLI /
backward-compat path only; it is *not* a stage in the chord pipeline.
"""

from __future__ import annotations

import datetime
import json
from typing import Any
from uuid import uuid4

from celery import chord, group, shared_task
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.celery.importer import workflow_lock_guard
from palace.manager.celery.task import Task
from palace.manager.celery.utils import ModelNotFoundError, load_from_id, signature_with
from palace.manager.core.query.customlist import CustomListQueries
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry
from palace.manager.sqlalchemy.model.lane import Lane

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Number of search-result pages (100 entries each) to process per
# update_custom_list_entries invocation.  At 500 entries/task we stay well
# under the 30-minute task_time_limit while keeping individual tasks short.
#
# Note: populate_query_pages defaults max_pages to 100,000 (10 M entries),
# which is effectively unlimited for any real-world custom list.  Lists that
# large would not be operationally useful.
_PAGES_PER_TASK: int = 5

# Lock TTL for per-list entry-update workflows.  Must outlive a single
# task invocation; short enough that a crashed worker doesn't block subsequent
# runs for too long.
_ENTRY_LOCK_TTL = datetime.timedelta(minutes=10)

# Lock TTL for the sweep-level orchestrator.  Should cover the full chord
# pipeline (entries → lane sizes) to prevent a second beat tick from launching
# a duplicate sweep.
_SWEEP_LOCK_TTL = datetime.timedelta(hours=2)


# ---------------------------------------------------------------------------
# Lock helpers
# ---------------------------------------------------------------------------


def _entry_update_lock(
    redis_client: Redis, custom_list_id: int, lock_value: str
) -> RedisLock:
    """Return a per-list Redis lock for the entry-update workflow."""
    return RedisLock(
        redis_client,
        ["CustomListEntriesUpdate", str(custom_list_id)],
        random_value=lock_value,
        lock_timeout=_ENTRY_LOCK_TTL,
    )


def _sweep_lock(redis_client: Redis, lock_value: str) -> RedisLock:
    """Return a Redis lock for the custom-list sweep orchestrator."""
    return RedisLock(
        redis_client,
        ["CustomListEntriesSweep"],
        random_value=lock_value,
        lock_timeout=_SWEEP_LOCK_TTL,
    )


# ---------------------------------------------------------------------------
# Stage 0 — Sweep orchestrator
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def update_custom_list_entries_sweep(task: Task) -> None:
    """Orchestrate the full custom list maintenance pipeline.

    Queries all auto-updating custom lists, fans them out into parallel
    ``update_custom_list_entries`` tasks, and uses ``update_lane_sizes_sweep``
    as the chord callback so that lane sizes are recalculated only after all
    list entries have been settled.

    A sweep-level Redis lock prevents a second beat-triggered run from
    overlapping with a sweep already in progress.  The lock is acquired here
    and released by ``finalize_lane_size_update`` at the end of the chord
    pipeline — so it genuinely covers the full entries → lane-sizes sequence.
    """
    redis = task.services.redis.client()
    lock_value = str(uuid4())

    if not _sweep_lock(redis, lock_value).acquire():
        task.log.warning(
            "Custom list entries sweep skipped: another sweep is already in progress."
        )
        return

    # The sweep lock is intentionally NOT released here.  It is passed through
    # the pipeline and released by finalize_lane_size_update once all lane
    # sizes have been updated.  The 2-hour TTL acts as a safety net in case the
    # orchestrator or any downstream task crashes before reaching finalize.

    with task.session() as session:
        list_ids: list[int] = list(
            session.scalars(
                select(CustomList.id).where(CustomList.auto_update_enabled.is_(True))
            )
        )

    task.log.info(f"Sweeping {len(list_ids)} auto-updating custom list(s).")

    if not list_ids:
        # No auto-updating lists; skip straight to lane size updates.
        update_lane_sizes_sweep.delay(lock_value=lock_value)
        return

    chord(
        group([update_custom_list_entries.si(list_id) for list_id in list_ids]),
        update_lane_sizes_sweep.si(lock_value=lock_value),
    ).delay()


# ---------------------------------------------------------------------------
# Stage 1 — Per-list entry update
# ---------------------------------------------------------------------------

# Internal sentinel: returned by _setup_first_invocation to signal "skip this list".
# The caller checks identity (json_query is _SKIP), not equality.
_SKIP: dict[str, Any] = {}


def _setup_first_invocation(
    task: Task,
    session: Session,
    custom_list: CustomList,
) -> dict[str, Any] | None:
    """Perform mode-specific setup on the first invocation of an entry-update run.

    Returns the ``json_query`` to pass to ``populate_query_pages``:

    - ``None`` for INIT and REPOPULATE modes (``populate_query_pages`` will
      load the raw query from ``custom_list.auto_update_query``).
    - A time-filtered query dict for UPDATED (steady-state) mode.
    - :data:`_SKIP` (identity sentinel) when the list should be skipped entirely.

    :param task: The calling Celery task (for logging).
    :param session: Active SQLAlchemy session.
    :param custom_list: The custom list being updated.
    """
    if custom_list.auto_update_status == CustomList.REPOPULATE:
        # Bulk-delete all entries before re-populating from page 1.
        # Using a bulk DELETE avoids the N individual DELETE statements that the
        # original script issued.
        task.log.info(
            f"Custom list {custom_list.name!r}: REPOPULATE — clearing all entries."
        )
        session.query(CustomListEntry).filter(
            CustomListEntry.list_id == custom_list.id
        ).delete(synchronize_session=False)
        # Expire the relationship so SQLAlchemy doesn't serve stale in-memory data.
        session.expire(custom_list, ["entries", "size"])
        return None  # full query, no time filter

    if custom_list.auto_update_status == CustomList.INIT:
        # Back-populate from page 1.  Re-adding already-present page-1 entries is
        # safe because add_entry uses get_one_or_create (no duplicates).
        task.log.info(
            f"Custom list {custom_list.name!r}: INIT — back-populating all entries."
        )
        return None  # full query, no time filter

    # UPDATED (steady state) — inject an availability-time filter so we only
    # fetch works that became available since the last update.
    if not custom_list.auto_update_query:
        task.log.info(
            f"Custom list {custom_list.name!r}: "
            "no auto_update_query configured; skipping."
        )
        return _SKIP

    try:
        json_query: dict[str, Any] = json.loads(custom_list.auto_update_query)
    except json.JSONDecodeError as exc:
        task.log.error(
            f"Custom list {custom_list.id} ({custom_list.name!r}): "
            f"could not decode auto_update_query: {exc}"
        )
        return _SKIP

    # Use the last-update timestamp as a filter floor so we only pick up newly
    # available titles.  Fall back to now() if the field is somehow null.
    availability_time = custom_list.auto_update_last_update or datetime.datetime.now()
    json_query["query"] = {
        "and": [
            {
                "key": "licensepools.availability_time",
                "op": "gte",
                "value": availability_time.timestamp(),
            },
            json_query["query"],
        ]
    }
    return json_query


@shared_task(queue=QueueNames.default, bind=True)
def update_custom_list_entries(
    task: Task,
    custom_list_id: int,
    json_query: dict[str, Any] | None = None,
    pagination_key: list[Any] | None = None,
) -> None:
    """Update entries for a single auto-updating custom list.

    Handles all three auto-update modes (INIT, REPOPULATE, UPDATED) on the
    first invocation, then pages through search results in batches of
    :data:`_PAGES_PER_TASK` pages.  When more pages remain the task re-queues
    itself via ``task.replace()`` so each worker slot stays short.

    After all entries are populated, the list's cached ``size`` is reconciled
    against the database count via :meth:`CustomList.update_size`.

    A per-list Redis lock (managed by :func:`~palace.manager.celery.importer.workflow_lock_guard`)
    prevents concurrent runs.  The lock is keyed on ``task.request.id``, which
    Celery preserves across ``task.replace()`` page hand-offs, so each
    continuation re-acquires the same lock without explicitly threading a value
    through the signature.

    :param custom_list_id: ID of the custom list to update.
    :param json_query: Pre-computed search-query dict.  ``None`` on the first
        invocation and for INIT/REPOPULATE continuations; non-``None`` for
        UPDATED continuations (carries the time-filtered query).
    :param pagination_key: Cursor from a previous :func:`populate_query_pages`
        call; ``None`` on the first invocation.
    """
    with workflow_lock_guard(
        task,
        custom_list_id,
        label=f"Custom list {custom_list_id} entries update",
        lock_factory=_entry_update_lock,
    ) as proceed:
        if not proceed:
            return

        is_first_invocation = pagination_key is None
        next_pagination_key: list[Any] | None = None

        try:
            with task.transaction() as session:
                custom_list = load_from_id(session, CustomList, custom_list_id)

                if not custom_list.auto_update_enabled:
                    task.log.info(
                        f"Custom list {custom_list_id} ({custom_list.name!r}): "
                        "auto_update_enabled is False; skipping."
                    )
                    return

                if is_first_invocation:
                    json_query = _setup_first_invocation(task, session, custom_list)
                    if json_query is _SKIP:
                        return

                search: ExternalSearchIndex = task.services.search.index()
                task.log.info(
                    f"Custom list {custom_list.name!r}: processing up to "
                    f"{_PAGES_PER_TASK * 100} entries "
                    f"(resuming={pagination_key is not None})."
                )

                total_added, next_pagination_key = (
                    CustomListQueries.populate_query_pages(
                        session,
                        search,
                        custom_list,
                        json_query=json_query,
                        pagination_key=pagination_key,
                        max_pages=_PAGES_PER_TASK,
                        # We manage metadata ourselves: write it only on the final invocation.
                        update_metadata=False,
                    )
                )

                task.log.info(
                    f"Custom list {custom_list.name!r}: "
                    f"added/updated {total_added} entries this batch."
                )

                if next_pagination_key is None:
                    # Final batch — write timestamps and reconcile cached size.
                    custom_list.auto_update_last_update = datetime.datetime.now()
                    custom_list.auto_update_status = CustomList.UPDATED
                    custom_list.update_size(session)
                    task.log.info(
                        f"Custom list {custom_list.name!r}: update complete, "
                        f"size={custom_list.size}."
                    )

            # Transaction committed above. If more pages remain, re-queue now
            # that the work is persisted.  signature_with carries all existing
            # arguments forward, overriding only the changed ones, so adding a
            # new parameter to this task's signature can never silently drop a
            # value.  task.replace() raises Ignore, which workflow_lock_guard
            # holds the lock across (Ignore is in its ignored_exceptions tuple).
            if next_pagination_key is not None:
                raise task.replace(
                    signature_with(
                        task,
                        json_query=json_query,
                        pagination_key=next_pagination_key,
                    )
                )

        except ModelNotFoundError:
            task.log.warning(
                f"Custom list {custom_list_id} not found; it may have been deleted. "
                "Skipping."
            )


# ---------------------------------------------------------------------------
# Standalone size reconciliation (CLI / backward-compat)
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def update_custom_list_size(task: Task, custom_list_id: int) -> None:
    """Reconcile the cached ``size`` column for a single custom list.

    This task is the CLI / backward-compat entrypoint.  In the chord-based
    pipeline the size is reconciled at the tail of ``update_custom_list_entries``
    instead, so this task is **not** a stage in the automated sweep.

    :param custom_list_id: ID of the custom list to reconcile.
    """
    try:
        with task.transaction() as session:
            custom_list = load_from_id(session, CustomList, custom_list_id)
            custom_list.update_size(session)
            task.log.info(
                f"Custom list {custom_list.name!r} ({custom_list_id}): "
                f"size updated to {custom_list.size}."
            )
    except ModelNotFoundError:
        task.log.warning(
            f"Custom list {custom_list_id} not found; it may have been deleted. Skipping."
        )


# ---------------------------------------------------------------------------
# Stage 2 — Lane size sweep orchestrator
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def update_lane_sizes_sweep(task: Task, lock_value: str | None = None) -> None:
    """Fan out lane size updates for all lanes.

    Queries all Lane IDs and creates a chord of ``update_lane_size`` tasks
    with ``finalize_lane_size_update`` as the callback.

    This task is the chord callback from ``update_custom_list_entries_sweep``
    and may also be invoked standalone (e.g. from the CLI or admin tooling).

    :param lock_value: Sweep-lock random value from
        ``update_custom_list_entries_sweep``.  Passed through to
        ``finalize_lane_size_update`` so the lock can be released there.
    """
    with task.session() as session:
        lane_ids: list[int] = list(session.scalars(select(Lane.id)))

    task.log.info(f"Sweeping sizes for {len(lane_ids)} lane(s).")

    if not lane_ids:
        finalize_lane_size_update.delay(lock_value=lock_value)
        return

    chord(
        group([update_lane_size.si(lane_id) for lane_id in lane_ids]),
        finalize_lane_size_update.si(lock_value=lock_value),
    ).delay()


# ---------------------------------------------------------------------------
# Stage 3 — Per-lane size update
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def update_lane_size(task: Task, lane_id: int) -> None:
    """Update the estimated size for a single lane.

    Suppresses the per-flush ``site_configuration_has_changed`` listener on
    the lane instance so that the cache-invalidation notification is batched
    into the single call made by ``finalize_lane_size_update`` after all lane
    tasks complete — matching the behaviour of the legacy
    ``UpdateLaneSizeScript``.

    :param lane_id: ID of the Lane to update.
    """
    search: ExternalSearchIndex = task.services.search.index()
    try:
        with task.transaction() as session:
            lane = load_from_id(session, Lane, lane_id)
            # Suppress the before-flush listener that calls
            # site_configuration_has_changed on every flush.
            # finalize_lane_size_update fires it once after all lane tasks finish.
            lane._suppress_before_flush_listeners = True
            lane.update_size(session, search_engine=search)
            task.log.info(f"{lane.full_identifier}: {lane.size}")
    except ModelNotFoundError:
        task.log.warning(
            f"Lane {lane_id} not found; it may have been deleted. Skipping."
        )


# ---------------------------------------------------------------------------
# Stage 4 — Finalize
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def finalize_lane_size_update(task: Task, lock_value: str | None = None) -> None:
    """Notify the system that lane sizes have changed.

    Called as the chord callback once all ``update_lane_size`` tasks complete.
    Fires ``site_configuration_has_changed`` a single time so downstream caches
    are invalidated without triggering a separate notification per lane.

    If ``lock_value`` is provided, releases the sweep-level Redis lock that was
    acquired by ``update_custom_list_entries_sweep`` at the start of the
    pipeline.  This ensures the lock truly covers the full chord sequence.

    :param lock_value: Sweep-lock random value to release, or ``None`` when
        this task is invoked standalone (outside the full pipeline).
    """
    with task.transaction() as session:
        site_configuration_has_changed(session)
    task.log.info("Lane size sweep complete: site configuration change recorded.")

    if lock_value is not None:
        redis = task.services.redis.client()
        released = _sweep_lock(redis, lock_value).release()
        if not released:
            task.log.warning(
                "Could not release sweep lock — it may have already expired or "
                "been released by another process."
            )
