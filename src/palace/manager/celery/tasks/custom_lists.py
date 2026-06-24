"""
Celery tasks for maintaining custom list entries.

Pipeline (orchestrated via a chord):

1. ``update_custom_list_entries_sweep``    — beat-scheduled orchestrator: queries all
   auto-updating custom lists, fans them out into parallel
   ``update_custom_list_entries`` tasks, and releases the sweep lock via
   ``finalize_custom_list_entries_sweep`` once they have all completed.
2. ``update_custom_list_entries``          — per-list: populates entries via OpenSearch
   and reconciles the list's cached ``size``.  Uses ``task.replace()`` to spread
   pagination over multiple short task invocations.
3. ``finalize_custom_list_entries_sweep``  — chord callback: releases the sweep-level
   Redis lock after every per-list task has finished.
"""

from __future__ import annotations

import datetime
import json
from typing import Any
from uuid import uuid4

from celery import chord, group, shared_task
from opensearchpy import RequestError
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
from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry

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

# Lock TTL for per-list entry-update workflows.  Sized to outlive a single task
# invocation: it must be >= the Celery task_time_limit (1800s / 30 min) so a
# slow-but-not-yet-killed batch can never run with an expired lock.  Each
# task.replace() page hand-off re-acquires the same lock (keyed on the stable
# task.request.id), so this only needs to span one batch, not the whole list.
# Beyond that it is purely a crash-recovery backstop (a dead worker frees the
# list after this window); concurrent *sweeps* are already serialized by the
# 2h sweep lock below.
_ENTRY_LOCK_TTL = datetime.timedelta(minutes=35)

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
    """Orchestrate the custom list entry-update pipeline.

    Queries all auto-updating custom lists and fans them out into parallel
    ``update_custom_list_entries`` tasks, with ``finalize_custom_list_entries_sweep``
    as the chord callback so the sweep lock is released only after every per-list
    task has finished.

    A sweep-level Redis lock prevents a second beat-triggered run from
    overlapping with a sweep already in progress.  The lock is acquired here
    and released by ``finalize_custom_list_entries_sweep`` at the end of the
    chord — so it genuinely covers the full fan-out.
    """
    redis = task.services.redis.client()
    lock_value = str(uuid4())

    if not _sweep_lock(redis, lock_value).acquire():
        task.log.warning(
            "Custom list entries sweep skipped: another sweep is already in progress."
        )
        return

    # The sweep lock is intentionally NOT released here.  It is passed through
    # the chord and released by finalize_custom_list_entries_sweep once all
    # per-list tasks have finished.  The 2-hour TTL acts as a safety net in case
    # the orchestrator or any downstream task crashes before reaching finalize.

    with task.session() as session:
        list_ids: list[int] = list(
            session.scalars(
                select(CustomList.id).where(CustomList.auto_update_enabled.is_(True))
            )
        )

    task.log.info(f"Sweeping {len(list_ids)} auto-updating custom list(s).")

    if not list_ids:
        # No auto-updating lists; release the sweep lock immediately.
        finalize_custom_list_entries_sweep.delay(lock_value=lock_value)
        return

    chord(
        group([update_custom_list_entries.si(list_id) for list_id in list_ids]),
        finalize_custom_list_entries_sweep.si(lock_value=lock_value),
    ).delay()


# ---------------------------------------------------------------------------
# Stage 1 — Per-list entry update
# ---------------------------------------------------------------------------


class _Skip:
    """Sentinel type returned by ``_setup_first_invocation`` to signal "skip this list".

    Using a dedicated class (rather than a bare ``{}``) makes the identity
    contract explicit: callers check ``result is _SKIP``, not ``result == {}``,
    which would be ambiguous if a legitimate empty dict were ever a valid query.
    """


_SKIP = _Skip()


def _setup_first_invocation(
    task: Task,
    session: Session,
    custom_list: CustomList,
) -> dict[str, Any] | _Skip | None:
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
                    setup_result = _setup_first_invocation(task, session, custom_list)
                    if isinstance(setup_result, _Skip):
                        return
                    json_query = setup_result

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
        except RequestError:
            # This task is a chord header, so an unhandled error aborts the whole
            # sweep chord. A RequestError (OpenSearch 400) means *this* list's
            # auto_update_query is malformed -- a list-specific problem, not an
            # infrastructure failure -- so we skip it rather than let one bad query
            # block the rest of the sweep.
            #
            # A malformed query should not be reachable through normal use: the
            # admin UI and circulation API validate queries before saving. Seeing
            # one here therefore points to an upstream validation bug, so we log it
            # as an exception (with traceback) to make it visible -- but we do not
            # escalate to a task failure, since a single bad list shouldn't take
            # down the sweep.
            #
            # Infrastructure failures are deliberately NOT caught here: a
            # SQLAlchemyError (database down) or an OpenSearch connection/transport
            # error affects every list, not just this one, and must propagate so
            # the task fails and triggers the "unhandled Celery error" CloudWatch
            # alarm in hosting-playbook rather than failing silently on every sweep.
            task.log.exception(
                f"Custom list {custom_list_id} has a malformed auto_update_query; "
                "skipping it so the rest of the sweep can proceed."
            )


# ---------------------------------------------------------------------------
# Stage 2 — Finalize
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def finalize_custom_list_entries_sweep(
    task: Task, lock_value: str | None = None
) -> None:
    """Release the sweep-level Redis lock at the tail of the entries sweep.

    Chord callback from ``update_custom_list_entries_sweep``.  It exists solely
    to release the sweep lock after every per-list ``update_custom_list_entries``
    task has finished, so the lock truly covers the full fan-out.

    :param lock_value: Sweep-lock random value from
        ``update_custom_list_entries_sweep``.  When provided, releases the lock.
    """
    if lock_value is None:
        return

    redis = task.services.redis.client()
    released = _sweep_lock(redis, lock_value).release()
    if not released:
        task.log.warning(
            "Could not release the custom-list sweep lock — it may have already "
            "expired or been released by another process."
        )
