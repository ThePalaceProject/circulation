"""
Celery tasks for maintaining custom list entries and custom-list lane sizes.

Pipeline (orchestrated via chords):

1. ``update_custom_list_entries_sweep``    — queries all auto-updating lists,
   fans them out into parallel ``update_custom_list_entries`` tasks, and wires
   ``update_custom_list_based_lane_sizes`` as the chord callback.
2. ``update_custom_list_entries``          — per-list: populates entries via
   OpenSearch and reconciles the cached ``size``.  Uses ``task.replace()`` to
   spread pagination over multiple short task invocations.
3. ``update_custom_list_based_lane_sizes`` — queries only lanes whose sizes
   depend on custom list content (see :func:`custom_list_lane_ids_query`) and
   fans them out into a chord of ``update_lane_size`` tasks with
   ``finalize_lane_size_update`` as the callback.

The generic lane-size primitives (``update_lane_size``,
``finalize_lane_size_update``) and the independent-lane sweep
(``update_independent_lane_sizes``, which recounts lanes *not* tied to any
custom list) live in :mod:`palace.manager.celery.tasks.lanes`.

The standalone ``update_custom_list_size`` task is kept for the CLI /
backward-compat path only; it is *not* a stage in the chord pipeline.
"""

from __future__ import annotations

import datetime
import json
from typing import Any
from uuid import uuid4

from celery import chord, group, shared_task
from opensearchpy import RequestError
from sqlalchemy import and_, exists, or_, select
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import Select

from palace.manager.celery.importer import workflow_lock_guard
from palace.manager.celery.task import Task
from palace.manager.celery.tasks.lanes import (
    finalize_lane_size_update,
    update_lane_size,
)
from palace.manager.celery.utils import ModelNotFoundError, load_from_id, signature_with
from palace.manager.core.query.customlist import CustomListQueries
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.customlist import CustomList, CustomListEntry
from palace.manager.sqlalchemy.model.lane import Lane, lanes_customlists

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
# Lane query helpers
# ---------------------------------------------------------------------------


def custom_list_lane_ids_query() -> Select:
    """Return a SELECT of lane IDs whose sizes depend on custom list content.

    A lane depends on custom list content if any of the following hold:

    - **Pattern A** — it has at least one direct association via the
      ``lanes_customlists`` junction table.
    - **Pattern B** — its ``_list_datasource_id`` is set (the lane shows all
      custom lists from a given DataSource, e.g. Best Sellers lanes).
    - **Pattern C** (inherited) — its parent lane satisfies Pattern A or B
      *and* the lane has ``inherit_parent_restrictions = True``.

    Pattern C uses a one-level parent join.  Lanes that inherit custom-list
    restrictions two or more levels up will be updated by
    ``update_independent_lane_sizes`` instead of the custom-list sweep;
    this is a deliberate conservative trade-off to avoid a recursive CTE.

    :return: A SELECT suitable for ``session.scalars(...)`` or
        ``Lane.id.in_(...)`` / ``Lane.id.not_in(...)``.
    """
    ParentLane = aliased(Lane, name="parent_lane")

    # Pattern A: direct association via junction table
    direct_assoc = exists().where(lanes_customlists.c.lane_id == Lane.id)

    # Pattern B: datasource-based (shows all lists from a DataSource)
    via_datasource = Lane._list_datasource_id.isnot(None)

    # For Pattern C: same checks applied to the parent lane
    parent_direct_assoc = exists().where(lanes_customlists.c.lane_id == ParentLane.id)
    parent_via_datasource = ParentLane._list_datasource_id.isnot(None)

    # Pattern C: lane inherits restrictions from a parent that satisfies A or B
    inherits_from_customlist_parent = (
        exists()
        .select_from(ParentLane)
        .where(
            and_(
                ParentLane.id == Lane.parent_id,
                Lane.inherit_parent_restrictions.is_(True),
                or_(parent_direct_assoc, parent_via_datasource),
            )
        )
    )

    return select(Lane.id).where(
        or_(direct_assoc, via_datasource, inherits_from_customlist_parent)
    )


# ---------------------------------------------------------------------------
# Stage 0 — Sweep orchestrator
# ---------------------------------------------------------------------------


@shared_task(queue=QueueNames.default, bind=True)
def update_custom_list_entries_sweep(task: Task) -> None:
    """Orchestrate the full custom list maintenance pipeline.

    Queries all auto-updating custom lists, fans them out into parallel
    ``update_custom_list_entries`` tasks, and uses ``update_custom_list_based_lane_sizes``
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
        update_custom_list_based_lane_sizes.delay(lock_value=lock_value)
        return

    chord(
        group([update_custom_list_entries.si(list_id) for list_id in list_ids]),
        update_custom_list_based_lane_sizes.si(lock_value=lock_value),
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
def update_custom_list_based_lane_sizes(
    task: Task, lock_value: str | None = None
) -> None:
    """Fan out lane size updates for lanes associated with custom lists.

    Queries lane IDs whose sizes depend on custom list content (via
    :func:`custom_list_lane_ids_query`) and creates a chord of
    ``update_lane_size`` tasks with ``finalize_lane_size_update`` as the
    callback.

    This task is the chord callback from ``update_custom_list_entries_sweep``.
    Lanes that do *not* depend on custom list content are updated separately by
    ``update_independent_lane_sizes`` on its own beat schedule.

    :param lock_value: Sweep-lock random value from
        ``update_custom_list_entries_sweep``.  Passed through to
        ``finalize_lane_size_update`` so the lock can be released there.
    """
    with task.session() as session:
        lane_ids: list[int] = list(session.scalars(custom_list_lane_ids_query()))

    task.log.info(f"Sweeping sizes for {len(lane_ids)} custom-list lane(s).")

    if not lane_ids:
        finalize_lane_size_update.delay(lock_value=lock_value)
        return

    chord(
        group([update_lane_size.si(lane_id) for lane_id in lane_ids]),
        finalize_lane_size_update.si(lock_value=lock_value),
    ).delay()
