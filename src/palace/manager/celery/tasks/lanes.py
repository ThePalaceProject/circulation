"""
Celery tasks for maintaining lane sizes.

A lane's cached ``size`` is an estimate of how many works it contains.  These
tasks recalculate that estimate by counting matching works in OpenSearch:

1. ``update_lane_size``              — per-lane worker: counts matching works for
   a single lane and stores the result.
2. ``finalize_lane_size_update``     — chord callback: fires
   ``site_configuration_has_changed`` once after a batch of lane updates settles
   (and optionally releases the custom-list sweep lock).
3. ``update_independent_lane_sizes`` — beat-scheduled sweep of every lane that is
   *not* associated with any custom list (genre, language, audience lanes, etc.).
   These change only when the underlying collection changes, so they run on their
   own schedule rather than being tied to the custom-list sweep.

Lanes whose sizes depend on custom-list content are swept separately by
``custom_lists.update_custom_list_based_lane_sizes`` as part of the custom-list
maintenance pipeline; that task and ``update_independent_lane_sizes`` both fan
out into the ``update_lane_size`` / ``finalize_lane_size_update`` primitives
defined here.
"""

from __future__ import annotations

from celery import chord, group, shared_task
from sqlalchemy import select

from palace.manager.celery.task import Task
from palace.manager.celery.utils import ModelNotFoundError, load_from_id
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.listeners import site_configuration_has_changed
from palace.manager.sqlalchemy.model.lane import Lane


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
        # Imported lazily to break a circular import: custom_lists imports
        # update_lane_size / finalize_lane_size_update from this module at module
        # load time, so this module must not import custom_lists at the top level.
        # The sweep-lock identity belongs to the custom-list sweep orchestrator.
        from palace.manager.celery.tasks.custom_lists import _sweep_lock

        redis = task.services.redis.client()
        released = _sweep_lock(redis, lock_value).release()
        if not released:
            task.log.warning(
                "Could not release sweep lock — it may have already expired or "
                "been released by another process."
            )


@shared_task(queue=QueueNames.default, bind=True)
def update_independent_lane_sizes(task: Task) -> None:
    """Fan out lane size updates for lanes not associated with any custom list.

    Queries lane IDs that do *not* depend on custom list content (the
    complement of :func:`custom_list_lane_ids_query`) and creates a chord of
    ``update_lane_size`` tasks with ``finalize_lane_size_update`` as the
    callback.

    These lanes (genre lanes, language lanes, audience lanes, etc.) only change
    when the underlying collection changes — not when custom list entries are
    updated — so they are scheduled independently every 6 hours rather than
    being tied to the custom list sweep.
    """
    # Imported lazily to break a circular import (see finalize_lane_size_update).
    # Identifying custom-list lanes is custom-list domain knowledge, so the query
    # lives in the custom_lists module; "independent" lanes are its complement.
    from palace.manager.celery.tasks.custom_lists import custom_list_lane_ids_query

    with task.session() as session:
        lane_ids: list[int] = list(
            session.scalars(
                select(Lane.id).where(Lane.id.not_in(custom_list_lane_ids_query()))
            )
        )

    task.log.info(f"Sweeping sizes for {len(lane_ids)} independent lane(s).")

    if not lane_ids:
        finalize_lane_size_update.delay()
        return

    chord(
        group([update_lane_size.si(lane_id) for lane_id in lane_ids]),
        finalize_lane_size_update.si(),
    ).delay()
