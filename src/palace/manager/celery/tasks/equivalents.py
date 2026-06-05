from datetime import timedelta

from celery import shared_task
from celery.exceptions import Ignore, Retry

from palace.manager.celery.task import Task
from palace.manager.celery.utils import signature_with
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.dirty_identifiers import DirtyIdentifierIds
from palace.manager.service.redis.models.lock import LockNotAcquired, TaskLock
from palace.manager.sqlalchemy.refresh_equivalents import (
    add_identity_equivalents,
    process_identifier_ids,
)


@shared_task(queue=QueueNames.default, bind=True, throws=(LockNotAcquired,))
def equivalent_identifiers_refresh(
    task: Task,
    batch_size: int = 200,
    full_refresh: bool = False,
    ensure_identity: bool = False,
) -> None:
    """
    Recompute the RecursiveEquivalencyCache for identifier chains marked as dirty.

    IDs are added to the dirty queue by SQLAlchemy listeners when Equivalency rows
    are created or deleted. This task pops a batch, recomputes the affected chains,
    then re-queues itself until the queue is empty.

    A global :class:`~palace.manager.service.redis.models.lock.TaskLock` (owned by
    ``task.request.root_id``, which Celery preserves across ``task.replace()`` and
    retries) ensures at most one refresh run is in progress at a time, so the
    daily/weekly beat schedules can't start a second run that races on the shared
    dirty queue or the RecursiveEquivalencyCache.

    Self-reference ``(id, id)`` rows are maintained for new identifiers by the
    ``Identifier`` creation listener, so a normal delta run does not need to check
    for missing ones. The full-table sweep that backfills any missing self-rows is
    therefore only run as part of a ``full_refresh`` (weekly), avoiding a daily
    full-table scan.

    :param batch_size: Number of identifier IDs to process per invocation.
    :param full_refresh: If True, seed the dirty queue with all identifier IDs
        from the equivalents table before processing. Use for initial deployment
        or to recover after a Redis restart wipes the queue.
    :param ensure_identity: Internal chain-state flag. Set automatically when a
        ``full_refresh`` run starts and carried through the self-replacements so
        that the missing-self-reference sweep runs once, when the queue drains at
        the end of a full refresh.
    """
    redis_client = task.services.redis().client()
    dirty = DirtyIdentifierIds(redis_client)
    task_lock = TaskLock(task, lock_timeout=timedelta(hours=2))

    # Hold the lock across our self-replacements (release_on_exit=False) so a
    # concurrent beat run can't start a second refresh. The lock is keyed on the
    # task name and owned by root_id, so each self-replacement re-acquires the same
    # lock for free while a different run is locked out.
    with task_lock.lock(release_on_exit=False, ignored_exceptions=(Retry, Ignore)):
        if full_refresh:
            with task.transaction() as session:
                total = dirty.add_all_from_db(session)
            task.log.info(
                f"Full refresh: seeded dirty queue with {total} identifier IDs."
            )
            # Run the missing-self-reference sweep when this full refresh drains.
            ensure_identity = True

        identifier_ids = dirty.pop(batch_size)

        if identifier_ids:
            task.log.info(f"Processing {len(identifier_ids)} dirty identifier IDs.")
            try:
                with task.transaction() as session:
                    process_identifier_ids(session, identifier_ids)
            except Exception:
                # pop() already removed these IDs from Redis. If processing or the
                # commit failed, put them back so the batch isn't silently lost
                # until the next weekly full refresh; the next scheduled run will
                # retry them.
                dirty.add(*identifier_ids)
                task.log.warning(
                    f"Re-queued {len(identifier_ids)} identifier IDs after a "
                    "processing failure."
                )
                raise
            # full_refresh=False so the replacement doesn't re-seed the queue, but
            # ensure_identity is carried forward so the end-of-chain sweep still runs.
            raise task.replace(
                signature_with(
                    task, full_refresh=False, ensure_identity=ensure_identity
                )
            )

        # Queue drained. Only on a full refresh do we sweep the whole identifiers
        # table for missing self-references; delta runs rely on the creation
        # listener and skip the scan.
        if ensure_identity:
            with task.transaction() as session:
                add_identity_equivalents(session, batch_size)
            task.log.info(
                "Dirty queue is empty; identity equivalents ensured for all identifiers."
            )
        else:
            task.log.info("Dirty queue is empty; nothing to do.")

    # Reached only on the drained (terminal) path — the task.replace() above exits
    # via Ignore and keeps the lock held for the next batch. Release here so the
    # next scheduled run can start fresh.
    task_lock.release()
