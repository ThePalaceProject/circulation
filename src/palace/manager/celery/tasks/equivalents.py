from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.dirty_identifiers import DirtyIdentifierIds
from palace.manager.sqlalchemy.refresh_equivalents import (
    add_identity_equivalents,
    process_identifier_ids,
)


@shared_task(queue=QueueNames.default, bind=True)
def equivalent_identifiers_refresh(
    task: Task, batch_size: int = 200, full_refresh: bool = False
) -> None:
    """
    Recompute the RecursiveEquivalencyCache for identifier chains marked as dirty.

    IDs are added to the dirty queue by SQLAlchemy listeners when Equivalency rows
    are created or deleted. This task pops a batch, recomputes the affected chains,
    then re-queues itself until the queue is empty.

    Once the queue is empty, self-reference rows are added for any Identifier
    that is still missing one.

    :param batch_size: Number of identifier IDs to process per invocation.
    :param full_refresh: If True, seed the dirty queue with all identifier IDs
        from the equivalents table before processing. Use for initial deployment
        or to recover after a Redis restart wipes the queue.
    """
    redis_client = task.services.redis().client()
    dirty = DirtyIdentifierIds(redis_client)

    if full_refresh:
        with task.transaction() as session:
            total = dirty.add_all_from_db(session)
        task.log.info(f"Full refresh: seeded dirty queue with {total} identifier IDs.")

    identifier_ids = dirty.pop(batch_size)

    if not identifier_ids:
        with task.transaction() as session:
            add_identity_equivalents(session, batch_size)
        task.log.info(
            "Dirty queue is empty; identity equivalents ensured for all identifiers."
        )
        return

    task.log.info(f"Processing {len(identifier_ids)} dirty identifier IDs.")
    with task.transaction() as session:
        process_identifier_ids(session, identifier_ids)

    raise task.replace(equivalent_identifiers_refresh.s(batch_size=batch_size))
