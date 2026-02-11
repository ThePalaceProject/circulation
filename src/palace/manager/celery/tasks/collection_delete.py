from __future__ import annotations

from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.collection import Collection


def _collection_name(collection: Collection) -> str:
    return f"{collection.name}/{collection.protocol} ({collection.id})"


@shared_task(queue=QueueNames.default, bind=True)
def collection_delete(task: Task, collection_id: int, batch_size: int = 1000) -> None:
    """Delete a collection in batches to avoid task timeouts.

    Each invocation deletes up to ``batch_size`` license pools. If more
    remain, the task re-queues itself via ``task.replace()``.
    """
    with task.transaction() as session:
        collection = Collection.by_id(session, collection_id)
        if collection is None:
            task.log.error(
                f"Collection with id {collection_id} not found. Unable to delete."
            )
            return

        task.log.info(f"Deleting collection {_collection_name(collection)}")
        complete = collection.delete(batch_size=batch_size)

    if not complete:
        task.log.info(
            f"Collection {collection_id} has more license pools to delete. "
            f"Re-queueing."
        )
        raise task.replace(collection_delete.s(collection_id, batch_size=batch_size))
