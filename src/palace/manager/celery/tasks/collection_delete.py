from __future__ import annotations

from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.collection import Collection


def _collection_name(collection: Collection) -> str:
    return f"{collection.name}/{collection.protocol} ({collection.id})"


@shared_task(queue=QueueNames.high, bind=True)
def collection_delete(task: Task, collection_id: int) -> None:
    with task.transaction() as session:
        collection = Collection.by_id(session, collection_id)
        if collection is None:
            task.log.error(
                f"Collection with id {collection_id} not found. Unable to delete."
            )
            return

        task.log.info(f"Deleting collection {_collection_name(collection)}")
        collection.delete()
