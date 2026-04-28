from datetime import timedelta

from celery.canvas import Signature

from palace.util.log import LoggerType, pluralize

from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection


def import_all(
    collections: list[Collection],
    import_collection: Signature,
    logger: LoggerType,
) -> None:
    """
    Create celery tasks to import all the Collections returned by the given query.
    """
    for collection in collections:
        task = import_collection.delay(
            collection_id=collection.id,
        )

        logger.info(
            f'Queued collection "{collection.name}" for import. Collection(id={collection.id}) Task(id={task.id})'
        )

    logger.info(
        f"Task complete. Queued {pluralize(len(collections), 'collection')} for import."
    )


def import_key(collection_id: int, *additional: str) -> list[str]:
    """
    Generate a Redis key for the given collection ID.
    """
    return [
        "ImportCollection",
        Collection.redis_key_from_id(collection_id),
        *additional,
    ]


def import_workflow_key(collection_id: int) -> list[str]:
    """
    Generate a Redis key for the workflow-level lock for the given collection.
    """
    return [
        "ImportCollectionWorkflow",
        Collection.redis_key_from_id(collection_id),
    ]


def import_workflow_lock(
    client: Redis, collection_id: int, random_value: str
) -> RedisLock:
    """
    Create a workflow-level lock spanning all pages of a single import run.

    This lock is held across page boundaries (between task.replace() calls)
    to ensure at most one import runs per collection at a time.
    """
    return RedisLock(
        client,
        import_workflow_key(collection_id),
        random_value=random_value,
        lock_timeout=timedelta(hours=2),
    )


def import_lock(client: Redis, collection_id: int) -> RedisLock:
    """
    Create a lock for the given collection.

    This makes sure only one task is importing data for the collection
    at a time.
    """
    return RedisLock(client, import_key(collection_id), lock_timeout=timedelta(hours=1))


def reap_workflow_key(collection_id: int) -> list[str]:
    """
    Generate a Redis key for the reap workflow-level lock for the given collection.
    """
    return [
        "ReapCollectionWorkflow",
        Collection.redis_key_from_id(collection_id),
    ]


def reap_workflow_lock(
    client: Redis, collection_id: int, random_value: str
) -> RedisLock:
    """
    Create a workflow-level lock spanning all batches of a single reap run.

    Held across task.replace() calls so at most one reap runs per collection at a time.
    Auto-expires after 2 hours if the process dies.
    """
    return RedisLock(
        client,
        reap_workflow_key(collection_id),
        random_value=random_value,
        lock_timeout=timedelta(hours=2),
    )
