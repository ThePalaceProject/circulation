from celery.canvas import Signature
from sqlalchemy.orm import Session
from sqlalchemy.sql import Select

from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.log import LoggerType, pluralize


def import_all(
    session: Session,
    collection_query: Select,
    import_collection: Signature,
    logger: LoggerType,
) -> None:
    """
    Create celery tasks to import all the Collections returned by the given query.
    """
    collections = session.scalars(collection_query).all()
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


def import_lock(client: Redis, collection_id: int) -> RedisLock:
    """
    Create a lock for the given collection.

    This makes sure only one task is importing data for the collection
    at a time.
    """
    return RedisLock(client, import_key(collection_id))
