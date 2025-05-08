from datetime import timedelta

from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.metadata_layer.identifier import IdentifierData
from palace.manager.metadata_layer.metadata import Metadata
from palace.manager.metadata_layer.policy.replacement import ReplacementPolicy
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.util import get_one


def metadata_apply_lock(client: Redis, identifier: IdentifierData) -> RedisLock:
    return RedisLock(
        client,
        ["MetadataApply", identifier],
        lock_timeout=timedelta(minutes=20),
    )


@shared_task(queue=QueueNames.metadata, bind=True)
def metadata_apply(
    task: Task,
    metadata: Metadata,
    edition_id: int,
    collection_id: int | None,
    replace: ReplacementPolicy | None = None,
) -> None:
    if metadata.primary_identifier_data is None:
        raise PalaceValueError("No primary identifier provided!")

    redis_client = task.services.redis.client()

    with (
        metadata_apply_lock(redis_client, metadata.primary_identifier_data).lock(),
        task.transaction() as session,
    ):
        edition = get_one(session, Edition, id=edition_id)
        if edition is None:
            raise PalaceValueError(f"Edition with id {edition_id} not found.")

        collection = (
            None
            if collection_id is None
            else get_one(session, Collection, id=collection_id)
        )
        if collection is None and collection_id is not None:
            raise PalaceValueError(f"Collection with id {collection_id} not found.")

        metadata.apply(session, edition, collection, replace)
