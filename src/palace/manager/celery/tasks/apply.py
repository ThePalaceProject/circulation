from datetime import timedelta

from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.celery.utils import load_from_id
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import LockNotAcquired, RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition


def _lock(client: Redis, identifier: IdentifierData) -> RedisLock:
    """
    Create a lock for the given identifier.

    This makes sure that only one task is applying data for a given identifier at a time.
    """
    return RedisLock(
        client,
        ["Apply", identifier],
        lock_timeout=timedelta(minutes=20),
    )


def _validate_primary_identifier(
    primary_identifier: IdentifierData | None,
) -> IdentifierData:
    """
    Validate that the primary identifier is not None.
    """
    if primary_identifier is None:
        raise PalaceValueError(
            "No primary identifier provided! (primary_identifier_data is None)."
        )

    return primary_identifier


@shared_task(
    queue=QueueNames.apply,
    bind=True,
    autoretry_for=(LockNotAcquired,),
    max_retries=4,
    retry_backoff=30,
)
def circulation_apply(
    task: Task,
    circulation: CirculationData,
    collection_id: int,
    replace: ReplacementPolicy | None = None,
) -> None:
    """
    Call CirculationData.apply() on the given collection.
    """

    redis_client = task.services.redis().client()
    primary_identifier = _validate_primary_identifier(
        circulation.primary_identifier_data
    )

    with (
        _lock(redis_client, primary_identifier).lock(),
        task.transaction() as session,
    ):
        collection = load_from_id(session, Collection, collection_id)
        circulation.apply(session, collection, replace)


@shared_task(
    queue=QueueNames.apply,
    bind=True,
    autoretry_for=(LockNotAcquired,),
    max_retries=4,
    retry_backoff=30,
)
def bibliographic_apply(
    task: Task,
    bibliographic: BibliographicData,
    edition_id: int,
    collection_id: int | None,
    replace: ReplacementPolicy | None = None,
) -> None:
    """
    Call BibliographicData.apply() on the given edition.
    """

    redis_client = task.services.redis().client()
    primary_identifier = _validate_primary_identifier(
        bibliographic.primary_identifier_data
    )

    with (
        _lock(redis_client, primary_identifier).lock(),
        task.transaction() as session,
    ):
        edition = load_from_id(session, Edition, edition_id)
        collection = (
            load_from_id(session, Collection, collection_id) if collection_id else None
        )

        bibliographic.apply(session, edition, collection, replace)
