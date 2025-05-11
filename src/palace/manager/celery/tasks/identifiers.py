from functools import partial

from celery import shared_task
from celery.canvas import Signature, chord
from sqlalchemy import select
from sqlalchemy.orm import raiseload

from palace.manager.celery.task import Task
from palace.manager.celery.tasks.apply import circulation_apply
from palace.manager.celery.utils import load_from_id
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import (
    IdentifierSet,
    RedisSetKwargs,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool


@shared_task(queue=QueueNames.default, bind=True)
def existing_available_identifiers(task: Task, collection_id: int) -> IdentifierSet:
    """
    Get all the identifiers that have licensepools that are available (licenses_available > 0
    and licenses_owned > 0) in the given collection and return them as a Redis IdentifierSet.

    This is meant to be used as part of a chord that marks any identifiers not found in
    a distributors feed as unavailable.

    See: mark_unavailable_chord.
    """

    redis_client = task.services.redis().client()
    identifier_set = IdentifierSet(redis_client, [task.name, task.request.id])

    try:
        with task.session() as session:
            identifiers_query = (
                select(Identifier)
                .join(LicensePool)
                .where(
                    LicensePool.collection_id == collection_id,
                    LicensePool.licenses_available != 0,
                    LicensePool.licenses_owned != 0,
                )
                .options(raiseload("*"))
            )

            for identifiers in (
                session.execute(identifiers_query).yield_per(100).scalars().partitions()
            ):
                identifier_set.add(*identifiers)
    except:
        identifier_set.delete()
        raise

    return identifier_set


@shared_task(queue=QueueNames.default, bind=True)
def mark_identifiers_unavailable(
    task: Task,
    identifiers: list[RedisSetKwargs],
    *,
    collection_id: int,
) -> None:
    """
    Takes two IdentiferSets as the first positional arg. The first set is the existing identifiers
    that are available in the collection. The second set is the active identifiers that we have
    received from the distributor.

    Any identifiers that are in the first set but not in the second set will be marked as
    unavailable in the collection. This is done by sending a circulation_apply task that sets
    the licenses_available and licenses_owned to 0 for the identifier in the collection.

    This is meant to be used as the body of a chord that is created by `mark_unavailable_chord`.
    """
    redis_client = task.services.redis().client()

    existing_identifiers, active_identifiers = identifiers
    existing_set = IdentifierSet(redis_client, **existing_identifiers)
    active_set = IdentifierSet(redis_client, **active_identifiers)

    try:
        if not existing_set.exists():
            task.log.warning(
                "Existing identifiers set does not exist in Redis. No identifiers to mark as unavailable."
            )
            return

        if not active_set.exists():
            task.log.error(
                "Active identifiers set does not exist in Redis. Refusing to mark all identifiers as unavailable."
            )
            raise PalaceValueError("Active identifiers set does not exist in Redis.")

        with task.session() as session:
            collection = load_from_id(session, Collection, collection_id)
            data_source = collection.data_source
            if data_source is None:
                raise PalaceValueError(
                    "Collection has no data source! (data_source is None)."
                )
            data_source_name = data_source.name
            collection_name = collection.name

        create_circulation_data = partial(
            CirculationData,
            data_source_name=data_source_name,
            licenses_owned=0,
            licenses_available=0,
        )
        identifiers_to_mark = existing_set - active_set
        for identifier in identifiers_to_mark:
            task.log.info(
                f"Marking identifier {identifier} as unavailable in collection {collection_name} ({collection_id})"
            )
            circulation_apply.delay(
                circulation=create_circulation_data(primary_identifier_data=identifier),
                collection_id=collection_id,
            )

        task.log.info(
            f"Sent tasks to mark {len(identifiers_to_mark)} identifiers as unavailable"
        )
    finally:
        existing_set.delete()
        active_set.delete()


def create_mark_unavailable_chord(
    collection_id: int, active_identifiers_sig: Signature
) -> Signature:
    """
    Create a celery chord that marks any identifiers that were not found in the distributors feed
    as unavailable for a given collection.

    This chord will first call the `existing_available_identifiers` task to get all the
    identifiers that are available in the collection.

    In parallel, it will call the `active_identifiers_sig` task to get the identifiers
    that are available in the distributors feed.

    Finally, it will call the `mark_identifiers_unavailable` task to mark any identifiers
    that are in the existing identifiers set but not in the active identifiers set as
    unavailable in the collection.

    The `active_identifiers_sig` task must be a celery signature that returns an IdentifierSet
    that contains the identifiers that are available in the distributors feed. The task can
    requeue itself if necessary, as long as it returns an IdentifierSet once it is done.
    """
    existing_identifiers_sig = existing_available_identifiers.s(collection_id)
    mark_identifiers_sig = mark_identifiers_unavailable.s(collection_id=collection_id)

    chord_header = [existing_identifiers_sig, active_identifiers_sig]

    return chord(chord_header, body=mark_identifiers_sig)
