from functools import partial

from celery import shared_task
from celery.canvas import Signature, chord
from sqlalchemy import select
from sqlalchemy.orm import raiseload

from palace.manager.celery.task import Task
from palace.manager.celery.tasks.apply import circulation_apply
from palace.manager.celery.utils import load_from_id, validate_not_none
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import (
    IdentifierSet,
    RedisSetKwargs,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolStatus,
)


@shared_task(queue=QueueNames.default, bind=True)
def existing_available_identifiers(task: Task, collection_id: int) -> IdentifierSet:
    """
    Retrieves all identifiers that have available licensepools (where licenses_available > 0 and
    licenses_owned > 0) in the specified collection and returns them as a Redis IdentifierSet.

    This function is designed to be used as part of a chord operation that identifies and
    marks identifiers not present in a distributor's feed as unavailable.

    See: `create_mark_unavailable_chord`.
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
    existing_and_active_identifier_sets: list[RedisSetKwargs | None],
    *,
    collection_id: int,
) -> bool:
    """
    Takes a list of two RedisSetKwargs elements as the first positional argument. These are used to create
    two IdentifierSets: the first represents existing identifiers that are available in the collection, and
    the second contains active identifiers received from the distributor.

    If the calling task wants to abort marking identifiers as unavailable, it can return None for either
    of the sets. This will cause the function to log a warning, clean up any IdentifierSets it was
    passed and exit without marking any identifiers as unavailable.

    Any identifiers present in the existing set but not in the active set will be marked as unavailable in
    the collection. This is done by sending a circulation_apply task that creates CirculationData with
    licenses_available and licenses_owned both set to 0, and status set to LicensePoolStatus.REMOVED.

    This function is designed to be used as the body of a chord created by `create_mark_unavailable_chord`.

    :return: True if identifiers were successfully marked as unavailable, False if the operation was skipped.
    """
    redis_client = task.services.redis().client()

    existing_identifier_kwargs, active_identifier_kwargs = (
        existing_and_active_identifier_sets
    )

    if existing_identifier_kwargs is None or active_identifier_kwargs is None:
        # If one of the sets is None, one of the tasks in the chord failed.
        task.log.warning(
            "Received None instead of IdentifierSet. Aborting without marking any identifiers as unavailable."
        )
        # Attempt to clean up any existing IdentifierSets that may have been created.
        for cleanup_kwargs in [existing_identifier_kwargs, active_identifier_kwargs]:
            if cleanup_kwargs is not None:
                IdentifierSet(redis_client, **cleanup_kwargs).delete()
        return False

    existing_identifiers = IdentifierSet(redis_client, **existing_identifier_kwargs)
    active_identifiers = IdentifierSet(redis_client, **active_identifier_kwargs)

    try:
        if not existing_identifiers.exists():
            task.log.warning(
                "Existing identifiers set does not exist in Redis. No identifiers to mark as unavailable."
            )
            return False

        if not active_identifiers.exists():
            task.log.error(
                "Active identifiers set does not exist in Redis. Refusing to mark all identifiers as unavailable."
            )
            raise PalaceValueError("Active identifiers set does not exist in Redis.")

        with task.session() as session:
            collection = load_from_id(session, Collection, collection_id)
            data_source_name = validate_not_none(
                collection.data_source,
                message="Collection has no data source! (data_source is None).",
            ).name
            collection_name = collection.name

        create_circulation_data = partial(
            CirculationData,
            data_source_name=data_source_name,
            licenses_owned=0,
            licenses_available=0,
            status=LicensePoolStatus.REMOVED,
        )
        identifiers_to_mark = existing_identifiers - active_identifiers
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
        return True
    finally:
        existing_identifiers.delete()
        active_identifiers.delete()


def create_mark_unavailable_chord(
    collection_id: int, active_identifiers_sig: Signature
) -> Signature:
    """
    Creates a Celery chord that identifies and marks as unavailable any identifiers that were not
    found in the distributor's feed for a given collection.

    This chord performs the following sequence:
    1. Performs the following in parallel:
        a. Calls the existing_available_identifiers task to retrieve all identifiers that are currently
           available in the collection.
        b. Executes the provided active_identifiers_sig task to retrieve identifiers that
           are present in the distributor's feed.
    2. Finally, calls the mark_identifiers_unavailable task with the results of the previous tasks
       to mark any identifiers that exist in the collection but are not in the distributor's feed
       as unavailable.

    :param collection_id: The ID of the collection to process.
    :param active_identifiers_sig: A Celery signature that returns an IdentifierSet containing
        identifiers available in the distributor's feed. This task may requeue itself if needed, as
        long as it ultimately returns an IdentifierSet.

    :return: A Celery chord signature that can be executed to perform the unavailable identifier marking process.
    """
    existing_identifiers_sig = existing_available_identifiers.s(collection_id)
    mark_identifiers_sig = mark_identifiers_unavailable.s(collection_id=collection_id)

    chord_header = [existing_identifiers_sig, active_identifiers_sig]

    return chord(chord_header, body=mark_identifiers_sig)
