from collections.abc import Iterable
from functools import partial
from logging import Logger

from celery import shared_task
from celery.canvas import Signature, chord
from sqlalchemy import and_, or_, select
from sqlalchemy.orm import raiseload
from sqlalchemy.sql import Select

from palace.util.exceptions import PalaceValueError
from palace.util.log import LoggerAdapterType

from palace.manager.celery.task import Task
from palace.manager.celery.tasks.apply import circulation_apply
from palace.manager.celery.utils import load_from_id, validate_not_none
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.identifier import IdentifierData
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
    LicensePoolType,
)


def available_identifiers_query(collection_id: int) -> Select:
    """
    Select the identifiers in a collection whose license pools are currently available.

    A pool is considered available if it is an UNLIMITED pool with ACTIVE status,
    or a metered/aggregated pool with non-zero owned and available license counts.
    UNLIMITED pools always have licenses_owned and licenses_available set to 0,
    so they need to be matched on status instead.

    Callers that only care about particular identifiers can narrow the returned
    query further, which is how a distributor that reports its own removals checks
    whether a title it has flagged is still available to us.

    No relationships are loaded: callers want the identifiers themselves, and the
    query is run over whole collections.
    """
    return (
        select(Identifier)
        .options(raiseload("*"))
        .join(LicensePool)
        .where(
            LicensePool.collection_id == collection_id,
            or_(
                and_(
                    LicensePool.type == LicensePoolType.UNLIMITED,
                    LicensePool.status == LicensePoolStatus.ACTIVE,
                ),
                and_(
                    LicensePool.type != LicensePoolType.UNLIMITED,
                    LicensePool.licenses_available != 0,
                    LicensePool.licenses_owned != 0,
                ),
            ),
        )
    )


def queue_unavailable_updates(
    identifiers: Iterable[IdentifierData],
    *,
    collection_id: int,
    collection_name: str,
    data_source_name: str,
    status: LicensePoolStatus,
    log: Logger | LoggerAdapterType,
) -> int:
    """
    Queue a `circulation_apply` task for each identifier, zeroing out its licenses.

    :param status: The status to record on the affected license pools.
    :return: The number of tasks queued.
    """
    create_circulation_data = partial(
        CirculationData,
        data_source_name=data_source_name,
        licenses_owned=0,
        licenses_available=0,
        status=status,
    )
    queued = 0
    for identifier in identifiers:
        log.info(
            f"Marking identifier {identifier} as unavailable in collection "
            f"{collection_name} ({collection_id})"
        )
        circulation_apply.delay(
            circulation=create_circulation_data(primary_identifier_data=identifier),
            collection_id=collection_id,
        )
        queued += 1
    return queued


@shared_task(queue=QueueNames.default, bind=True)
def existing_available_identifiers(
    task: Task, collection_id: int, *, expire_time: int | None = None
) -> IdentifierSet:
    """
    Retrieves all identifiers in the specified collection whose license pools are
    currently available, returning them as a Redis IdentifierSet.

    See `available_identifiers_query` for what counts as available.

    This function is designed to be used as part of a chord operation that identifies and
    marks identifiers not present in a distributor's feed as unavailable.

    :param expire_time: How long, in seconds, the set should outlive its creation.
        This set is built once at the start of the chord and not touched again, so it
        must outlast the other half of the chord however long that takes to run. The
        default suits a feed that is walked in minutes; a distributor whose active-side
        task runs for hours needs to say so.

    See: `create_mark_unavailable_chord`.
    """

    redis_client = task.services.redis().client()
    identifier_set = (
        IdentifierSet(redis_client, [task.name, task.request.id])
        if expire_time is None
        else IdentifierSet(
            redis_client, [task.name, task.request.id], expire_time=expire_time
        )
    )

    try:
        with task.session() as session:
            identifiers_query = available_identifiers_query(collection_id)

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
    status: LicensePoolStatus = LicensePoolStatus.REMOVED,
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
    licenses_available and licenses_owned both set to 0, and status set to `status`.

    This function is designed to be used as the body of a chord created by `create_mark_unavailable_chord`.

    :param status: The status to record on the affected license pools. Defaults to
        `LicensePoolStatus.REMOVED`, which revokes outstanding loans and holds. Distributors
        whose feeds routinely drop titles that are merely out of circulation should pass
        `LicensePoolStatus.EXHAUSTED` instead.
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
            # An empty set is never materialised in Redis, so this is either a
            # collection with nothing available to reap -- normal, and marking nothing
            # is the right outcome -- or a set that expired before the other half of
            # the chord finished, which throws away a completed run. Only the second
            # is worth waking anyone up for.
            with task.session() as session:
                collection_has_identifiers = (
                    session.execute(
                        available_identifiers_query(collection_id).limit(1)
                    ).first()
                    is not None
                )
            message = "Existing identifiers set does not exist in Redis. No identifiers to mark as unavailable."
            if collection_has_identifiers:
                task.log.error(
                    f"{message} The collection does have available identifiers, so a "
                    f"completed run has been discarded."
                )
            else:
                task.log.info(f"{message} The collection has none to mark.")
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

        queued = queue_unavailable_updates(
            existing_identifiers - active_identifiers,
            collection_id=collection_id,
            collection_name=collection_name,
            data_source_name=data_source_name,
            status=status,
            log=task.log,
        )

        task.log.info(f"Sent tasks to mark {queued} identifiers as unavailable")
        return True
    finally:
        existing_identifiers.delete()
        active_identifiers.delete()


def create_mark_unavailable_chord(
    collection_id: int,
    active_identifiers_sig: Signature,
    *,
    status: LicensePoolStatus = LicensePoolStatus.REMOVED,
    expire_time: int | None = None,
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
    :param status: The status to record on the affected license pools. See
        `mark_identifiers_unavailable`.
    :param expire_time: How long, in seconds, the existing-identifier set should live.
        The two halves of the chord start together but the existing side finishes
        immediately, so this has to cover however long `active_identifiers_sig` runs
        for. See `existing_available_identifiers`.

    :return: A Celery chord signature that can be executed to perform the unavailable identifier marking process.
    """
    existing_identifiers_sig = existing_available_identifiers.s(
        collection_id, expire_time=expire_time
    )
    mark_identifiers_sig = mark_identifiers_unavailable.s(
        collection_id=collection_id, status=status
    )

    chord_header = [existing_identifiers_sig, active_identifiers_sig]

    return chord(chord_header, body=mark_identifiers_sig)
