import time
from datetime import datetime

from celery import shared_task
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from palace.manager.api.axis import Axis360API
from palace.manager.api.circulation import (
    BaseCirculationAPI,
    LibrarySettingsType,
    SettingsType,
)
from palace.manager.celery.task import Task
from palace.manager.core.metadata_layer import CirculationData, Metadata
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.backoff import exponential_backoff
from palace.manager.util.datetime_helpers import datetime_utc, utc_now

DEFAULT_BATCH_SIZE: int = 25
DEFAULT_START_TIME = datetime_utc(1970, 1, 1)
TARGET_MAX_EXECUTION_SECONDS = 120


@shared_task(queue=QueueNames.default, bind=True)
def import_all_collections(
    task: Task, import_all: bool = False, batch_size: int = DEFAULT_BATCH_SIZE
) -> None:
    """
    A shared task that loops through all Axis360 Api based collections and kick off an
    import task for each.
    """
    with task.session() as session:
        count = 0
        for collection in get_collections_by_protocol(
            task=task, session=session, protocol_class=Axis360API
        ):
            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for importing...'
            )
            list_identifiers_for_import.apply_async(
                kwargs={"collection_id": collection.id},
                link=import_identifiers.s(
                    collection_id=collection.id,
                    batch_size=batch_size,
                ),
            )

            count += 1
        task.log.info(f'Finished queuing {count} collection{"s" if count > 1 else ""}.')


@shared_task(queue=QueueNames.default, bind=True)
def list_identifiers_for_import(
    task: Task,
    collection_id: int,
    import_all: bool = False,
) -> list[str] | None:
    """
    A task for resolving a list identifiers to import an axis collection based on the
     most recent timestamp's start date.
    """

    # ensure batch queuing not already running for this collection.
    lock = _redis_lock_list_identifiers_for_import(
        task.services.redis.client(), collection_id
    )
    with lock.lock(raise_when_not_acquired=False) as locked:

        with task.transaction() as session:
            collection = Collection.by_id(session, collection_id)
            if not collection:
                task.log.error(f"Collection not found:  {collection_id} : ignoring...")
                return None

            if not locked:
                # we want to log
                task.log.warning(
                    f'Skipping list_identifiers_for_import for "{collection.name}"({collection_id}) because another '
                    f"task holds its lock. This means that a previously spawned instance of this task is taking an "
                    f"unexpectedly long time. It is likely that  this collection is being processed for the first time "
                    f"and therefore must read the entire list of identifiers for this collection."
                )
                return None

            # retrieve timestamp of last run
            ts = timestamp(
                _db=session,
                default_start_time=DEFAULT_START_TIME,
                service_name=task.name,
                collection=collection,
            )

            # if import_all use default start date
            if import_all:
                start_time_of_last_scan = DEFAULT_START_TIME
            else:
                # otherwise use the start date of the previous timestamp.
                start_time_of_last_scan = ts.start if ts.start else DEFAULT_START_TIME

            task_run_start_time = utc_now()
            # loop through feed :  for every {batch_size} items, pack them in a list and pass along to sub task for
            # processing
            api = create_api(collection, session)
            task.log.info(
                f"Starting process of queuing items in collection {collection.name} (id={collection_id} "
                f"for import that have changed since {start_time_of_last_scan}. "
            )
            # start stopwatch
            start_seconds = time.perf_counter()
            title_ids: list[str] = []
            for metadata, circulation in api.recent_activity(start_time_of_last_scan):
                if metadata.primary_identifier is not None:
                    title_ids.append(metadata.primary_identifier.identifier)
            elapsed_time = time.perf_counter() - start_seconds
            achievements = (
                f"Total items queued for import:  {len(title_ids)}; "
                f"elapsed time: {elapsed_time:0.2f}"
            )
            ts.update(
                start=task_run_start_time, finish=utc_now(), achievements=achievements
            )
            # log the end of the run
            task.log.info(
                f"Finished listing identifiers in collection {collection.name} (id={collection_id} "
                f"for import that have changed since {start_time_of_last_scan}. "
                f"{achievements}"
            )

            return title_ids


def create_api(collection: Collection, session: Session) -> Axis360API:
    return Axis360API(session, collection)


def timestamp(
    _db: Session,
    default_start_time: datetime,
    service_name: str,
    collection: Collection,
    default_counter: int = 0,
) -> Timestamp:
    """Find or create a Timestamp.
    A new timestamp will have .finish set to None, since the first
    run is presumably in progress.
    """
    timestamp, new = get_one_or_create(
        _db,
        Timestamp,
        service=service_name,
        service_type=Timestamp.TASK_TYPE,
        collection=collection,
        create_method_kwargs=dict(
            start=default_start_time,
            finish=None,
            counter=default_counter,
        ),
    )
    return timestamp


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def import_identifiers(
    task: Task,
    identifiers: list[str] | None,
    collection_id: int,
    processed_count: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
    target_max_execution_time_in_seconds: float = TARGET_MAX_EXECUTION_SECONDS,
) -> None:
    """
    This method creates new or updates new editions and license pools for each identifier in the list of identifiers.
    It will query the axis availability api in batches of {batch_size} IDs and process each result in a single database
    transaction.  It will continue in this way until it has finished the list or exceeded the max execution time
    which defaults to 2 minutes.  If it has not finished in the target time, it will requeue the task with the
    remaining unprocessed identifiers.
    """
    with task.transaction() as session:
        collection = Collection.by_id(session, id=collection_id)
        if not collection:
            task.log.error(f"Collection not found:  {collection_id} : ignoring...")
            return None

        def log_run_end_message() -> None:
            task.log.info(
                f"Finished importing identifiers for collection ({collection.name}, id={collection_id}), "
                f"task(id={task.request.id})"
            )

        if identifiers is None:

            task.log.info(
                f"Identifiers list is None: the list_identifiers_for_import "
                f"must have been locked. Ignoring import run for collection_id={collection_id}"
            )
            return

        if not identifiers:
            task.log.info(
                f"Identifiers list is empty: Nothing remains for processing in task(id={task.request.id})"
            )
            log_run_end_message()
            return

        api = create_api(session=session, collection=collection)
        start_seconds = time.perf_counter()
        total_imported_in_current_task = 0
        while len(identifiers) > 0:
            batch = identifiers[:batch_size]

            try:
                for metadata, circulation in api.availability_by_title_ids(
                    title_ids=batch
                ):
                    process_book(task, session, api, metadata, circulation)
            except (ObjectDeletedError, StaleDataError) as e:
                wait_time = exponential_backoff(task.request.retries)
                task.log.exception(
                    f"Something unexpected went wrong while processing a batch of titles for collection "
                    f'"{collection.name}" task(id={task.request.id} due to {e}. Retrying in {wait_time} seconds.'
                )
                raise task.retry(countdown=wait_time)

            batch_length = len(batch)
            task.log.info(
                f"Imported {batch_length} identifiers for collection ({collection.name}, id={collection_id})"
            )
            total_imported_in_current_task += batch_length
            task.log.info(
                f"Total imported {total_imported_in_current_task} identifiers in current task for collection ({collection.name}, id={collection_id})"
            )

            # remove identifiers processed in previous batch
            identifiers = identifiers[len(batch) :]
            identifiers_list_length = len(identifiers)
            # measure elapsed seconds
            elapsed_seconds = time.perf_counter() - start_seconds

            if elapsed_seconds > target_max_execution_time_in_seconds:
                task.log.info(
                    f"Execution time exceeded max allowable seconds (max={target_max_execution_time_in_seconds}): "
                    f"elapsed seconds={elapsed_seconds}"
                )
                break

    processed_count += total_imported_in_current_task

    task.log.info(
        f"Imported {processed_count} identifiers in run for collection ({collection.name}, id={collection_id})"
    )

    if len(identifiers) > 0:
        task.log.info(
            f"Replacing task to continue importing remaining {len(identifiers)} identifier{'' if len(identifiers) == 1 else 's'} "
            f"for collection ({collection.name}, id={collection.id})"
        )

        raise task.replace(
            import_identifiers.s(
                collection_id=collection.id,
                identifiers=identifiers,
                batch_size=batch_size,
                processed_count=processed_count,
            )
        )
    else:
        log_run_end_message()


def process_book(
    task: Task,
    _db: Session,
    api: Axis360API,
    metadata: Metadata,
    circulation: CirculationData,
) -> None:
    edition, new_edition, license_pool, new_license_pool = api.update_book(
        bibliographic=metadata, availability=circulation
    )

    task.log.info(
        f"Edition (id={edition.id}, title={edition.title}) {'created' if new_edition else 'updated'}. "
        f"License pool (id={license_pool.id}) {'created' if new_license_pool else 'updated'}."
    )


def _redis_lock_list_identifiers_for_import(
    client: Redis, collection_id: int
) -> RedisLock:
    return RedisLock(
        client,
        lock_name=[
            f"ListIdentifiersForImport",
            Collection.redis_key_from_id(collection_id),
        ],
    )


def get_collections_by_protocol(
    task: Task,
    session: Session,
    protocol_class: type[BaseCirculationAPI[SettingsType, LibrarySettingsType]],
) -> list[Collection]:
    registry = task.services.integration_registry.license_providers()
    protocols = registry.get_protocols(protocol_class, default=False)
    collections = [
        collection
        for collection in Collection.by_protocol(session, protocols)
        if collection.id is not None
    ]
    return collections


@shared_task(queue=QueueNames.default, bind=True)
def reap_all_collections(task: Task) -> None:
    """
    A shared task that  kicks off a reap collection task for each Axis 360 collection.
    """
    with task.session() as session:
        for collection in get_collections_by_protocol(task, session, Axis360API):
            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for reaping...'
            )
            reap_collection.delay(collection_id=collection.id)

        task.log.info(f"Finished queuing reap collection tasks.")


@shared_task(queue=QueueNames.default, bind=True)
def reap_collection(
    task: Task, collection_id: int, offset: int = 0, batch_size: int = 25
) -> None:
    """
    Update the license pools associated with a subset of identifiers in a collection
    defined by the offset and batch size.
    """

    start_seconds = time.perf_counter()

    with task.transaction() as session:
        collection = Collection.by_id(session, collection_id)
        if not collection:
            task.log.error(f"Collection not found:  {collection_id} : ignoring...")
            return None

        api = create_api(session=session, collection=collection)

        identifiers = (
            session.scalars(
                select(Identifier)
                .join(Identifier.licensed_through)
                .where(LicensePool.collection == collection)
                .order_by(Identifier.id)
                .limit(batch_size)
                .offset(offset)
            )
            .unique()
            .all()
        )

        identifier_count = len(identifiers)
        if identifier_count > 0:
            api.update_licensepools_for_identifiers(identifiers=identifiers)

    task.log.info(
        f'Reaper updated {identifier_count} books in collection (name="{collection.name}", id={collection.id}.'
    )
    # Requeue at the next offset if the batch of identifiers was full otherwise do nothing since
    # the run is complete.

    task.log.info(
        f"reap_collection task at offset={offset} with {identifier_count} identifiers for collection "
        f'(name="{collection.name}", id={collection.id}): elapsed seconds={time.perf_counter() - start_seconds: 0.2}'
    )

    if identifier_count >= batch_size:
        new_offset = offset + identifier_count

        task.log.info(
            f"Re-queuing reap_collection task at offset={new_offset} for collection "
            f'(name="{collection.name}", id={collection.id}).'
        )

        raise task.replace(
            reap_collection.s(
                collection_id=collection_id,
                offset=new_offset,
                batch_size=batch_size,
            )
        )

    else:
        task.log.info(
            f'Reaping of collection (name="{collection.name}", id={collection.id}) complete.'
        )
