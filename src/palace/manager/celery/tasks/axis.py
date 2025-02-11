import time
from datetime import datetime

from celery import shared_task
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from palace.manager.api.axis import Axis360API
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
        for collection in get_collections_by_protocol(task, session, Axis360API):
            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for importing...'
            )
            list_identifiers_for_import.apply_async(
                (collection.id),
                link=import_identifiers.s(
                    collection_id=collection.id, batch_size=batch_size
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
    lock = _redis_lock_queue_collection_import(
        task.services.redis.client(), collection_id
    )
    with lock.lock() as locked:
        if not locked:
            task.log.info(
                f"Skipping collection batch queuing:  {collection_id} because another task holds its lock."
            )
            return None

        with task.transaction() as session:

            collection = Collection.by_id(session, collection_id)

            # retrieve timestamp of last run
            ts = timestamp(
                _db=session,
                default_start_time=DEFAULT_START_TIME,
                service_name=task.name,
                collection=collection,
            )

            # if import_all  use default start date
            if import_all:
                start_time_of_last_scan = DEFAULT_START_TIME
            else:
                # otherwise use the start date of the previous timestamp.
                start_time_of_last_scan = ts.start

            task_run_start_time = utc_now()
            # loop through feed :  for every {batch_size} items, pack them in a list and pass along to sub task for
            # processing
            count = 0
            collection = Collection.by_id(session, collection_id)
            api = create_api(collection, session)
            task.log.info(
                f"Starting process of queuing items in collection {collection.name} (id={collection_id} "
                f"for import that have changed since {start_time_of_last_scan}. "
            )
            # start stop watch
            start_seconds = time.perf_counter()
            title_ids: list[str] = []
            for metadata, circulation in api.recent_activity(start_time_of_last_scan):
                title_ids.append(metadata.primary_identifier.identifier)
                count = +1
            elapsed_time = time.perf_counter() - start_seconds
            achievements = (
                f"Total items queued for import:  {count}; "
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


def create_api(collection, session):
    return Axis360API(session, collection)


def timestamp(
    _db: Session,
    default_start_time: datetime,
    service_name: str,
    collection: Collection,
    default_counter: int = None,
):
    """Find or create a Timestamp.
    A new timestamp will have .finish set to None, since the first
    run is presumably in progress.
    """
    timestamp, new = get_one_or_create(
        _db,
        Timestamp,
        service=service_name,
        service_type=Timestamp.MONITOR_TYPE,
        collection=collection,
        create_method_kwargs=dict(
            start=default_start_time,
            finish=None,
            counter=default_counter,
        ),
    )
    return timestamp


@shared_task(queue=QueueNames.default, bind=True)
def import_identifiers(
    task: Task,
    collection_id: int,
    identifiers: list[str] | None,
    processed_count: int = 0,
    batch_size: int = 25,
    target_max_execution_time_in_seconds: float = TARGET_MAX_EXECUTION_SECONDS,
) -> list[str] | None:
    """
    This method creates new or updates new editions and license pools for each pair of metadata and circulation data in
    the items list.
    """

    if not identifiers:
        task.log.info(
            f"Identifiers list is None: the list_identifiers_for_"
            f"import must have been locked. Ignoring import run for collection_id={collection_id}"
        )
        return None

    with task.session() as session:
        collection = Collection.by_id(session, id=collection_id)
        api = create_api(session, collection)
        batch: list[str] = []
        start_seconds = time.perf_counter()
        total_imported_in_current_task = 0
        identifiers_list_length = len(identifiers)
        while identifiers_list_length > 0:
            batch = identifiers[
                0 : (
                    identifiers_list_length
                    if identifiers_list_length < batch_size
                    else batch_size
                )
            ]

            for metadata, circulation in api.availability_by_title_ids(title_ids=batch):
                process_book(task, session, api, metadata, circulation)

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

    if len(identifiers) > 0:
        requeue_import_identifiers_task(
            batch_size=batch_size,
            collection=collection,
            identifiers=identifiers,
            processed_count=processed_count,
        )
    else:
        task.log.info(
            f"Finished run importing identifiers for collection ({collection.name}, id={collection_id})"
        )

    task.log.info(
        f"Imported {processed_count} identifiers in run for collection ({collection.name}, id={collection_id})"
    )


def requeue_import_identifiers_task(
    task,
    batch_size: int,
    collection: Collection,
    identifiers: list[str],
    processed_count: int,
):
    import_identifiers.delay(
        collection_id=collection.id,
        identifiers=identifiers,
        batch_size=batch_size,
        processed_count=processed_count,
    )
    task.log.info(
        f"Spawned subtask to continue importing remaining {len(identifiers)} "
        f"for collection ({collection.name}, id={collection.id})"
    )


@retry(
    retry=(
        retry_if_exception_type(StaleDataError)
        | retry_if_exception_type(ObjectDeletedError)
    ),
    stop=stop_after_attempt(max_attempt_number=5),
    wait=wait_exponential(multiplier=1, min=1, max=60),
    reraise=True,
)
def process_book(
    task: Task,
    _db: Session,
    api: Axis360API,
    metadata: Metadata,
    circulation: CirculationData,
) -> None:
    with _db.begin_nested():
        edition, new_edition, license_pool, new_license_pool = api.update_book(
            bibliographic=metadata, availability=circulation
        )

    task.log.info(
        f"Edition (id={edition.id}, title={edition.title}) {'created' if new_edition else 'updated'}. "
        f"License pool (id={license_pool.id}) {'created' if new_license_pool else 'updated'}."
    )


def _redis_lock_queue_collection_import(client: Redis, collection_id: int) -> RedisLock:
    return RedisLock(
        client,
        lock_name=f"Axis360QueueCollectionImport-{collection_id}",
    )


def get_collections_by_protocol(
    task: Task, session: Session, protocol_class
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
    A shared task that loops through all Axis360 Api based collections and kick off an
    import task for each.
    """
    with task.session() as session:
        for collection in get_collections_by_protocol(task, session, Axis360API):
            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for reaping...'
            )
            reap_collection.delay(collection_id=collection.id)

        task.log.info(f"Finished queuing collection for reaping.")


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
        api = create_api(session, collection)

        identifiers = (
            session.query(Identifier)
            .join(Identifier.licensed_through)
            .filter(LicensePool.collection == collection)
            .order_by(Identifier.id)
            .limit(batch_size)
            .offset(offset)
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
        requeue_reap_collection(
            batch_size=batch_size, collection_id=collection.id, new_offset=new_offset
        )
        task.log.info(
            f"Queued reap_collection task at offset={new_offset} for collection "
            f'(name="{collection.name}", id={collection.id}).'
        )
    else:
        task.log.info(
            f'Reaping of collection (name="{collection.name}", id={collection.id}) complete.'
        )


def requeue_reap_collection(batch_size: int, collection_id: int, new_offset: int):
    reap_collection.delay(
        collection_id=collection_id, new_offset=new_offset, batch_size=batch_size
    )
