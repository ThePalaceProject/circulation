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

DEFAULT_BATCH_SIZE: int = 100
DEFAULT_START_TIME = datetime_utc(1970, 1, 1)


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
            queue_collection_import_batches.delay(
                collection_id=collection.id, batch_size=batch_size
            )
            count += 1
        task.log.info(f'Finished queuing {count} collection{"s" if count > 1 else ""}.')


@shared_task(queue=QueueNames.default, bind=True)
def queue_collection_import_batches(
    task: Task,
    collection_id: int,
    import_all: bool = False,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """
    A shared task for queuing batches of identifiers to import an axis collection.
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
            return

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
            batch: list[tuple[Metadata, CirculationData]] = []

            task.log.info(
                f"Starting process of queuing items in collection {collection.name} (id={collection_id} "
                f"for import that have changed since {start_time_of_last_scan}. "
            )
            # start stop watch
            start_seconds = time.perf_counter()

            for bibliographic, circulation in api.recent_activity(
                start_time_of_last_scan
            ):
                batch.append((bibliographic, circulation))
                count = +1
                if len(batch) == batch_size:
                    import_items.delay(items=batch, collection_id=collection_id)
                    batch = []

                # log every 500 items to keep logs relatively lean
                if count % 500 == 0:
                    task.log.info(
                        f"Queued {count} items in batches of {batch_size} for "
                        f'collection: name="{collection.name}" (collection_id={collection_id}).'
                    )

            # queue import_items tasks for any remaining items.
            if len(batch) > 0:
                import_items.delay(items=batch, collection_id=collection_id)

            elapsed_time = time.perf_counter() - start_seconds
            achievements = (
                f"Total items queued for import:  {count}; "
                f"elapsed time: {elapsed_time:0.2f}"
            )
            ts.update(task_run_start_time, utc_now(), achievements)
            # log the end of the run
            task.log.info(
                f"Finished queuing items in collection {collection.name} (id={collection_id} "
                f"for import that have changed since {start_time_of_last_scan}. "
                f"{achievements}"
            )


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
def import_items(
    task: Task, collection_id: int, items: list[tuple[Metadata, CirculationData]]
) -> None:
    """
    This method creates new or updates new editions and license pools for each pair of metadata and circulation data in
    the items list.
    """
    with task.session() as session:
        collection = Collection.by_id(session, id=collection_id)
        api = create_api(session, collection)
        for metadata, circulation in items:
            process_book(task, session, api, metadata, circulation)


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
            f"Queuing reap_collection task at offset={new_offset} for collection "
            f'(name="{collection.name}", id={collection.id}).'
        )
        reap_collection.delay(collection_id, new_offset, batch_size)
    else:
        task.log.info(
            f'Reaping of collection (name="{collection.name}", id={collection.id}) complete.'
        )
