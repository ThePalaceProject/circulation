import time
from datetime import datetime

from celery import shared_task
from psycopg2.errors import DeadlockDetected
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from palace.manager.api.axis import Axis360API
from palace.manager.api.circulation import (
    BaseCirculationAPI,
    LibrarySettingsType,
    SettingsType,
)
from palace.manager.celery.task import Task
from palace.manager.core.exceptions import IntegrationException
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
from palace.manager.util.http import BadResponseException
from palace.manager.util.log import pluralize

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
                countdown=count * 5,  # stagger the execution of the collection import
                # in order to minimize chance of deadlocks caused by
                # simultaneous updates to the metadata when CM has multiple
                # axis collections configured with overlapping content
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

            collection_name = collection.name

            if not locked:
                # we want to log
                task.log.warning(
                    f'Skipping list_identifiers_for_import for "{collection_name}"({collection_id}) because another '
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
            if not _check_api_credentials(task, collection, api):
                return None

            task.log.info(
                f"Starting process of queuing items in collection {collection_name} (id={collection_id} "
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
                f"Finished listing identifiers in collection {collection_name} (id={collection_id} "
                f"for import that have changed since {start_time_of_last_scan}. "
                f"{achievements}"
            )

            return title_ids


def create_api(
    collection: Collection, session: Session, bearer_token: str | None = None
) -> Axis360API:
    return Axis360API(session, collection, bearer_token)


def _check_api_credentials(task: Task, collection: Collection, api: Axis360API) -> bool:
    # Try to get a bearer token, to make sure the collection is configured correctly.
    try:
        api.bearer_token()
        return True
    except BadResponseException as e:
        if e.response.status_code == 401:
            task.log.error(
                f"Failed to authenticate with Axis 360 API for collection {collection.name} "
                f"(id={collection.id}). Please check the collection configuration."
            )
            return False
        raise


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


def _check_if_deadlock(e: Exception) -> None:
    if isinstance(e, OperationalError):
        if not isinstance(e.orig, DeadlockDetected):
            raise e


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def import_identifiers(
    task: Task,
    identifiers: list[str] | None,
    collection_id: int,
    processed_count: int = 0,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> None:
    """
    This method creates or updates editions and license pools for each identifier in the list of identifiers.
    It will query the axis availability api in batches of {batch_size} IDs and process each result in a single database
    transaction.  If it has not finished processing the list of identifiers, it will requeue the task with the
    remaining unprocessed identifiers.
    """
    count = 0
    total_imported_in_current_task = 0

    def log_run_end_message() -> None:
        task.log.info(
            f"Finished importing identifiers for collection ({collection_name}, id={collection_id}), "
            f"task(id={task.request.id})"
        )

    start_seconds = time.perf_counter()

    with task.transaction() as session:
        collection = Collection.by_id(session, id=collection_id)
        if not collection:
            task.log.error(f"Collection not found:  {collection_id} : ignoring...")
            return

        collection_name = collection.name

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
        bearer_token = api.bearer_token()
        identifier_batch = identifiers[:batch_size]

        try:
            circ_data = [
                (metadata, circulation)
                for metadata, circulation in api.availability_by_title_ids(
                    title_ids=identifier_batch
                )
            ]
        except IntegrationException as e:
            wait_time = exponential_backoff(task.request.retries)
            task.log.exception(
                f"Something unexpected went wrong while retrieving a batch of titles for collection "
                f'"{collection_name}" task(id={task.request.id} due to {e}. Retrying in {wait_time} seconds.'
            )
            raise task.retry(countdown=wait_time)

    for metadata, circulation in circ_data:
        with task.transaction() as session:
            collection = Collection.by_id(session, id=collection_id)
            api = create_api(session=session, collection=collection, bearer_token=bearer_token)  # type: ignore[arg-type]
            try:
                process_book(task, session, api, metadata, circulation)
                total_imported_in_current_task += 1
            except (ObjectDeletedError, StaleDataError, OperationalError) as e:
                _check_if_deadlock(e)

                wait_time = exponential_backoff(task.request.retries)
                task.log.exception(
                    f"Something unexpected went wrong while processing a batch of titles for collection "
                    f'"{collection_name}" task(id={task.request.id} due to {e}. Retrying in {wait_time} seconds.'
                )
                raise task.retry(countdown=wait_time)

    task.log.info(
        f"Total imported {total_imported_in_current_task} identifiers in current task"
        f" for collection ({collection_name}, id={collection_id})"
    )

    # remove the processed identifiers from the list
    identifiers = identifiers[len(identifier_batch) :]
    identifiers_list_length = len(identifiers)

    elapsed_seconds = time.perf_counter() - start_seconds

    task.log.info(
        f'Imported {total_imported_in_current_task} books into collection(name="{collection_name}", '
        f"id={collection_id} in {elapsed_seconds:.2f} secs"
    )

    processed_count += total_imported_in_current_task

    if len(identifiers) > 0:
        task.log.info(
            f"Imported {processed_count} identifiers so far in run for "
            f"collection ({collection_name}, id={collection_id})"
        )
        task.log.info(
            f"Replacing task to continue importing remaining "
            f'{pluralize(identifiers_list_length, "identifier")} '
            f"for collection ({collection_name}, id={collection_id})"
        )

        raise task.replace(
            import_identifiers.s(
                collection_id=collection_id,
                identifiers=identifiers,
                batch_size=batch_size,
                processed_count=processed_count,
            )
        )
    else:
        task.log.info(
            f"Import run complete for collection ({collection_name}, id={collection_id}:  "
            f"{processed_count} identifiers imported successfully"
        )
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
        count = 0
        for collection in get_collections_by_protocol(task, session, Axis360API):
            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for reaping...'
            )
            reap_collection.apply_async(
                kwargs={"collection_id": collection.id},
                countdown=count * 5,  # stagger the execution of the collection import
                # in order to minimize chance of deadlocks caused by
                # simultaneous updates to the metadata when CM has multiple
                # axis collections configured with overlapping content
            )
            count += 1

        task.log.info(f"Finished queuing all reap_collection tasks.")


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def reap_collection(
    task: Task, collection_id: int, offset: int = 0, batch_size: int = 25
) -> None:
    """
    Update the editions and license pools (and in the process reap where appropriate)
    associated with a collection.  This task will process {batch_size} books (each in
    a separate task) and requeue itself for further processing.
    """

    start_seconds = time.perf_counter()

    with task.transaction() as session:
        collection = Collection.by_id(session, collection_id)
        if not collection:
            task.log.error(f"Collection not found:  {collection_id} : ignoring...")
            return

        collection_name = collection.name

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

    bearer_token: str | None = None

    for identifier in identifiers:
        with task.transaction() as session:
            identifier = session.merge(identifier)
            collection = Collection.by_id(session, collection_id)
            # We just checked that the collection exists, so it should still exist. Assert
            # that is does for the sake of the type checker.
            assert collection is not None
            try:
                api = create_api(
                    session=session, collection=collection, bearer_token=bearer_token
                )
                if not _check_api_credentials(task, collection, api):
                    return

                # store the bearer token for subsequent api calls so that we're minimizing the calls for fresh bearer
                # tokens since every new api instance requires a new bearer token.
                bearer_token = api.bearer_token()

                api.update_licensepools_for_identifiers(identifiers=[identifier])
            except (IntegrationException, OperationalError) as e:
                _check_if_deadlock(e)
                wait_time = exponential_backoff(task.request.retries)
                task.log.exception(
                    f"Something unexpected went wrong while updating license pools for identifier({identifier}) "
                    f'in collection("{collection_name}") task(id={task.request.id} due to {e}. '
                    f"Retrying in {wait_time} seconds."
                )
                raise task.retry(countdown=wait_time)

    task.log.info(
        f'Reaper updated {len(identifiers)} books in collection (name="{collection_name}", id={collection_id}.'
    )
    # Requeue at the next offset if the batch if identifiers list was full otherwise do nothing since
    # the run is complete.

    task.log.info(
        f"reap_collection task at offset={offset} with {len(identifiers)} identifiers for collection "
        f'(name="{collection_name}", id={collection_id}): elapsed seconds={time.perf_counter() - start_seconds: 0.2}'
    )

    if len(identifiers) >= batch_size:
        new_offset = offset + len(identifiers)

        task.log.info(
            f"Re-queuing reap_collection task at offset={new_offset} for collection "
            f'(name="{collection_name}", id={collection_id}).'
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
            f'Reaping of collection (name="{collection_name}", id={collection_id}) complete.'
        )
