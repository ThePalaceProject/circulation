import datetime
from typing import Any, Literal, TypedDict, TypeGuard
from uuid import uuid4

from celery import chain, chord, group, shared_task
from celery.exceptions import Ignore
from sqlalchemy import select

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.importer import (
    import_all as create_import_tasks,
    import_key,
    import_workflow_lock,
    reap_workflow_lock,
)
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
)
from palace.manager.integration.license.overdrive.importer import OverdriveImporter
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)

IMPORT_SKIPPED: str = "import_skipped"


class ImportSkippedPayload(TypedDict):
    """Payload returned when import is skipped (workflow lock already held)."""

    import_skipped: Literal[True]


class ImportRouterResult(TypedDict, total=False):
    """Result of import_result_router: chord_id when chord runs, import_skipped when skipped."""

    import_skipped: Literal[True]
    chord_id: str | None


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_collection(
    task: Task,
    collection_id: int,
    *,
    import_all: bool = False,
    page: str | None = None,
    modified_since: datetime.datetime | None = None,
    start_time: datetime.datetime | None = None,
    return_identifiers: bool = True,
    parent_identifiers: dict[str, Any] | None = None,
    lock_value: str | None = None,
) -> IdentifierSet | ImportSkippedPayload | None:
    """
    Run an import for a single Overdrive collection.

    This task processes identifiers from the OverDrive API in a paginated
    fashion. When multiple pages are present, the task chains itself using task.replace()
    to process subsequent pages while maintaining the same modified_since timestamp
    and start_time across all pages.

    :param collection_id: The ID of the collection to import
    :param import_all: If True, import all titles regardless of whether they have changed.
        If False, import titles that have changed since the modified_since date.
    :param page: The "page" to be processed. The page param is a url represented as a string. A None value means the
        import will start from the beginning of the set.
    :param modified_since: Only process titles modified after this datetime. This field parameter is ignored if
        import_all is True. If import_all is False and modified_since is None, then only titles after the last import
        timestamp's start time will be imported. If there is no prior timestamp, all titles will be imported.
         Finally, if import_all is False and modified_since is not None, then
        only titles modified after modified_since date will be imported.
    :param start_time: The datetime when this import process began. Used to update
        the collection's timestamp only after all pages have been processed. If None,
        will be set to the current time on the first page.
    :param return_identifiers: A running set of identifiers that have been processed so far in this run.
    :param parent_identifiers: A running set of parent identifiers (if not a parent collection)
        that were processed before this run started. This value is a serialized representation of the
        parent_identifier IdentifierSet.
    :param lock_value: UUID identifying this import workflow. Passed between pages to hold the workflow lock
        across page boundaries. Generated on the first page when None.
    :return: IdentifierSet when import completes and return_identifiers is True; None when
        return_identifiers is False or collection is marked for deletion; {IMPORT_SKIPPED: True}
        when the workflow lock is held and another import is already in progress.
    """
    redis = task.services.redis().client()
    registry = task.services.integration_registry().license_providers()

    if start_time is None:
        start_time = utc_now()

    # Both page and lock_value are None only on the first page of a fresh import.
    # They are always set together when task.replace() chains to the next page.
    is_first_page = page is None and lock_value is None
    if lock_value is None:
        lock_value = str(uuid4())

    workflow_lock = import_workflow_lock(redis, collection_id, lock_value)

    # Ignore is raised by task.replace() and Retry is raised by autoretry_for exceptions.
    # Neither should release the workflow lock: replace() hands it to the next page task,
    # and retries should continue holding the lock across the backoff window.
    with workflow_lock.lock(
        raise_when_not_acquired=False,
        ignored_exceptions=(Ignore, BadResponseException, RequestTimedOut),
    ) as workflow_lock_acquired:
        if not workflow_lock_acquired and is_first_page:
            task.log.warning(
                f"OverDrive import skipped for collection {collection_id}: "
                "another import is already in progress."
            )
            return _import_skipped_payload()
        if not workflow_lock_acquired and not is_first_page:
            task.log.warning(
                f"OverDrive import for collection {collection_id}: workflow lock expired "
                "between pages; continuing (another import may be running)."
            )

        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            identifier_set = (
                IdentifierSet(redis, import_key(collection.id, task.request.id))
                if return_identifiers
                else None
            )

            if collection.marked_for_deletion:
                task.log.warning(
                    f"This collection is marked for deletion. "
                    f"Skipping import of '{collection_name}'."
                )
                return identifier_set

            parent_identifier_set = (
                rehydrate_identifier_set(task, parent_identifiers)
                if parent_identifiers
                else None
            )

            importer = OverdriveImporter(
                db=session,
                collection=collection,
                registry=registry,
                identifier_set=identifier_set,
                parent_identifier_set=parent_identifier_set,
            )

            if modified_since is None:
                if import_all:
                    modified_since = None
                else:
                    timestamp = importer.get_timestamp()
                    modified_since = timestamp.start

            task.log.info(
                f"OverDrive import started: '{collection_name}' Modified since: {modified_since}, "
                f"page: {None if not page else page}"
            )

            endpoint = None if not page else BookInfoEndpoint(page)

            result = importer.import_collection(
                apply_bibliographic=apply.bibliographic_apply.delay,
                apply_circulation=apply.circulation_apply.delay,
                endpoint=endpoint,
                modified_since=modified_since,
            )

            task.log.info(
                f"OverDrive import page complete: '{collection_name}' Page: {result.current_page}. "
                f"Processed: {result.processed_count}. "
            )

            if identifier_set:
                task.log.info(
                    f"OverDrive collection import '{collection_name}': Total processed in run so far: {identifier_set.len()}"
                )

            if result.next_page is None:
                # We are done. We only update the timestamp once we have processed all pages.
                # To make sure that if we fail or are interrupted, we re-process any
                # titles we may have missed.
                timestamp = importer.get_timestamp()
                timestamp.start = start_time
                timestamp.finish = utc_now()
                task.log.info(
                    f"OverDrive import complete: '{collection_name}' Total time: {timestamp.elapsed}."
                )

        if result.next_page is not None:
            task.log.info(
                f"OverDrive import re-queueing: '{collection_name}' Next page: {result.next_page}."
            )
            # Serialize parent_identifier_set to dict for passing to next task
            serialized_parent_identifiers = (
                parent_identifier_set.__json__() if parent_identifier_set else None
            )
            raise task.replace(
                task.s(
                    collection_id=collection_id,
                    import_all=import_all,
                    parent_identifiers=serialized_parent_identifiers,
                    return_identifiers=return_identifiers,
                    page=result.next_page.url,
                    modified_since=modified_since,
                    start_time=start_time,
                    lock_value=lock_value,
                )
            )
        else:
            return identifier_set


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_collection_group(
    task: Task,
    collection_id: int,
    *,
    import_all: bool = False,
    modified_since: datetime.datetime | None = None,
    start_time: datetime.datetime | None = None,
) -> dict[str, Any] | ImportSkippedPayload:
    """Import an Overdrive collection and all its child (Advantage) collections.

    This task orchestrates the import of a parent Overdrive collection and chains
    the import of any associated Overdrive Advantage (child) collections. It uses
    Celery's chord pattern to run child imports in parallel after the parent completes,
    and then cleans up the shared identifier set.

    Workflow:
        1. Imports the parent collection
        2. Passes the parent's identifier set to child collections
        3. Imports all child collections in parallel (if any exist)
        4. Cleans up the shared identifier set after all imports complete

    :param collection_id: The ID of the parent collection to import
    :param import_all: If True, import all titles regardless of change status.
                       If False, only import changed titles based on modified_since.
    :param modified_since: Only process titles modified after this datetime.
                          See import_collection docstring for detailed behavior.
    :param start_time: The datetime when this import began. Used to update the
                      collection's timestamp. If None, uses current time.
    :return: Dictionary containing the chain_id for tracking the async import chain,
             or an import-skipped payload if another workflow is already in progress.
    """
    redis = task.services.redis().client()
    # Defense-in-depth: skip chain creation if a workflow is already running.
    # This prevents redundant chains from being queued when the workflow lock
    # expires between pages and a new Beat tick fires before import_collection
    # can re-acquire and enforce the first-page guard itself.
    if import_workflow_lock(redis, collection_id, str(uuid4())).locked():
        task.log.info(
            f"OverDrive import skipped for collection {collection_id}: "
            "another import is already in progress (skipping at group level)."
        )
        return _import_skipped_payload()

    result = chain(
        import_collection.s(
            collection_id=collection_id,
            import_all=import_all,
            page=None,
            parent_identifiers=None,
            return_identifiers=True,
            modified_since=modified_since,
            start_time=start_time,
        ),
        import_result_router.s(
            collection_id=collection_id,
            import_all=import_all,
            modified_since=modified_since,
        ),
    )()
    return {"chain_id": result.id}


def _is_import_skipped(
    result: IdentifierSet | dict[str, Any] | None,
) -> TypeGuard[dict[str, Any]]:
    """Type guard: True when result is the skip payload."""
    return isinstance(result, dict) and result.get(IMPORT_SKIPPED) is True


def _import_skipped_payload() -> ImportSkippedPayload:
    """Build the skip payload for return values."""
    return {"import_skipped": True}


@shared_task(queue=QueueNames.default, bind=True)
def import_result_router(
    task: Task,
    import_result: IdentifierSet | dict[str, Any] | None,
    collection_id: int,
    import_all: bool,
    modified_since: datetime.datetime | None,
) -> ImportRouterResult:
    """Route import result to child imports or short-circuit when skipped.

    This task receives the result of import_collection and either invokes the
    child-import chord (when the import ran) or returns early (when the import
    was skipped due to another import already in progress).

    :param import_result: Result from import_collection. Either an IdentifierSet
        (or its serialized form when passed through Celery), ImportSkippedPayload
        when skipped, or None when the import returned no identifier set.
    :param collection_id: The parent collection ID.
    :param import_all: Whether to import all titles in children.
    :param modified_since: Only import titles modified after this datetime.
    :return: {"chord_id": "..."} when chord is invoked, {IMPORT_SKIPPED: True}
        when skipped, or {"chord_id": None} when import_result is None.
    """
    if _is_import_skipped(import_result):
        task.log.info(
            f"OverDrive import skipped for collection {collection_id}: "
            "skipping child imports (another import already in progress)."
        )
        skip_result: ImportRouterResult = {"import_skipped": True}
        return skip_result

    if import_result is None:
        task.log.warning(
            f"OverDrive import for collection {collection_id}: no identifier set "
            "returned; skipping child imports."
        )
        return {"chord_id": None}

    identifier_set_info = (
        import_result.__json__()
        if isinstance(import_result, IdentifierSet)
        else import_result
    )
    async_res = import_children_and_cleanup_chord.apply_async(
        args=[identifier_set_info, collection_id, import_all, modified_since],
    )
    return {"chord_id": async_res.id}


def rehydrate_identifier_set(
    task: Task, identifier_set_info: dict[str, Any]
) -> IdentifierSet:
    """Reconstruct an IdentifierSet from its serialized representation.

    This helper function takes a dictionary containing identifier set metadata
    (specifically the Redis key) and recreates the IdentifierSet object that
    can be used to access the data in Redis.

    :param task: The Celery task instance (provides access to Redis client)
    :param identifier_set_info: Dictionary containing the identifier set's key
                                Format: {"key": ["key", "parts"]}
    :return: Reconstructed IdentifierSet connected to Redis
    """
    return IdentifierSet(task.services.redis().client(), identifier_set_info["key"])


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_children_and_cleanup_chord(
    task: Task,
    identifier_set_info: dict[str, Any],
    collection_id: int,
    import_all: bool,
    modified_since: datetime.datetime,
) -> dict[str, Any]:
    """Import child (Advantage) collections and clean up the parent identifier set.

    This task is called as the callback/link after a parent collection import completes.
    It receives the parent collection's identifier set and uses a Celery chord to:

    1. Import all child Overdrive Advantage collections in parallel, passing the
       parent's identifier set to optimize metadata fetching (children skip books
       already imported by the parent)
    2. After all child imports complete, remove the shared identifier set from Redis

    The chord pattern ensures the cleanup (step 2) only runs after all child imports
    have finished, preventing premature deletion of the shared identifier set.

    :param identifier_set_info: Serialized parent identifier set info from the parent import.
                                Format: {"key": ["redis", "key", "parts"]}
    :param collection_id: The ID of the parent collection whose children to import
    :param import_all: If True, import all titles in children regardless of change status.
                      If False, only import changed titles.
    :param modified_since: Only process titles modified after this datetime in child collections
    :return: Dictionary containing the chord ID for tracking: {"chord_id": "..."}

    .. note::
       If the parent collection has no children, the chord will still be created
       but with an empty group, and cleanup will proceed normally.
    """
    with task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        identifier_set = rehydrate_identifier_set(task, identifier_set_info)
        header = group(
            [
                import_collection.si(
                    collection_id=c.id,
                    page=None,
                    import_all=import_all,
                    modified_since=modified_since,
                    parent_identifiers=identifier_set,
                )
                for c in collection.children
            ]
        )
        async_res = chord(
            header=header,
            body=remove_identifier_set.si(identifier_set_info=identifier_set_info),
        ).apply_async()
        return {"chord_id": async_res.id}


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def remove_identifier_set(task: Task, identifier_set_info: dict[str, Any]) -> None:
    """Clean up a temporary identifier set from Redis after import completes.

    This task is used as the callback body of the chord in import_children_and_cleanup_chord.
    It deletes the temporary Redis set used to share identifiers between
    parent and child collection imports.  If the set doesn't exist, the operation will
    still succeed.


    :param identifier_set_info: Serialized identifier set info.
                                Format: {"key": ["redis", "key", "parts"]}
    """
    identifier_set = rehydrate_identifier_set(task, identifier_set_info)
    if not identifier_set.exists():
        task.log.warning(
            f"Identifier set (key={identifier_set._key}) does not exist in Redis. Skipping cleanup."
        )
    else:
        identifier_set.delete()


@shared_task(queue=QueueNames.default, bind=True)
def import_all_collections(task: Task, *, import_all: bool = False) -> None:
    """
    A shared task that loops through all OverDrive parent collections and kick off an
    import task for each.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OverdriveAPI, registry=registry
        ).where(Collection.parent == None)
        create_import_tasks(
            session.scalars(collection_query).all(),
            import_collection_group.s(
                import_all=import_all,
            ),
            task.log,
        )


@shared_task(queue=QueueNames.default, bind=True)
def reap_all_collections(task: Task) -> None:
    """
    Queue a reap task for every Overdrive collection.

    Includes both parent collections and Advantage (child) collections, since titles
    can be removed from either independently.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OverdriveAPI, registry=registry
        )
        for collection in session.scalars(collection_query):
            reap_collection.delay(collection.id)


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def reap_collection(
    task: Task,
    collection_id: int,
    *,
    offset: int = 0,
    batch_size: int = 50,
    lock_value: str | None = None,
) -> None:
    """
    Check for books that are in the local collection but have left our Overdrive collection.

    Processes identifiers in batches, re-queuing itself via task.replace() until all
    identifiers have been checked. A Redis workflow lock prevents overlapping runs for
    the same collection; the lock auto-expires after 2 hours if the process dies.

    :param collection_id: The ID of the Overdrive collection to reap.
    :param offset: The last Identifier.id processed; used to resume across batches.
    :param batch_size: Number of identifiers to process per batch.
    :param lock_value: UUID identifying this reap workflow. Generated on the first batch
        when None, then passed to each subsequent batch to hold the lock across replacements.
    """
    redis = task.services.redis().client()

    is_first_batch = lock_value is None
    if lock_value is None:
        lock_value = str(uuid4())

    workflow_lock = reap_workflow_lock(redis, collection_id, lock_value)

    with workflow_lock.lock(
        raise_when_not_acquired=False,
        ignored_exceptions=(Ignore, BadResponseException, RequestTimedOut),
    ) as workflow_lock_acquired:
        if not workflow_lock_acquired and is_first_batch:
            task.log.warning(
                f"Overdrive reaper skipped for collection {collection_id}: "
                "another reap is already in progress."
            )
            return
        if not workflow_lock_acquired and not is_first_batch:
            task.log.warning(
                f"Overdrive reaper for collection {collection_id}: workflow lock expired "
                "between batches; continuing (another reap may be running)."
            )

        new_offset = 0
        processed_count = 0
        collection_name = None

        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            identifiers = (
                session.execute(
                    select(Identifier)
                    .join(Identifier.licensed_through)
                    .where(
                        LicensePool.collection_id == collection_id,
                        Identifier.id > offset,
                    )
                    .order_by(Identifier.id)
                    .limit(batch_size)
                )
                .scalars()
                .all()
            )

            if not identifiers:
                task.log.info(
                    f"Overdrive reaper complete for collection '{collection_name}'."
                )
                return

            api = OverdriveAPI(session, collection)
            for identifier in identifiers:
                api.update_licensepool(identifier.identifier)

            new_offset = identifiers[-1].id
            processed_count = len(identifiers)

        task.log.info(
            f"Overdrive reaper: processed {processed_count} identifiers for "
            f"collection '{collection_name}' (offset: {offset} -> {new_offset})."
        )

        if processed_count == batch_size:
            raise task.replace(
                task.s(
                    collection_id=collection_id,
                    offset=new_offset,
                    batch_size=batch_size,
                    lock_value=lock_value,
                )
            )
