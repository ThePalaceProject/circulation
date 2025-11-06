import datetime
from typing import Any

from celery import chain, chord, group, shared_task

from palace.manager.celery.importer import import_key, import_lock
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.overdrive.api import BookInfoEndpoint
from palace.manager.integration.license.overdrive.importer import OverdriveImporter
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)


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
) -> IdentifierSet | None:
    """
    Run an import for a single Overdrive collection.

    This task processes identifiers from the OverDrive API in a paginated
    fashion. When multiple pages are present, the task chains itself using task.replace()
    to process subsequent pages while maintaining the same modified_since timestamp
    and start_time across all pages.

    :param collection_id: The ID of the collection to import
    :param import_all: If True, import all titles regardless of whether they have changed.
        If False, only import titles that have changed since the modified_since date.
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
    """
    redis = task.services.redis().client()
    registry = task.services.integration_registry().license_providers()

    if start_time is None:
        start_time = utc_now()

    with (
        import_lock(redis, collection_id).lock(),
        task.transaction() as session,
    ):
        collection = load_from_id(session, Collection, collection_id)
        collection_name = collection.name

        identifier_set = (
            IdentifierSet(redis, import_key(collection.id, task.request.id))
            if return_identifiers
            else None
        )

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
) -> None:
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

    .. note::
       This task does not return a value. Results are tracked through the
       linked chord and cleanup tasks.
    """

    chain(
        import_collection.s(
            collection_id=collection_id,
            import_all=import_all,
            page=None,
            parent_identifiers=None,
            return_identifiers=True,
            modified_since=modified_since,
            start_time=start_time,
        ),
        import_children_and_cleanup_chord.s(
            collection_id=collection_id,
            import_all=import_all,
            modified_since=modified_since,
        ),
    )()


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
    It deletes the temporary Redis set that was used to share identifiers between
    parent and child collection imports. This cleanup prevents Redis memory leaks
    from accumulating identifier sets.

    The task asserts that the set exists before attempting deletion to catch
    cases where the set was unexpectedly removed or never created.

    :param identifier_set_info: Serialized identifier set info.
                                Format: {"key": ["redis", "key", "parts"]}
    :raises AssertionError: If the identifier set doesn't exist in Redis

    .. note::
       This task is designed to be used as a chord callback and should not
       be called directly in most cases.
    """
    identifier_set = rehydrate_identifier_set(task, identifier_set_info)
    if not identifier_set.exists():
        task.log.warning(
            f"Identifier set (key={identifier_set._key}) does not exist in Redis. Skipping cleanup."
        )
    else:
        identifier_set.delete()
