import datetime
from typing import Any

from celery import chord, group, shared_task

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

# IdentifierSetKey =  Sequence[SupportsRedisKey | str | int] | list[str]


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
        If False, only import titles that have been modified since the last import.
    :param page: The "page" to be processed. The page param is a url represented as a string. When starting an import task,
        the value should be None. Defaults to None.
    :param modified_since: Only process titles modified after this datetime. If None,
        will be determined based on import_all flag and last import timestamp.
    :param start_time: The datetime when this import process began. Used to update
        the collection's timestamp only after all pages have been processed. If None,
        will be set to the current time on the first page.
    :param return_identifiers: A running set of identifiers that have been processed so far in this run.
    :param parent_identifiers: A running set of parent identifiers (if not a parent collection)
        that were processed before this run started.
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
            import_all=import_all,
            identifier_set=identifier_set,
            parent_identifier_set=parent_identifier_set,
        )

        if modified_since is None:
            if import_all:
                modified_since = OverdriveImporter.DEFAULT_START_TIME
            else:
                timestamp = importer.get_timestamp()
                modified_since = timestamp.start or OverdriveImporter.DEFAULT_START_TIME

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
        raise task.replace(
            task.s(
                collection_id=collection_id,
                import_all=import_all,
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

    import_collection.s(
        collection_id=collection_id,
        import_all=import_all,
        page=None,
        parent_identifiers=None,
        return_identifiers=True,
        modified_since=modified_since,
        start_time=start_time,
    ).apply_async(
        link=import_children_and_cleanup_chord.s(
            collection_id=collection_id,
            import_all=import_all,
            modified_since=modified_since,
        )
    )
    # ), import_children_and_cleanup_chord.s(collection_id=collection_id, import_all=import_all, modified_since=modified_since).apply_async()


def rehydrate_identifier_set(
    task: Task, identifier_set_info: dict[str, Any]
) -> IdentifierSet:
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
    identifier_set = rehydrate_identifier_set(task, identifier_set_info)
    assert identifier_set.exists()
    identifier_set.delete()
