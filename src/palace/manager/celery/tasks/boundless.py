import datetime

from celery import shared_task

from palace.manager.celery.importer import (
    import_all as create_import_tasks,
    import_lock,
)
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.utils import load_from_id
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.importer import BoundlessImporter
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)


@shared_task(queue=QueueNames.default, bind=True)
def import_all_collections(task: Task, *, import_all: bool = False) -> None:
    """
    A shared task that loops through all Boundless Api based collections and kick off an
    import task for each.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            BoundlessApi, registry=registry
        )
        create_import_tasks(
            session.scalars(collection_query).all(),
            import_collection.s(
                import_all=import_all,
            ),
            task.log,
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
    page: int = 1,
    modified_since: datetime.datetime | None = None,
    start_time: datetime.datetime | None = None,
) -> None:
    """
    Run an import for a single Boundless collection.

    This task processes titles from the Boundless Title License API in a paginated
    fashion. When multiple pages are present, the task chains itself using task.replace()
    to process subsequent pages while maintaining the same modified_since timestamp
    and start_time across all pages.

    :param collection_id: The ID of the collection to import
    :param import_all: If True, import all titles regardless of whether they have changed.
        If False, only import titles that have been modified since the last import.
    :param page: The current page number being processed (1-indexed). Defaults to 1.
    :param modified_since: Only process titles modified after this datetime. If None,
        will be determined based on import_all flag and last import timestamp.
    :param start_time: The datetime when this import process began. Used to update
        the collection's timestamp only after all pages have been processed. If None,
        will be set to the current time on the first page.
    :raises PalaceValueError: If page > 1 but modified_since or start_time is None.
        These parameters must be provided when processing pages beyond the first to
        maintain consistency across the paginated import.
    """
    # Validate parameters before acquiring the lock for fail-fast behavior
    if page != 1 and (modified_since is None or start_time is None):
        raise PalaceValueError(
            "modified_since and start_time are required after the first page."
        )

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

        importer = BoundlessImporter(session, collection, registry, import_all)

        if modified_since is None:
            if import_all:
                modified_since = BoundlessImporter.DEFAULT_START_TIME
            else:
                timestamp = importer.get_timestamp(session, collection)
                modified_since = timestamp.start or BoundlessImporter.DEFAULT_START_TIME

        task.log.info(
            f"Boundless import started: '{collection_name}' Page: {page}. Modified since: {modified_since}."
        )

        result = importer.import_collection(
            apply_bibliographic=apply.bibliographic_apply.delay,
            apply_circulation=apply.circulation_apply.delay,
            page=page,
            modified_since=modified_since,
        )

        task.log.info(
            f"Boundless import page complete: '{collection_name}' Page: {result.current_page}/{result.total_pages}. "
            f"Active processed: {result.active_processed} Inactive processed: {result.inactive_processed}."
        )

        if result.next_page is None:
            # We are done. We only update the timestamp once we have processed all pages.
            # To make sure that if we fail, or are interrupted, we re-process any
            # titles we may have missed.
            timestamp = importer.get_timestamp(session, collection)
            timestamp.start = start_time
            timestamp.finish = utc_now()
            task.log.info(
                f"Boundless import complete: '{collection_name}' Total time: {timestamp.elapsed}."
            )

    if result.next_page is not None:
        task.log.info(
            f"Boundless import re-queueing: '{collection_name}' Next page: {result.next_page}."
        )
        raise task.replace(
            task.s(
                collection_id=collection_id,
                import_all=import_all,
                page=result.next_page,
                modified_since=modified_since,
                start_time=start_time,
            )
        )
