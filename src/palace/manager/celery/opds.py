from collections.abc import Callable
from typing import Any

from palace.manager.celery.importer import import_key, workflow_lock_guard
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.integration.license.opds.importer import (
    FeedImportResult,
    OpdsImporter,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection


def opds_import_task[FeedType](
    task: Task,
    collection: Collection,
    importer: OpdsImporter[FeedType, Any],
    url: str | None,
    *,
    label: str,
    post_import_hook: Callable[[FeedImportResult[FeedType]], None] | None = None,
    force: bool = False,
    return_identifiers: bool = False,
) -> IdentifierSet | None:
    """
    Use the supplied importer to import an OPDS feed.

    This function provides the shared logic for our OPDS import tasks. It handles requeuing
    itself, calling a post-import hook (if provided), and returning a set of identifiers that were
    imported (if requested). Concurrency is governed by
    :func:`~palace.manager.celery.importer.workflow_lock_guard`, which holds a per-collection
    Redis lock across both ``task.replace()`` calls and Celery retries so at most one import
    runs per collection at a time; a run that finds the lock already held is skipped.

    :param task: The calling celery task.
    :param collection: The collection to import into.
    :param importer: The OpdsImporter to use for the import operation.
    :param url: The URL to import. If None, the importer's default URL will be used.
    :param label: Human-readable name for the importing task (e.g. ``"OPDS2 import"``),
        used in the workflow-lock skip warning so blocked runs identify their source.
    :param post_import_hook: A callable that will be called with the import result after each page is imported.
    :param force: If True, import even if the feed hasn't changed since the last import.
    :param return_identifiers: Return a set containing all the identifiers that were found in the feed.

    :return: An IdentifierSet if return_identifiers is True, otherwise None.
    """
    with workflow_lock_guard(task, collection.id, label=label) as proceed:
        if not proceed:
            return None

        redis = task.services.redis().client()
        # Create a set to store identifiers that will be imported
        identifier_set = (
            IdentifierSet(redis, import_key(collection.id, task.request.id))
            if return_identifiers
            else None
        )

        import_result = importer.import_feed(
            collection,
            url,
            apply_bibliographic=apply.bibliographic_apply.delay,
            apply_circulation=apply.circulation_apply.delay,
            identifier_set=identifier_set,
            import_even_if_unchanged=force,
        )

        if not import_result:
            task.log.info(
                f"Import failed, aborting task for collection '{collection.name}' (id={collection.id})"
            )
            return None

        # If a post-import hook is provided, call it with the import result.
        if post_import_hook:
            post_import_hook(import_result)

        unchanged_publication = import_result.found_unchanged_publication
        next_link = import_result.next_url
        should_continue = not unchanged_publication or return_identifiers or force
        if next_link is not None and should_continue:
            # This page is complete, but there are more pages to import, so we requeue ourselves with the
            # next page URL.
            raise task.replace(
                task.s(
                    collection_id=collection.id,
                    url=next_link,
                    force=force,
                    return_identifiers=return_identifiers,
                )
            )

        if not should_continue:
            task.log.info(
                f"Found unchanged publications in feed, stopping import without harvesting the rest of the feed"
                f" for collection '{collection.name}' (id={collection.id}) "
            )

        task.log.info(
            f"Import complete for collection '{collection.name}' (id={collection.id})"
        )
        return identifier_set
