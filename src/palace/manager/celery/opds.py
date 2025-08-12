from collections.abc import Callable
from typing import Any, TypeVar

from palace.manager.celery.importer import import_key, import_lock
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.integration.license.opds.importer import (
    FeedImportResult,
    OpdsImporter,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection

FeedType = TypeVar("FeedType")


def opds_import_task(
    task: Task,
    collection: Collection,
    importer: OpdsImporter[FeedType, Any],
    url: str | None,
    *,
    post_import_hook: Callable[[FeedImportResult[FeedType]], None] | None = None,
    force: bool = False,
    return_identifiers: bool = False,
) -> IdentifierSet | None:
    redis = task.services.redis().client()
    with import_lock(redis, collection.id).lock():
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
            identifier_set=identifier_set,
            import_even_if_unchanged=force,
        )

        if not import_result:
            task.log.info("Import failed, aborting task.")
            return None

        # If a post-import hook is provided, call it with the import result.
        if post_import_hook:
            post_import_hook(import_result)

    unchanged_publication = import_result.found_unchanged_publication
    next_link = import_result.next_url
    should_continue = not unchanged_publication or return_identifiers
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
            f"Found unchanged publications in feed, stopping import without harvesting the rest of the feed."
        )

    task.log.info("Import complete.")
    return identifier_set
