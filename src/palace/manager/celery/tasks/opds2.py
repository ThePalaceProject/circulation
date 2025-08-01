from celery import shared_task
from celery.canvas import Signature

from palace.manager.celery import importer
from palace.manager.celery.importer import import_key, import_lock
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply, identifiers
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.opds2.importer import (
    importer_from_collection,
)
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.resource import Hyperlink


@shared_task(queue=QueueNames.default, bind=True)
def import_all(task: Task, force: bool = False) -> None:
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(OPDS2API, registry=registry)
        importer.import_all(
            session,
            collection_query,
            import_collection.s(
                force=force,
            ),
            task.log,
        )


@shared_task(queue=QueueNames.default, bind=True)
def import_collection(
    task: Task,
    collection_id: int,
    url: str | None = None,
    *,
    force: bool = False,
    return_identifiers: bool = False,
) -> IdentifierSet | None:
    redis = task.services.redis().client()
    with import_lock(redis, collection_id).lock(), task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        registry = task.services.integration_registry().license_providers()

        # Create a set to store identifiers that will be imported
        identifier_set = (
            IdentifierSet(redis, import_key(collection_id, task.request.id))
            if return_identifiers
            else None
        )

        importer = importer_from_collection(collection, registry)
        feed_page = importer.get_feed(url)

        # Update the token auth endpoint for the collection.
        if token_auth_link := feed_page.links.get(rel=Hyperlink.TOKEN_AUTH):
            changed = OPDS2API.update_collection_token_auth_url(
                collection, token_auth_link.href
            )
            if changed:
                session.commit()

        unchanged_publication = importer.import_feed(
            session,
            feed_page,
            collection,
            apply_bibliographic=apply.bibliographic_apply.delay,
            identifier_set=identifier_set,
            import_even_if_unchanged=force,
        )

    next_link = importer.next_page(feed_page)
    should_continue = not unchanged_publication or return_identifiers
    if next_link is not None and should_continue:
        # This page is complete, but there are more pages to import, so we requeue ourselves with the
        # next page URL.
        raise task.replace(
            import_collection.s(
                collection_id=collection_id,
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


def import_and_reap_not_found_chord(
    collection_id: int, force: bool = False
) -> Signature:
    """
    Creates a Celery chord that imports a collection and then reaps identifiers that were not found in the feed.
    """
    return identifiers.create_mark_unavailable_chord(
        collection_id,
        import_collection.s(
            collection_id=collection_id, force=force, return_identifiers=True
        ),
    )
