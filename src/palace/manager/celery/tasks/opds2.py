from celery import shared_task
from celery.canvas import Signature

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply, identifiers
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.opds.opds2 import OPDS2API, OPDS2Importer
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection


@shared_task(queue=QueueNames.default, bind=True)
def import_all(task: Task, force: bool = False) -> None:
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        for collection in session.execute(
            Collection.select_by_protocol(OPDS2API, registry=registry)
        ).scalars():
            import_collection.delay(
                collection_id=collection.id,
                force=force,
            )

            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for importing...'
            )


def _key(collection_id: int, *additional: str) -> list[str]:
    """
    Generate a Redis key for the given collection ID.
    """
    return [
        "Opds2",
        "ImportCollection",
        Collection.redis_key_from_id(collection_id),
        *additional,
    ]


def _lock(client: Redis, collection_id: int) -> RedisLock:
    """
    Create a lock for the given collection.

    This makes sure only one task is importing data for the collection
    at a time.
    """
    return RedisLock(client, _key(collection_id))


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
    with _lock(redis, collection_id).lock(), task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        registry = task.services.integration_registry().license_providers()

        # Create a set to store identifiers that will be imported
        identifier_set = (
            IdentifierSet(redis, _key(collection_id, task.request.id))
            if return_identifiers
            else None
        )

        importer = OPDS2Importer.from_collection(collection, registry)
        feed_page = importer.get_feed(url)

        # Check if we need to update integration context
        if importer.update_integration_context(feed_page, collection):
            session.commit()

        publications = importer.extract_feed_data(feed_page)
        unchanged_publication = False
        for publication in publications:
            if force or importer.is_changed(session, publication):
                # Queue task to import publication
                apply.bibliographic_apply.delay(
                    publication,
                    collection_id=collection_id,
                )
            else:
                unchanged_publication = True

        if identifier_set is not None:
            identifier_set.add(
                *[
                    ident
                    for pub in publications
                    if (ident := pub.primary_identifier_data) is not None
                ]
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
