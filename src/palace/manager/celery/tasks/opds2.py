from celery import chain, shared_task
from celery.canvas import Signature

from palace.manager.celery.opds import opds_import_task
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import identifiers
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.opds.importer import FeedImportResult
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.opds2.importer import (
    importer_from_collection,
)
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)
from palace.manager.util.log import pluralize


@shared_task(queue=QueueNames.default, bind=True)
def import_all(task: Task, force: bool = False) -> None:
    """
    Queue an import task for every OPDS2 collection.

    For each collection, checks if reaping should occur based on the
    collection's reap_schedule setting. If reaping is due, queues a
    reap task; otherwise, queues a normal import task.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(OPDS2API, registry=registry)
        collections = session.scalars(collection_query).all()

        for collection in collections:
            if OPDS2API.should_reap(collection):
                # Get settings and last reap time for logging
                settings = OPDS2API.settings_load(collection.integration_configuration)
                last_reap_time_str = collection.integration_configuration.context.get(
                    OPDS2API.LAST_REAP_TIME_KEY
                )

                # Queue reap task. last_reap_time will be updated by update_last_reap_time callback after successful reap.
                reap_task = import_and_reap_not_found_chord(collection.id, force)
                reap_result = reap_task.delay()

                task.log.info(
                    f'Queued collection "{collection.name}" for reap + import. '
                    f"Collection(id={collection.id}) Task(id={reap_result.id}) "
                    f"Schedule: {settings.reap_schedule} "
                    f"Last reaped: {last_reap_time_str or 'never'}"
                )
            else:
                # Queue normal import task
                import_task = import_collection.delay(
                    collection_id=collection.id,
                    force=force,
                )

                task.log.info(
                    f'Queued collection "{collection.name}" for import. '
                    f"Collection(id={collection.id}) Task(id={import_task.id})"
                )

        task.log.info(
            f"Task complete. Queued {pluralize(len(collections), 'collection')} for import."
        )


@shared_task(queue=QueueNames.default, bind=True)
def update_last_reap_time(task: Task, result: bool, *, collection_id: int) -> None:
    """
    Update the last_reap_time context for a collection after a successful reap.

    This task is designed to be chained after a reap chord completes successfully.
    It records the timestamp when the reap operation finished.

    :param result: The result from the previous task (True if successful, False otherwise)
    :param collection_id: The ID of the collection that was reaped
    """
    if not result:
        task.log.warning(
            f"Reap task for collection id={collection_id} did not complete successfully; "
            "last_reap_time not updated."
        )
        return

    with task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        collection.integration_configuration.context_update(
            {OPDS2API.LAST_REAP_TIME_KEY: utc_now().isoformat()}
        )
        session.commit()

        task.log.info(
            f'Updated last_reap_time for collection "{collection.name}" '
            f"(id={collection.id}) after successful reap."
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
    url: str | None = None,
    *,
    force: bool = False,
    return_identifiers: bool = False,
) -> IdentifierSet | None:
    """
    Run an OPDS2 import for the given collection.
    """
    registry = task.services.integration_registry().license_providers()
    with task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        importer = importer_from_collection(collection, registry)

        def update_token_auth_link(
            import_result: FeedImportResult[PublicationFeedNoValidation],
        ) -> None:
            """
            Update the token auth link for the collection if it exists in the import result.
            """
            if token_auth_link := import_result.feed.links.get(
                rel=Hyperlink.TOKEN_AUTH
            ):
                changed = OPDS2API.update_collection_token_auth_url(
                    collection, token_auth_link.href
                )
                if changed:
                    session.commit()

        return opds_import_task(
            task,
            collection,
            importer,
            url,
            post_import_hook=update_token_auth_link,
            force=force,
            return_identifiers=return_identifiers,
        )


def import_and_reap_not_found_chord(
    collection_id: int, force: bool = False
) -> Signature:
    """
    Creates a Celery chord that imports a collection and then reaps identifiers that were not found in the feed.

    After the reap completes successfully, the last_reap_time is updated via the update_last_reap_time callback.
    """
    chord_sig = identifiers.create_mark_unavailable_chord(
        collection_id,
        import_collection.s(
            collection_id=collection_id, force=force, return_identifiers=True
        ),
    )
    # Chain the callback to update last_reap_time after successful reap
    return chain(chord_sig, update_last_reap_time.s(collection_id=collection_id))
