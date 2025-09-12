from celery import shared_task
from celery.canvas import Signature

from palace.manager.celery.importer import (
    import_all as create_import_tasks,
    import_key,
    import_lock,
)
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply, identifiers
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.boundless.api import BoundlessApi
from palace.manager.integration.license.boundless.importer import BoundlessImporter
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.collection import Collection
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
    return_identifiers: bool = False,
) -> IdentifierSet | None:
    """
    Run an import for a single Boundless collection.
    """
    redis = task.services.redis().client()
    registry = task.services.integration_registry().license_providers()

    with (
        import_lock(redis, collection_id).lock(),
        task.transaction() as session,
    ):
        collection = load_from_id(session, Collection, collection_id)

        identifier_set = (
            IdentifierSet(redis, import_key(collection_id, task.request.id))
            if return_identifiers
            else None
        )

        return BoundlessImporter(
            session, collection, registry, import_all, identifier_set
        ).import_collection(
            apply_bibliographic=apply.bibliographic_apply.delay,
        )


@shared_task(queue=QueueNames.default, bind=True)
def reap_all_collections(task: Task, *, import_all: bool = False) -> None:
    """
    Queue a reap task for every Boundless collection.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            BoundlessApi, registry=registry
        )
        for collection in session.scalars(collection_query):
            import_and_reap_not_found_chord(
                collection_id=collection.id, import_all=import_all
            ).delay()
            task.log.info(
                f'Queued collection("{collection.name}" [id={collection.id}] for reaping...'
            )

    task.log.info(f"Finished queuing all collection reaping tasks.")


def import_and_reap_not_found_chord(
    collection_id: int, import_all: bool = False
) -> Signature:
    """
    Creates a Celery chord that imports a collection and then reaps
    identifiers that were not found in the feed.
    """
    return identifiers.create_mark_unavailable_chord(
        collection_id,
        import_collection.s(
            collection_id=collection_id, import_all=import_all, return_identifiers=True
        ),
    )
