from celery import shared_task
from celery.canvas import Signature

from palace.manager.celery import importer
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
from palace.manager.util.http import BadResponseException


@shared_task(queue=QueueNames.default, bind=True)
def import_all(task: Task, force: bool = False) -> None:
    """
    Queue an import task for every OPDS2 collection.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(OPDS2API, registry=registry)
        importer.import_all(
            session.scalars(collection_query).all(),
            import_collection.s(
                force=force,
            ),
            task.log,
        )


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException,),
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
    """
    return identifiers.create_mark_unavailable_chord(
        collection_id,
        import_collection.s(
            collection_id=collection_id, force=force, return_identifiers=True
        ),
    )
