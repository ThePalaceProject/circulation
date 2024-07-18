from celery import shared_task

from palace.manager.api.circulation import PatronActivityCirculationAPI
from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.redis.models.patron_activity import PatronActivity
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.util import get_one


@shared_task(queue=QueueNames.high, bind=True)
def sync_patron_activity(
    task: Task, collection_id: int, patron_id: int, pin: str | None, force: bool = False
) -> None:
    redis_client: Redis = task.services.redis.client()
    patron_activity_status = PatronActivity(
        redis_client, collection_id, patron_id, task.request.id
    )

    if force:
        # If force is True, try to clear the status before continuing with the sync.
        patron_activity_status.clear()

    with patron_activity_status as acquired:
        if not acquired:
            status = patron_activity_status.status()
            state = status.state.name if status is not None else "UNKNOWN"
            task.log.info(
                f"Patron activity sync task could not acquire lock. Task will not "
                f"perform sync. Lock state ({state}) for patron (id: {patron_id}) and "
                f"collection (id: {collection_id})."
            )
            return

        with task.transaction() as session:
            patron = get_one(session, Patron, id=patron_id)
            collection = get_one(session, Collection, id=collection_id)

            if patron is None:
                task.log.error(
                    f"Patron (id: {patron_id}) not found. Marking patron activity as failed."
                )
                patron_activity_status.fail()
                return

            if collection is None:
                task.log.error(
                    f"Collection (id: {collection_id}) not found. Marking patron activity as failed."
                )
                patron_activity_status.fail()
                return

            registry: LicenseProvidersRegistry = (
                task.services.integration_registry.license_providers()
            )
            api = registry.from_collection(session, collection)

            if not isinstance(api, PatronActivityCirculationAPI):
                # Set the status to not supported, and log that we can't sync patron activity.
                patron_activity_status.not_supported()
                task.log.info(
                    f"Collection '{collection.name}' (id: {collection_id}) does not support patron activity sync."
                )
                return

            api.sync_patron_activity(patron, pin)

            task.log.info(
                f"Patron activity sync for patron '{patron.authorization_identifier}' (id: {patron_id}) "
                f"and collection '{collection.name}' (id: {collection_id}) complete."
            )
