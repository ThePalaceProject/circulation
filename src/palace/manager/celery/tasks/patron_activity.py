from celery import shared_task
from sqlalchemy.orm.exc import StaleDataError

from palace.manager.api.circulation.base import PatronActivityCirculationAPI
from palace.manager.api.circulation.exceptions import PatronAuthorizationFailedException
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
from palace.manager.util.backoff import exponential_backoff
from palace.manager.util.http.exception import RemoteIntegrationException


@shared_task(queue=QueueNames.high, bind=True, max_retries=4)
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

            try:
                api.sync_patron_activity(patron, pin)
            except PatronAuthorizationFailedException:
                patron_activity_status.fail()
                task.log.exception(
                    "Patron activity sync task failed due to PatronAuthorizationFailedException. "
                    "Marking patron activity as failed."
                )
                return
            except (RemoteIntegrationException, StaleDataError) as e:
                # This may have been a transient network error with the remote integration or some data
                # changed while we were processing the sync. Attempt to retry.
                retries = task.request.retries
                if retries < task.max_retries:
                    wait_time = exponential_backoff(retries)
                    patron_activity_status.clear()
                    task.log.exception(
                        f"Patron activity sync task failed ({e}). Retrying in {wait_time} seconds."
                    )
                    raise task.retry(countdown=wait_time)

                # We've reached the max number of retries. Mark the status as failed, but don't fail
                # the task itself, since this is likely an error that is outside our control.
                patron_activity_status.fail()
                task.log.exception(
                    f"Patron activity sync task failed ({e}). Max retries exceeded."
                )
                return
            except Exception:
                # In the case of an unknown exception, we log some helpful details for troubleshooting and
                # re-raise the exception to ensure the task is marked as failed.
                task.log.exception(
                    f"An exception occurred during the sync_patron_activity task. "
                    f"Collection '{collection.name}' (id: '{collection_id}', protocol: '{collection.protocol}')."
                    f" Patron '{patron.authorization_identifier}' (id: {patron_id})."
                )
                raise

            task.log.info(
                f"Patron activity sync for patron '{patron.authorization_identifier}' (id: {patron_id}) "
                f"and collection '{collection.name}' (id: {collection_id}) complete."
            )
