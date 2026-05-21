"""Celery tasks for Bibliotheca (3M Cloud) collection management.

Two tasks handle near-real-time event import for Bibliotheca collections:

- ``import_all_collections``: Fans out to one ``import_collection`` task per collection.
- ``import_collection``: Processes one time slice of circulation events, then
  re-queues itself via ``task.replace()`` until the collection is caught up, holding
  a Redis workflow lock across the chain so at most one run proceeds per collection.
"""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

from celery import shared_task
from celery.exceptions import Ignore

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.importer import import_workflow_lock
from palace.manager.celery.task import Task
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.bibliotheca_importer import (
    EVENT_IMPORT_OVERLAP,
    BibliothecaEventImporter,
)
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)


@shared_task(queue=QueueNames.default, bind=True)
def import_all_collections(task: Task) -> None:
    """Queue an ``import_collection`` task for every Bibliotheca collection."""
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            BibliothecaAPI, registry=registry
        )
        collections = session.scalars(collection_query).all()

    for collection in collections:
        import_collection.delay(collection_id=collection.id)

    task.log.info(
        f"Queued {len(collections)} Bibliotheca collection(s) for event import."
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
    start: datetime | None = None,
    lock_value: str | None = None,
) -> None:
    """Process one time slice of Bibliotheca circulation events.

    Fetches events from the Bibliotheca API for the window ``[start, slice_end]``,
    updates ``LicensePool`` availability, then re-queues itself for the next
    slice via ``task.replace()`` until the collection is caught up to
    ``now - EVENT_IMPORT_OVERLAP``.

    A Redis workflow lock keyed to ``collection_id`` ensures at most one chain
    runs per collection at a time.  The lock is held across both ``task.replace()``
    calls and Celery retries (``BadResponseException``, ``RequestTimedOut``) so that
    a transient API failure does not open a window for a second concurrent run.

    :param collection_id: Database ID of the Bibliotheca collection.
    :param start: Start of the slice to process.  ``None`` on the first
        invocation — the start is derived from the stored ``Timestamp``.
    :param lock_value: UUID identifying this workflow.  Generated on the first
        invocation and forwarded unchanged to every subsequent slice so the
        lock is held across ``task.replace()`` calls.
    """
    redis = task.services.redis().client()

    is_first_slice = lock_value is None
    if lock_value is None:
        lock_value = str(uuid4())

    workflow_lock = import_workflow_lock(redis, collection_id, lock_value)

    with workflow_lock.lock(
        raise_when_not_acquired=False,
        # Hold the lock across task.replace() (Ignore), and across Celery retries
        # (BadResponseException, RequestTimedOut) so a transient API failure doesn't
        # open a window for a concurrent run to start on the same collection.
        ignored_exceptions=(Ignore, BadResponseException, RequestTimedOut),
    ) as workflow_lock_acquired:
        if not workflow_lock_acquired and is_first_slice:
            task.log.warning(
                f"Bibliotheca event import skipped for collection {collection_id}: "
                "another run is already in progress."
            )
            return
        if not workflow_lock_acquired and not is_first_slice:
            task.log.warning(
                f"Bibliotheca event import for collection {collection_id}: "
                "workflow lock expired between slices; continuing "
                "(another run may be active)."
            )

        cutoff = utc_now() - EVENT_IMPORT_OVERLAP
        result = None
        collection_name: str | None = None

        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name
            importer = BibliothecaEventImporter(session, collection)

            if start is None:
                start = importer.get_start(cutoff)

            if start >= cutoff:
                task.log.info(
                    f"Bibliotheca event import: '{collection_name}' is already up to date."
                )
                return

            result = importer.import_time_slice(start, cutoff)

        assert result is not None

        task.log.info(
            f"Bibliotheca event import: handled {result.events_handled} event(s) for "
            f"'{collection_name}' "
            f"({result.slice_start.strftime('%Y-%m-%dT%H:%M:%S')} -> "
            f"{result.slice_end.strftime('%Y-%m-%dT%H:%M:%S')})."
        )

        if result.slice_end < cutoff:
            raise task.replace(
                task.s(
                    collection_id=collection_id,
                    start=result.slice_end,
                    lock_value=lock_value,
                )
            )
