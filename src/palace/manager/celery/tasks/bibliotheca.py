"""Celery tasks for Bibliotheca (3M Cloud) collection management.

Three tasks handle different aspects of keeping a Bibliotheca collection in sync:

- ``import_all_collections`` / ``import_collection``:  Near-real-time circulation
  events, processed in 5-minute slices.
- ``purchase_monitor_all_collections`` / ``purchase_monitor_collection`` (PR 2):
  MARC-record-based purchase history, one day at a time.
- ``circulation_sweep_all_collections`` / ``circulation_sweep_collection`` (PR 3):
  Full identifier sweep for ground-truth availability reconciliation.

Each per-collection task processes one page of data then re-queues itself via
``task.replace()``, holding a Redis workflow lock across the chain so at most one
workflow runs per collection at a time.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import uuid4

from celery import shared_task
from celery.exceptions import Ignore
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import utc_now
from palace.util.log import LoggerType

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.utils import load_from_id
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)

EVENT_IMPORT_SERVICE_NAME = "Bibliotheca Event Monitor"

_LOG_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Amount of time to overlap between consecutive event-import runs to reduce
# the risk of missing events at the boundary.
_EVENT_IMPORT_OVERLAP = timedelta(minutes=5)

# Maximum time window processed in a single task invocation.
_EVENT_IMPORT_SLICE_SIZE = timedelta(minutes=5)


def _event_import_workflow_lock(
    client: Redis, collection_id: int, random_value: str
) -> RedisLock:
    """Workflow-level lock spanning all slices of a single event-import run.

    Held across ``task.replace()`` calls so at most one run proceeds per
    collection.  Auto-expires after 2 hours if the worker dies mid-chain.
    """
    return RedisLock(
        client,
        [
            "BibliothecaEventImportWorkflow",
            Collection.redis_key_from_id(collection_id),
        ],
        random_value=random_value,
        lock_timeout=timedelta(hours=2),
    )


def _handle_event(
    session: Session,
    api: BibliothecaAPI,
    collection: Collection,
    bibliotheca_id: str,
    isbn: str,
    foreign_patron_id: str | None,
    start_time: datetime,
    end_time: datetime | None,
    internal_event_type: str,
    logger: LoggerType,
) -> None:
    """Process a single Bibliotheca circulation event.

    Ported from ``BibliothecaEventMonitor.handle_event``.  Creates or updates the
    ``LicensePool``, links the ISBN identifier, adjusts availability based on the
    event delta, and queues a ``bibliographic_apply`` task when the title's metadata
    has changed (hash-based deduplication).
    """
    license_pool, _ = LicensePool.for_foreign_id(
        session,
        api.data_source,
        Identifier.BIBLIOTHECA_ID,
        bibliotheca_id,
        collection=collection,
    )

    # Fetch bibliographic metadata and queue an apply task only if the
    # content hash differs from what is already stored.
    for bibliographic in api.bibliographic_lookup(bibliotheca_id):
        if bibliographic.needs_apply(session):
            apply.bibliographic_apply.delay(
                bibliographic,
                collection_id=collection.id,
                replace=ReplacementPolicy.from_license_source(),
            )

    bibliotheca_identifier = license_pool.identifier
    isbn_identifier, _ = Identifier.for_foreign_id(session, Identifier.ISBN, isbn)

    Edition.for_foreign_id(
        session, api.data_source, Identifier.BIBLIOTHECA_ID, bibliotheca_id
    )

    bibliotheca_identifier.equivalent_to(api.data_source, isbn_identifier, strength=1)

    license_pool.update_availability_from_delta(internal_event_type, start_time, 1)

    logger.info(
        "%s: %s",
        start_time.strftime(_LOG_DATE_FORMAT),
        internal_event_type,
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
    """Process one 5-minute slice of Bibliotheca circulation events.

    Fetches events from the Bibliotheca API for the window ``[start, slice_end]``,
    updates ``LicensePool`` availability, then re-queues itself for the next
    slice via ``task.replace()`` until the collection is caught up to
    ``now - OVERLAP``.

    A Redis workflow lock keyed to ``collection_id`` ensures at most one chain
    runs per collection at a time.

    .. note::

        ``BadResponseException`` and ``RequestTimedOut`` are listed in
        ``autoretry_for``, but the workflow lock is *not* released on those
        exceptions (they appear in ``ignored_exceptions``).  Each retry fires
        as a fresh first-slice invocation (``lock_value=None``), generates a
        new UUID, fails to acquire the still-held lock, and silently skips.
        In practice the 2-hour lock expiry is the only recovery path after a
        persistent API failure.

    :param collection_id: Database ID of the Bibliotheca collection.
    :param start: Start of the slice to process.  ``None`` on the first
        invocation — the start is derived from the stored ``Timestamp``.
    :param lock_value: UUID identifying this workflow.  Generated on the first
        invocation (``start is None``) and forwarded unchanged to every
        subsequent slice so the lock is held across ``task.replace()`` calls.
    """
    redis = task.services.redis().client()

    is_first_slice = lock_value is None
    if lock_value is None:
        lock_value = str(uuid4())

    workflow_lock = _event_import_workflow_lock(redis, collection_id, lock_value)

    with workflow_lock.lock(
        raise_when_not_acquired=False,
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

        cutoff = utc_now() - _EVENT_IMPORT_OVERLAP
        slice_end: datetime | None = None
        events_handled = 0
        collection_name: str | None = None

        with task.transaction() as session:
            collection = load_from_id(session, Collection, collection_id)
            collection_name = collection.name

            if start is None:
                timestamp = Timestamp.lookup(
                    session,
                    EVENT_IMPORT_SERVICE_NAME,
                    Timestamp.MONITOR_TYPE,
                    collection,
                )
                if timestamp is None or timestamp.finish is None:
                    start = cutoff - _EVENT_IMPORT_OVERLAP
                else:
                    start = timestamp.finish - _EVENT_IMPORT_OVERLAP

            if start >= cutoff:
                task.log.info(
                    f"Bibliotheca event import: '{collection_name}' is already up to date."
                )
                return

            slice_end = min(start + _EVENT_IMPORT_SLICE_SIZE, cutoff)

            api = BibliothecaAPI(session, collection)

            task.log.info(
                f"Bibliotheca event import: requesting events for '{collection_name}' "
                f"between {start.strftime(_LOG_DATE_FORMAT)} and "
                f"{slice_end.strftime(_LOG_DATE_FORMAT)}."
            )

            for event_tuple in api.get_events_between(start, slice_end):
                _handle_event(session, api, collection, *event_tuple, logger=task.log)
                events_handled += 1

            Timestamp.stamp(
                session,
                service=EVENT_IMPORT_SERVICE_NAME,
                service_type=Timestamp.MONITOR_TYPE,
                collection=collection,
                start=start,
                finish=slice_end,
                achievements=f"Events handled: {events_handled}.",
            )

        # Control flow guarantees slice_end was assigned inside the transaction.
        assert slice_end is not None

        task.log.info(
            f"Bibliotheca event import: handled {events_handled} event(s) for "
            f"'{collection_name}' ({start.strftime(_LOG_DATE_FORMAT)} -> "
            f"{slice_end.strftime(_LOG_DATE_FORMAT)})."
        )

        if slice_end < cutoff:
            raise task.replace(
                task.s(
                    collection_id=collection_id,
                    start=slice_end,
                    lock_value=lock_value,
                )
            )
