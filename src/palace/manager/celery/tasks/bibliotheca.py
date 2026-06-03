"""Celery tasks for Bibliotheca (3M Cloud) collection management.

Four tasks handle near-real-time event import and historical purchase record import:

- ``import_all_collections``: Fans out to one ``import_collection`` task per collection.
- ``import_collection``: Processes one time slice of circulation events, then
  re-queues itself via ``task.replace()`` until the collection is caught up, holding
  a Redis workflow lock across the chain so at most one run proceeds per collection.
- ``import_purchase_records_for_all_collections``: Fans out to one
  ``import_purchase_records_by_collection`` task per collection.
- ``import_purchase_records_by_collection``: Processes one page of MARC purchase
  records, then re-queues itself via ``task.replace()`` until the collection is
  caught up to ``utc_now()``, holding a separate Redis workflow lock so at most one
  purchase record import run proceeds per collection at a time.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from celery import shared_task

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.importer import workflow_lock_guard
from palace.manager.celery.task import Task
from palace.manager.celery.utils import ModelNotFoundError, load_from_id, signature_with
from palace.manager.integration.license.bibliotheca import BibliothecaAPI
from palace.manager.integration.license.bibliotheca_importer import (
    EVENT_IMPORT_OVERLAP,
    BibliothecaEventImporter,
)
from palace.manager.integration.license.bibliotheca_purchase_record_importer import (
    DEFAULT_PURCHASE_RECORD_START_TIME,
    PURCHASE_RECORD_SERVICE_NAME,
    BibliothecaPurchaseRecordImporter,
    DayImportResult,
)
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
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
) -> None:
    """Process one time slice of Bibliotheca circulation events.

    Fetches events from the Bibliotheca API for the window ``[start, slice_end]``,
    updates ``LicensePool`` availability, then re-queues itself for the next
    slice via ``task.replace()`` until the collection is caught up to
    ``now - EVENT_IMPORT_OVERLAP``.

    Concurrency is governed by
    :func:`~palace.manager.celery.importer.workflow_lock_guard`, which holds a
    per-collection Redis lock across both ``task.replace()`` calls and Celery retries.

    :param collection_id: Database ID of the Bibliotheca collection.
    :param start: Start of the slice to process.  ``None`` on the first
        invocation — the start is derived from the stored ``Timestamp``.
    """
    with workflow_lock_guard(
        task, collection_id, label="Bibliotheca event import"
    ) as proceed:
        if not proceed:
            return

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
                signature_with(
                    task,
                    start=result.slice_end,
                )
            )


def _purchase_record_workflow_lock(
    client: Redis, collection_id: int, random_value: str
) -> RedisLock:
    """Create a workflow-level lock for the purchase record importer."""
    return RedisLock(
        client,
        [
            "PurchaseRecordCollectionWorkflow",
            Collection.redis_key_from_id(collection_id),
        ],
        random_value=random_value,
        lock_timeout=timedelta(hours=2),
    )


@shared_task(queue=QueueNames.default, bind=True)
def import_purchase_records_for_all_collections(
    task: Task, *, force_reimport: bool = False
) -> None:
    """Queue an ``import_purchase_records_by_collection`` task for every Bibliotheca collection.

    :param force_reimport: When ``True``, each per-collection task reimports from
        :data:`DEFAULT_PURCHASE_RECORD_START_TIME` (2014-01-01) rather than resuming
        where the last run left off.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            BibliothecaAPI, registry=registry
        )
        collections = session.scalars(collection_query).all()

    current_day = DEFAULT_PURCHASE_RECORD_START_TIME if force_reimport else None
    for collection in collections:
        import_purchase_records_by_collection.delay(
            collection_id=collection.id,
            current_day=current_day,
            reset_timestamp=force_reimport,
        )

    suffix = " (force reimport from start)" if force_reimport else ""
    task.log.info(
        f"Queued {len(collections)} Bibliotheca collection(s) for purchase record import{suffix}."
    )


@shared_task(
    queue=QueueNames.default,
    bind=True,
    max_retries=4,
    autoretry_for=(BadResponseException, RequestTimedOut),
    throws=(RemoteIntegrationException,),
    retry_backoff=60,
)
def import_purchase_records_by_collection(
    task: Task,
    collection_id: int,
    *,
    current_day: datetime | None = None,
    offset: int = 1,
    reset_timestamp: bool = False,
) -> None:
    """Process one page of Bibliotheca MARC purchase records.

    Fetches up to 50 MARC records for ``[current_day, current_day+1day]``
    starting at ``offset``, creates ``LicensePool`` entries, queues
    ``bibliographic_apply`` for new or changed titles, then re-queues itself
    via ``task.replace()``:

    - with ``offset`` advanced when the current page was full (more records
      remain for the same day), or
    - with ``current_day`` advanced to the next day and ``offset`` reset to 1
      when the current day is fully processed.

    This continues until the collection is caught up to ``utc_now()``.

    Concurrency is governed by
    :func:`~palace.manager.celery.importer.workflow_lock_guard` using a dedicated
    :func:`_purchase_record_workflow_lock` (prefix ``PurchaseRecordCollectionWorkflow``),
    which holds a per-collection Redis lock across both ``task.replace()`` calls and
    Celery retries so at most one purchase-record-import chain runs per collection.

    :param collection_id: Database ID of the Bibliotheca collection.
    :param current_day: Start of the day to process.  ``None`` on the first
        invocation — the start is derived from the stored ``Timestamp``
        (defaulting to 2014-01-01 when no timestamp exists).
    :param offset: 1-based record offset within the current day's result set.
        Defaults to ``1`` (the first page).
    :param reset_timestamp: When ``True`` on the **first** invocation, clears
        ``Timestamp.finish`` within the transaction so that ``get_start()``
        returns :data:`DEFAULT_PURCHASE_RECORD_START_TIME` rather than the stale
        finish date.  Has no effect when ``current_day`` is explicitly provided,
        since ``get_start()`` is not called in that case.  Not forwarded to
        replacement tasks.
    """
    with workflow_lock_guard(
        task,
        collection_id,
        label="Bibliotheca purchase record import",
        lock_factory=_purchase_record_workflow_lock,
    ) as proceed:
        if not proceed:
            return

        cutoff = utc_now()
        result: DayImportResult
        collection_name: str | None = None

        with task.transaction() as session:
            try:
                collection = load_from_id(session, Collection, collection_id)
            except ModelNotFoundError:
                task.log.warning(
                    f"Bibliotheca purchase record import: collection {collection_id} not "
                    "found; it may have been deleted. Stopping chain."
                )
                return
            collection_name = collection.name

            if collection.marked_for_deletion:
                task.log.warning(
                    f"Bibliotheca purchase record import: collection '{collection_name}' "
                    "is marked for deletion. Stopping chain."
                )
                return

            importer = BibliothecaPurchaseRecordImporter(session, collection)

            # On a force-reimport's first invocation, clear Timestamp.finish so
            # that a crash before the first record is processed causes the next
            # scheduled run to fall back to DEFAULT_PURCHASE_RECORD_START_TIME
            # rather than the stale pre-reimport finish date. reset_timestamp is
            # only ever True on the first invocation (replacements force it False).
            if reset_timestamp:
                ts = Timestamp.lookup(
                    session,
                    PURCHASE_RECORD_SERVICE_NAME,
                    Timestamp.TASK_TYPE,
                    collection,
                )
                if ts is not None:
                    ts.finish = None

            if current_day is None:
                current_day = importer.get_start()

            if current_day >= cutoff:
                task.log.info(
                    f"Bibliotheca purchase record import: '{collection_name}' is already up to date."
                )
                return

            result = importer.import_day(current_day, cutoff, offset)

        task.log.info(
            f"Bibliotheca purchase record import: fetched {result.records_fetched} record(s) for "
            f"'{collection_name}' "
            f"({result.day_start.strftime('%Y-%m-%dT%H:%M:%S')} -> "
            f"{result.day_end.strftime('%Y-%m-%dT%H:%M:%S')}, offset {offset})."
        )

        if result.next_offset is not None:
            # More pages remain for the current day.
            raise task.replace(
                signature_with(
                    task,
                    # current_day may have been resolved from None inside the task body
                    # (via get_start()), so it must be carried forward explicitly.
                    current_day=current_day,
                    offset=result.next_offset,
                    # reset_timestamp applies only to the first invocation.
                    reset_timestamp=False,
                )
            )

        if result.day_end < cutoff:
            # Current day is complete; advance to the next day.
            raise task.replace(
                signature_with(
                    task,
                    current_day=result.day_end,
                    offset=1,
                    # reset_timestamp applies only to the first invocation.
                    reset_timestamp=False,
                )
            )
