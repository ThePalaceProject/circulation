import datetime

from celery import shared_task
from sqlalchemy import delete, select
from sqlalchemy.exc import NoResultFound
from sqlalchemy.orm import Session

from palace.manager.celery import importer
from palace.manager.celery.importer import import_lock
from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.celery.utils import load_from_id
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.importer import (
    importer_from_collection,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import RedisLock
from palace.manager.service.redis.redis import Redis
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import License, LicensePool
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import (
    BadResponseException,
    RemoteIntegrationException,
    RequestTimedOut,
)


def _remove_expired_holds_for_collection(
    db: Session,
    collection_id: int,
) -> list[AnalyticsEventData]:
    """
    Remove expired holds from the database for this collection.
    """

    select_query = select(Hold).where(
        Hold.position == 0,
        Hold.end < utc_now(),
        Hold.license_pool_id == LicensePool.id,
        LicensePool.collection_id == collection_id,
    )

    expired_holds = db.scalars(select_query).all()
    expired_hold_events = []
    for hold in expired_holds:
        expired_hold_events.append(
            AnalyticsEventData.create(
                library=hold.library,
                license_pool=hold.license_pool,
                event_type=CirculationEvent.CM_HOLD_EXPIRED,
                patron=hold.patron,
            )
        )

    # delete the holds
    query = (
        delete(Hold)
        .where(Hold.id.in_(h.id for h in expired_holds))
        .execution_options(synchronize_session="fetch")
    )

    db.execute(query)

    return expired_hold_events


def _licensepool_ids_with_holds(
    db: Session, collection_id: int, batch_size: int, after_id: int | None
) -> list[int]:
    query = (
        select(LicensePool.id)
        .join(Hold)
        .where(LicensePool.collection_id == collection_id)
        .order_by(LicensePool.id)
        .limit(batch_size)
        .distinct()
    )

    if after_id:
        query = query.where(LicensePool.id > after_id)

    return db.scalars(query).all()


def _lock_licenses(license_pool: LicensePool) -> None:
    """
    Acquire a row level lock on all the licenses for a license pool.

    WARNING: This function should be called within a transaction that
    will be relatively short-lived. Since this will cause all the
    licenses for the license pool to be locked, it could cause
    contention or deadlocks if it is held for a long time.
    """
    session = Session.object_session(license_pool)
    session.execute(
        select(License.id).where(License.license_pool == license_pool).with_for_update()
    ).all()


def _recalculate_holds_for_licensepool(
    license_pool: LicensePool,
    reservation_period: datetime.timedelta,
) -> tuple[int, list[AnalyticsEventData]]:
    # We take out row level locks on all the licenses and holds for this license pool, so that
    # everything is in a consistent state while we update the hold queue. This means we should be
    # quickly committing the transaction, to avoid contention or deadlocks.
    _lock_licenses(license_pool)
    holds = license_pool.get_active_holds(for_update=True)

    license_pool.update_availability_from_licenses()
    reserved = license_pool.licenses_reserved

    ready = holds[:reserved]
    waiting = holds[reserved:]
    updated = 0

    events = []

    # These holds have a copy reserved for them.
    for hold in ready:
        # If this hold isn't already in position 0, the hold just became available.
        # We need to set it to position 0 and set its end date.
        if hold.position != 0 or hold.end is None:
            hold.position = 0
            hold.end = utc_now() + reservation_period
            updated += 1
            events.append(
                AnalyticsEventData.create(
                    library=hold.library,
                    license_pool=hold.license_pool,
                    event_type=CirculationEvent.CM_HOLD_READY_FOR_CHECKOUT,
                    patron=hold.patron,
                )
            )

    # Update the position for the remaining holds.
    for idx, hold in enumerate(waiting):
        position = idx + 1
        if hold.position != position:
            hold.position = position
            hold.end = None
            updated += 1

    return updated, events


@shared_task(queue=QueueNames.default, bind=True)
def remove_expired_holds_for_collection_task(task: Task, collection_id: int) -> None:
    """
    A shared task for removing expired holds from the database for a collection
    """
    analytics = task.services.analytics.analytics()

    with task.transaction() as session:
        collection = Collection.by_id(session, collection_id)
        events = _remove_expired_holds_for_collection(
            session,
            collection_id,
        )

        collection_name = None if not collection else collection.name
        task.log.info(
            f"Removed {len(events)} expired holds for collection {collection_name} ({collection_id})."
        )

    with task.transaction() as session:
        _collect_events(session, events, analytics)


@shared_task(queue=QueueNames.default, bind=True)
def remove_expired_holds(task: Task) -> None:
    """
    Issue remove expired hold tasks for eligible collections
    """
    registry = task.services.integration_registry.license_providers()
    with task.session() as session:
        collections = [
            (collection.id, collection.name)
            for collection in session.execute(
                Collection.select_by_protocol(OPDS2WithODLApi, registry=registry)
            ).scalars()
        ]
    for collection_id, collection_name in collections:
        remove_expired_holds_for_collection_task.delay(collection_id)


@shared_task(queue=QueueNames.default, bind=True)
def recalculate_hold_queue(task: Task) -> None:
    """
    Queue a task for each OPDS2WithODLApi integration to recalculate the hold queue.
    """
    registry = task.services.integration_registry.license_providers()
    with task.session() as session:
        for collection in session.execute(
            Collection.select_by_protocol(OPDS2WithODLApi, registry=registry)
        ).scalars():
            recalculate_hold_queue_collection.delay(collection.id)


def _redis_lock_recalculate_holds(client: Redis, collection_id: int) -> RedisLock:
    return RedisLock(
        client,
        lock_name=[
            "RecalculateHolds",
            Collection.redis_key_from_id(collection_id),
        ],
    )


def _collect_events(
    session: Session, events: list[AnalyticsEventData], analytics: Analytics
) -> None:
    """
    Collect events after successful database is commit and any row locks are removed.
    We perform this operation outside after completed the transaction to ensure that any row locks
    are held for the shortest possible duration in case writing to the s3 analytics provider is slow.
    """
    for event in events:
        analytics.collect(event, session)


@shared_task(queue=QueueNames.default, bind=True)
def recalculate_hold_queue_collection(
    task: Task, collection_id: int, batch_size: int = 100, after_id: int | None = None
) -> None:
    """
    Recalculate the hold queue for a collection.
    """
    analytics = task.services.analytics.analytics()
    redis_client = task.services.redis.client()
    with _redis_lock_recalculate_holds(redis_client, collection_id).lock():
        with task.transaction() as session:
            collection = Collection.by_id(session, collection_id)
            if collection is None:
                task.log.info(
                    f"Skipping collection {collection_id} because it no longer exists."
                )
                return

            collection_name = collection.name
            reservation_period = datetime.timedelta(
                days=collection.default_reservation_period
            )
            task.log.info(
                f"Recalculating hold queue for collection {collection_name} ({collection_id})."
            )

            license_pool_ids = _licensepool_ids_with_holds(
                session, collection_id, batch_size, after_id
            )

        for license_pool_id in license_pool_ids:
            with task.transaction() as session:
                try:
                    license_pool = (
                        session.scalars(
                            select(LicensePool).where(LicensePool.id == license_pool_id)
                        )
                        .unique()
                        .one()
                    )
                except NoResultFound:
                    task.log.info(
                        f"Skipping license pool {license_pool_id} because it no longer exists."
                    )
                    continue

                updated, events = _recalculate_holds_for_licensepool(
                    license_pool,
                    reservation_period,
                )
                edition = license_pool.presentation_edition
                title = edition.title if edition else None
                author = edition.author if edition else None
                task.log.debug(
                    f"Updated hold queue for license pool {license_pool_id} ({title} by {author}). "
                    f"{updated} holds out of date."
                )

            with task.transaction() as session:
                _collect_events(session, events, analytics)

    if len(license_pool_ids) == batch_size:
        # We are done this batch, but there is probably more work to do, we queue up the next batch.
        raise task.replace(
            recalculate_hold_queue_collection.s(
                collection_id, batch_size, license_pool_ids[-1]
            )
        )

    task.log.info(
        f"Finished recalculating hold queue for collection {collection_name} ({collection_id})."
    )


@shared_task(queue=QueueNames.default, bind=True)
def import_all(task: Task, force: bool = False) -> None:
    """
    Run import task for all OPDS2WithODLApi collections.
    """
    with task.session() as session:
        registry = task.services.integration_registry().license_providers()
        collection_query = Collection.select_by_protocol(
            OPDS2WithODLApi, registry=registry
        )
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
    max_retries=5,
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
) -> None:
    """
    Run an OPDS2+ODL import for the given collection.
    """
    redis = task.services.redis().client()
    with import_lock(redis, collection_id).lock(), task.session() as session:
        collection = load_from_id(session, Collection, collection_id)
        registry = task.services.integration_registry().license_providers()

        import_result = importer_from_collection(collection, registry).import_feed(
            collection,
            url,
            apply_bibliographic=apply.bibliographic_apply.delay,
            apply_circulation=apply.circulation_apply.delay,
            import_even_if_unchanged=force,
        )

    if not import_result:
        task.log.info(
            f"Import failed, aborting task for collection '{collection.name}' (id={collection_id})."
        )
        return

    next_link = import_result.next_url
    if next_link is not None:
        # This page is complete, but there are more pages to import, so we requeue ourselves with the
        # next page URL.
        raise task.replace(
            task.s(
                collection_id=collection_id,
                url=next_link,
                force=force,
            )
        )

    task.log.info(
        f"Import complete for collection '{collection.name}' (id={collection_id})."
    )
