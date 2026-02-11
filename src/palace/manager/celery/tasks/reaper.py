from collections.abc import Callable
from datetime import timedelta
from typing import Any

from celery import shared_task
from sqlalchemy import and_, delete, select, true
from sqlalchemy.orm import Session, defer, lazyload, raiseload, selectinload
from sqlalchemy.sql import Delete
from sqlalchemy.sql.elements import or_

from palace.manager.celery.task import Task
from palace.manager.celery.tasks.collection_delete import collection_delete
from palace.manager.celery.tasks.notifications import (
    NotificationType,
    NotificationTypeT,
    RemovedItemNotificationData,
    send_item_removed_notification,
)
from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.celery.celery import QueueNames
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.integration import IntegrationLibraryConfiguration
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolStatus,
)
from palace.manager.sqlalchemy.model.measurement import Measurement
from palace.manager.sqlalchemy.model.patron import Annotation, Hold, Loan, Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import pluralize


def _execute_delete(session: Session, deletion_query: Delete) -> int:
    # The transactions in the reaper tasks are short lived, so we don't bother to do the
    # extra work of synchronizing the session with the database after the delete. Once
    # the transaction is committed, the session contents won't matter anyway.
    # If we ever need to do something more complex with the session after the delete,
    # we can revisit this.
    # https://docs.sqlalchemy.org/en/20/orm/queryguide/dml.html#selecting-a-synchronization-strategy
    result = session.execute(
        deletion_query, execution_options={"synchronize_session": False}
    )
    # We need the type ignores here because result doesn't always have
    # a rowcount, but the sqlalchemy docs swear it will in the case of
    # a delete statement.
    # https://docs.sqlalchemy.org/en/20/tutorial/data_update.html#getting-affected-row-count-from-update-delete
    return result.rowcount  # type: ignore[attr-defined,no-any-return]


@shared_task(queue=QueueNames.default, bind=True)
def credential_reaper(task: Task) -> None:
    """
    Remove Credentials that expired more than a day ago.
    """
    cutoff = utc_now() - timedelta(days=1)
    deletion_query = delete(Credential).where(Credential.expires < cutoff)
    with task.transaction() as session:
        rows_removed = _execute_delete(session, deletion_query)
    task.log.info(f"Deleted {pluralize(rows_removed, 'expired credential')}.")


@shared_task(queue=QueueNames.default, bind=True)
def patron_reaper(task: Task) -> None:
    """
    Remove patron records that expired more than 60 days ago.
    """
    cutoff = utc_now() - timedelta(days=60)
    deletion_query = delete(Patron).where(Patron.authorization_expires < cutoff)
    with task.transaction() as session:
        rows_removed = _execute_delete(session, deletion_query)
    task.log.info(f"Deleted {pluralize(rows_removed, 'expired patron record')}.")


@shared_task(queue=QueueNames.default, bind=True)
def work_reaper(task: Task, batch_size: int = 1000) -> None:
    """
    Remove Works that have no associated LicensePools.

    As soon as a Work loses its last LicensePool it can be removed.
    """
    work_query = (
        select(Work)
        .outerjoin(LicensePool)
        .where(LicensePool.id == None)
        .order_by(Work.id)
        .limit(batch_size)
        # We defer loading of any fields defined as large on the Work to speed up
        # our query, and since we are loading works without license pools, we want
        # to override the default joined eager loading of license_pools.
        .options(
            *(defer(getattr(Work, f)) for f in Work.LARGE_FIELDS),
            lazyload(Work.license_pools),
        )
    )
    search_index = task.services.search.index()
    with task.transaction() as session:
        works = session.execute(work_query).scalars().all()
        for work in works:
            task.log.info(
                f"Deleting {work!r} because it has no associated LicensePools."
            )
            work.delete(search_index=search_index)

    removed = len(works)
    task.log.info(
        f"Deleted {pluralize(removed, 'Work')} with no associated LicensePools."
    )
    if len(works) == batch_size:
        task.log.info("There may be more Works to delete. Re-queueing the reaper.")
        raise task.replace(work_reaper.s(batch_size=batch_size))


@shared_task(queue=QueueNames.default, bind=True)
def collection_reaper(task: Task) -> None:
    """
    Queue ``collection_delete`` tasks for collections marked for deletion.

    The actual deletion work is handled by :func:`collection_delete`, which
    processes license pools in batches to avoid task timeouts.
    """

    collection_query = (
        select(Collection.id)
        .where(Collection.marked_for_deletion == True)
        .order_by(Collection.id)
    )
    with task.session() as session:
        collection_ids = session.execute(collection_query).scalars().all()

    for cid in collection_ids:
        task.log.info(f"Queueing deletion of collection {cid}.")
        collection_delete.delay(cid)


@shared_task(queue=QueueNames.default, bind=True)
def measurement_reaper(task: Task) -> None:
    """
    Remove measurements that are not the most recent
    """
    deletion_query = delete(Measurement).where(Measurement.is_most_recent == False)
    with task.transaction() as session:
        rows_removed = _execute_delete(session, deletion_query)
    task.log.info(f"Deleted {pluralize(rows_removed, 'outdated measurement')}.")


@shared_task(queue=QueueNames.default, bind=True)
def annotation_reaper(task: Task) -> None:
    """
    The annotation must have motivation=IDLING, must be at least 60
    days old (meaning there has been no attempt to read the book
    for 60 days), and must not be associated with one of the
    patron's active loans or holds.
    """
    cutoff = utc_now() - timedelta(days=60)

    restrictions = []
    for t in Loan, Hold:
        active_subquery = (
            select(Annotation.id)
            .join(t, t.patron_id == Annotation.patron_id)
            .join(
                LicensePool,
                and_(
                    LicensePool.id == t.license_pool_id,
                    LicensePool.identifier_id == Annotation.identifier_id,
                ),
            )
        )
        restrictions.append(~Annotation.id.in_(active_subquery))

    deletion_query = delete(Annotation).where(
        Annotation.timestamp < cutoff,
        Annotation.motivation == Annotation.IDLING,
        *restrictions,
    )

    with task.transaction() as session:
        rows_removed = _execute_delete(session, deletion_query)
    task.log.info(f"Deleted {pluralize(rows_removed, 'outdated idling annotation')}.")


@shared_task(queue=QueueNames.default, bind=True)
def hold_reaper(task: Task, batch_size: int = 100) -> None:
    """
    Remove seemingly abandoned holds from the database.
    """
    cutoff = utc_now() - timedelta(days=365)
    analytics_service = task.services.analytics.analytics()
    query = (
        select(Hold)
        .where(Hold.start < cutoff, or_(Hold.end == None, Hold.end < utc_now()))
        .order_by(Hold.id)
        .limit(batch_size)
    )
    events_to_be_logged = []
    with task.transaction() as session:
        holds = session.execute(query).scalars().all()
        for hold in holds:
            events_to_be_logged.append(
                AnalyticsEventData.create(
                    library=hold.library,
                    license_pool=hold.license_pool,
                    event_type=CirculationEvent.CM_HOLD_EXPIRED,
                    patron=hold.patron,
                )
            )
            session.delete(hold)

    count = len(holds)
    task.log.info(f"Deleted {pluralize(count, 'expired hold')}.")

    with task.transaction() as session:
        for event in events_to_be_logged:
            analytics_service.collect(event=event, session=session)

    if count == batch_size:
        task.log.info("There may be more holds to delete. Re-queueing the reaper.")
        raise task.replace(hold_reaper.s(batch_size=batch_size))


@shared_task(queue=QueueNames.default, bind=True)
def loan_reaper(task: Task) -> None:
    """
    Remove expired and abandoned loans from the database.
    """
    now = utc_now()
    deletion_query = delete(Loan).where(
        Loan.license_pool_id == LicensePool.id,
        LicensePool.metered_or_equivalent_type == true(),
        or_(
            Loan.end < now,
            and_(Loan.start < now - timedelta(days=90), Loan.end == None),
        ),
    )

    with task.transaction() as session:
        rows_removed = _execute_delete(session, deletion_query)

    task.log.info(f"Deleted {pluralize(rows_removed, 'expired loan')}.")


def _removed_license_pool_reaper_with_notifications[ItemT: type[Loan | Hold]](
    task: Task,
    item_cls: ItemT,
    notification_task: Callable[[RemovedItemNotificationData, NotificationTypeT], Any],
    notification_type: NotificationTypeT,
    batch_size: int,
) -> int:
    """
    Remove loans or holds from REMOVED license pools and queue notification tasks.

    :param task: The Celery task instance
    :param item_cls: Either Loan or Hold class
    :param notification_task: Celery task to queue for sending notifications
    :param notification_type: Type of notification to send (hold removed or loan removed)
    :param batch_size: Number of items to process in one batch
    :return: Number of items deleted in this batch
    """
    if batch_size <= 0:
        return 0

    # Build query for items with REMOVED license pools
    # Eagerly load all relationships needed by RemovedItemNotificationData.from_item()
    query = (
        select(item_cls)
        .join(LicensePool, item_cls.license_pool_id == LicensePool.id)
        .where(LicensePool.status == LicensePoolStatus.REMOVED)
        .options(
            selectinload(item_cls.patron).selectinload(Patron.library),
            selectinload(item_cls.license_pool).selectinload(LicensePool.identifier),
            selectinload(item_cls.license_pool)
            .selectinload(LicensePool.work)
            .selectinload(Work.presentation_edition),
            selectinload(item_cls.license_pool)
            .selectinload(LicensePool.presentation_edition)
            .selectinload(Edition.work),
            # Prevent any other lazy loading - fail fast if we missed something
            raiseload("*"),
        )
        .order_by(item_cls.id)
        .limit(batch_size)
    )

    # Collect notification data and delete items
    notification_data = []
    with task.transaction() as session:
        items = session.execute(query).scalars().all()
        for item in items:
            # Extract notification data before deletion
            data = RemovedItemNotificationData.from_item(item)
            if data:
                notification_data.append(data)

            # Delete the item
            session.delete(item)

    count = len(items)
    task.log.info(
        f"Deleted {pluralize(count, item_cls.__name__.lower())} on "
        f"license pools that have been removed."
    )

    # Queue notification tasks AFTER transaction commits
    for data in notification_data:
        notification_task(data, notification_type)

    if notification_data:
        task.log.info(
            f"Queued {len(notification_data)} {item_cls.__name__.lower()} "
            f"removed notifications"
        )

    return count


@shared_task(queue=QueueNames.default, bind=True)
def removed_license_pool_hold_loan_reaper(task: Task, batch_size: int = 100) -> None:
    """
    Remove loans and holds from license pools that have been marked as removed.

    Queues push notification tasks to inform patrons that their loans/holds
    have been removed and are no longer available. Notifications are sent
    asynchronously by separate worker tasks.

    :param task: The Celery task instance
    :param batch_size: Number of items to process per batch (default 100)
    """
    # Process holds first
    items_deleted = _removed_license_pool_reaper_with_notifications(
        task,
        Hold,
        send_item_removed_notification.delay,
        NotificationType.HOLD_REMOVED,
        batch_size,
    )

    # Process loans
    remaining_batch_size = max(0, batch_size - items_deleted)
    items_deleted += _removed_license_pool_reaper_with_notifications(
        task,
        Loan,
        send_item_removed_notification.delay,
        NotificationType.LOAN_REMOVED,
        remaining_batch_size,
    )

    # Re-queue if we hit the batch limit (more items may exist)
    if items_deleted == batch_size:
        task.log.info(
            "Batch size reached. There may be more items to delete. Re-queueing the reaper."
        )
        raise task.replace(
            removed_license_pool_hold_loan_reaper.s(batch_size=batch_size)
        )


@shared_task(queue=QueueNames.default, bind=True)
def reap_unassociated_loans(task: Task) -> None:
    reap_unassociated_loans_or_holds(task, Loan)


@shared_task(queue=QueueNames.default, bind=True)
def reap_unassociated_holds(task: Task) -> None:
    reap_unassociated_loans_or_holds(task, Hold)


@shared_task(queue=QueueNames.default, bind=True)
def reap_loans_in_inactive_collections(task: Task) -> None:
    reap_loans_or_holds_in_inactive_collections(task, Loan)


@shared_task(queue=QueueNames.default, bind=True)
def reap_holds_in_inactive_collections(task: Task) -> None:
    reap_loans_or_holds_in_inactive_collections(task, Hold)


def reap_unassociated_loans_or_holds(
    task: Task, deletion_class: type[Loan | Hold]
) -> None:
    """
    Delete loans or holds that are no longer available to the patron
    because the patron's library is no longer associated with the collection
    containing the loan or hold's license pool.
    """
    ids_to_delete = (
        select(deletion_class.id)
        .join(LicensePool, LicensePool.id == deletion_class.license_pool_id)
        .join(Patron, Patron.id == deletion_class.patron_id)
        .join(Collection, Collection.id == LicensePool.collection_id)
        .outerjoin(
            IntegrationLibraryConfiguration,
            and_(
                Collection.integration_configuration_id
                == IntegrationLibraryConfiguration.parent_id,
                IntegrationLibraryConfiguration.library_id == Patron.library_id,
            ),
        )
        .where(IntegrationLibraryConfiguration.parent_id == None)
    )
    # Delete all loans or holds matching the ids to delete query
    deletion_query = delete(deletion_class).where(deletion_class.id.in_(ids_to_delete))

    with task.transaction() as tx:
        deletion_count = _execute_delete(tx, deletion_query)
    task.log.info(
        f"deleted {deletion_count} {pluralize(deletion_count, deletion_class.__name__.lower())} "
        f"because the patron's library was no longer associated with the collection."
    )


def reap_loans_or_holds_in_inactive_collections(
    task: Task, deletion_class: type[Loan | Hold]
) -> None:
    """
    Delete the loans or holds associated with inactive collections
    """
    active_colls = Collection.active_collections_filter(
        sa_select=select(Collection.id)
    ).scalar_subquery()
    ids_to_delete = (
        select(deletion_class.id)
        .join(LicensePool)
        .where(LicensePool.collection_id.not_in(active_colls))
        .scalar_subquery()
    )
    deletion_query = delete(deletion_class).where(deletion_class.id.in_(ids_to_delete))

    with task.transaction() as tx:
        deletion_count = _execute_delete(tx, deletion_query)
    task.log.info(
        f"deleted {pluralize(deletion_count, deletion_class.__name__.lower())} "
        f"because the associated collection is inactive."
    )
