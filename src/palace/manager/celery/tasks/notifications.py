import datetime
import logging
import math
from enum import StrEnum
from operator import and_
from typing import Literal, Self

from celery import shared_task
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.celery.utils import load_from_id
from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.base.frozen import BaseFrozenData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.fcm.fcm import SendNotificationsCallable
from palace.manager.service.redis.models.lock import TaskLock
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin

log = logging.getLogger(__name__)


class RemovedItemNotificationData(BaseFrozenData, LoggerMixin):
    """Data needed to send a notification about a removed loan or hold."""

    patron_id: int
    work_title: str
    library_name: str
    library_short_name: str
    identifier: IdentifierData

    @classmethod
    def from_item(cls, item: Loan | Hold) -> Self | None:
        """
        Extract notification data from a Loan or Hold object before deletion.

        :param item: The loan or hold object to extract data from
        :return: RemovedItemNotificationData if all required data is present, None otherwise
        """
        item_type = item.__class__.__name__.lower()

        work = item.work
        if work is None or work.title is None:
            cls.logger().error(
                f"Failed to extract {item_type} notification data because work/title is missing for "
                f"{item_type} '{item.id}', patron '{item.patron.authorization_identifier}'"
            )
            return None

        library_name = item.library.name
        if library_name is None:
            cls.logger().error(
                f"Failed to extract {item_type} notification data because library name is missing for "
                f"{item_type} '{item.id}', patron '{item.patron.authorization_identifier}'"
            )
            return None

        return cls(
            patron_id=item.patron.id,
            work_title=work.title,
            library_name=library_name,
            library_short_name=item.library.short_name,
            identifier=IdentifierData.from_identifier(item.license_pool.identifier),
        )


class NotificationType(StrEnum):
    LOAN_EXPIRY = "LoanExpiry"
    HOLD_AVAILABLE = "HoldAvailable"
    LOAN_REMOVED = "LoanRemoved"
    HOLD_REMOVED = "HoldRemoved"


def get_expiring_loans(
    session: Session,
    loan_expiration_days: list[int],
    batch_size: int,
) -> list[Loan]:
    """
    This function retrieves a batch of loans that have `loan_expiration_days` days left until they
    expire and haven't been notified in the last 24 hours.

    We calculate the days to expiration by comparing the current time to the end time of the loan.

    A loan notification for 3 days left until expiration can be sent when the loan end date is in the
    interval of 3 days left to 2 days + 1 second left until expiration. Because we run this task fairly
    frequently, and we only send a single notification a day, the notification should go out near the
    start of the interval, so the loan will have a little less than 3 days left until expiration when
    the notification is sent.
    """
    now = utc_now()

    if not loan_expiration_days or min(loan_expiration_days) < 1:
        raise PalaceValueError(
            "loan_expiration_days must be a list of positive integers"
        )

    loan_expiration_clauses = [
        and_(
            Loan.end <= now + datetime.timedelta(days=expiration_days),
            Loan.end > now + datetime.timedelta(days=expiration_days - 1),
        )
        for expiration_days in loan_expiration_days
    ]

    loan_query = (
        select(Loan)
        .where(
            or_(
                # Only sent to patrons who have not been notified in the last 24 hours
                Loan.patron_last_notified == None,
                Loan.patron_last_notified < now - datetime.timedelta(days=1),
            ),
            or_(*loan_expiration_clauses),
        )
        .order_by(Loan.id)
        .limit(batch_size)
    )

    return session.execute(loan_query).scalars().all()


def get_days_to_expiration(now: datetime.datetime, end: datetime.datetime) -> int:
    """
    This function calculates the number of days between now and the end datetime. This number of
    days is rounded up to the nearest whole day.

    So this function will return 4 for a loan that expires in 96 hours (4 days exactly) and will also
    return 4 for a loan that expires in 73 hours (3 days and 1 hour), but will return 3 for a loan
    that expires in 72 hours (3 days exactly).
    """
    return math.ceil((end - now) / datetime.timedelta(days=1))


def send_loan_expiry_notification(
    send_notifications: SendNotificationsCallable,
    base_url: str,
    loan: Loan,
    days_to_expiry: int,
) -> list[str]:
    """
    Call the send_notifications function with the appropriate data to send a loan expiry notification
    to a patron's devices.
    """
    tokens = loan.patron.device_tokens
    if not tokens:
        log.info(
            f"Patron {loan.patron.authorization_identifier} has no device tokens. "
            f"Cannot send notification."
        )
        return []

    edition = loan.license_pool.presentation_edition
    if edition is None:
        log.error(
            f"Failed to send loan expiry notification because the edition is missing for "
            f"loan '{loan.id}', patron '{loan.patron.authorization_identifier}', lp '{loan.license_pool.id}'"
        )
        return []

    identifier = loan.license_pool.identifier
    library = loan.library
    library_short_name = library.short_name
    library_name = library.name
    title = f"Only {days_to_expiry} {'days' if days_to_expiry != 1 else 'day'} left on your loan!"
    body = f'Your loan for "{edition.title}" at {library_name} is expiring soon'
    data = dict(
        event_type=NotificationType.LOAN_EXPIRY,
        loans_endpoint=f"{base_url}/{library.short_name}/loans",
        type=identifier.type,
        identifier=identifier.identifier,
        library=library_short_name,
        days_to_expiry=str(days_to_expiry),
    )
    if loan.patron.external_identifier:
        data["external_identifier"] = loan.patron.external_identifier
    if loan.patron.authorization_identifier:
        data["authorization_identifier"] = loan.patron.authorization_identifier

    log.info(
        f"Patron {loan.patron.authorization_identifier} has {len(tokens)} device tokens. "
        f"Sending loan expiry notification(s)."
    )
    return send_notifications(tokens, title, body, data)


def get_available_holds(
    session: Session,
    batch_size: int,
) -> list[Hold]:
    """
    This function retrieves a batch of holds that are available for checkout and haven't been
    notified in the last 24 hours.

    NOTE: We exclude Overdrive holds from notifications for now. See inline comment for more info.
    """

    now = utc_now()

    # We explicitly exclude Overdrive holds from notifications until we have a
    # better way to update their position in the hold queue. As is we don't have
    # a good way to do this. See: PP-2048.
    overdrive_data_source = DataSource.lookup(
        session, DataSource.OVERDRIVE, autocreate=True
    )

    query = (
        select(Hold)
        .join(LicensePool)
        .where(
            or_(
                # Only sent to patrons who have not been notified in the last 24 hours
                Hold.patron_last_notified == None,
                Hold.patron_last_notified < now - datetime.timedelta(days=1),
            ),
            LicensePool.data_source_id != overdrive_data_source.id,
            Hold.position == 0,
            Hold.end > now,
        )
        .order_by(Hold.id)
        .limit(batch_size)
    )

    return session.execute(query).scalars().all()


def send_hold_notification(
    send_notifications: SendNotificationsCallable, base_url: str, hold: Hold
) -> list[str]:
    """
    Call the send_notifications function with the appropriate data to send a hold available notification
    to a patron's devices.
    """
    patron = hold.patron
    tokens = patron.device_tokens

    if not tokens:
        log.info(
            f"Patron {patron.authorization_identifier or patron.username} has no device tokens. "
            f"Skipping hold available notification."
        )
        return []

    work = hold.work
    if work is None:
        log.error(
            f"Failed to send hold available notification because the work is missing for "
            f"hold '{hold.id}', patron '{patron.authorization_identifier}'"
        )
        return []

    work_title = work.title
    if work_title is None:
        log.error(
            f"Failed to send hold available notification because title is missing for "
            f"work '{work.id}', hold '{hold.id}', patron '{patron.authorization_identifier}'"
        )
        return []

    log.info(
        f"Notifying patron {patron.authorization_identifier or patron.username} for "
        f"hold: {work_title}. Patron has {len(tokens)} device tokens."
    )
    identifier = hold.license_pool.identifier
    library_name = patron.library.name
    title = "Your hold is available!"
    body = f'Your hold on "{work_title}" is available at {library_name}!'
    data = dict(
        event_type=NotificationType.HOLD_AVAILABLE,
        loans_endpoint=f"{base_url}/{patron.library.short_name}/loans",
        identifier=identifier.identifier,
        type=identifier.type,
        library=patron.library.short_name,
    )
    if patron.external_identifier:
        data["external_identifier"] = patron.external_identifier
    if patron.authorization_identifier:
        data["authorization_identifier"] = patron.authorization_identifier

    return send_notifications(tokens, title, body, data)


def _send_removed_item_notification(
    session: Session,
    send_notifications: SendNotificationsCallable,
    base_url: str,
    data: RemovedItemNotificationData,
    item_type: Literal["loan", "hold"],
    notification_type: NotificationType,
) -> None:
    """
    Helper function to send a notification about a removed loan or hold.

    :param session: Database session (must persist for the entire transaction)
    :param send_notifications: FCM notification sending callable
    :param base_url: Base URL for the application
    :param data: Notification data extracted before deletion
    :param item_type: Type of item being removed ("loan" or "hold")
    :param notification_type: The notification type enum value
    """
    patron = load_from_id(session, Patron, data.patron_id)
    device_tokens = patron.device_tokens
    patron_auth_id = patron.authorization_identifier
    patron_external_id = patron.external_identifier

    if not device_tokens:
        log.info(
            f"Patron {patron_auth_id or data.patron_id} has no device tokens. "
            f"Cannot send {item_type} removed notification."
        )
        return

    title = f'"{data.work_title}" No Longer Available'
    body = (
        f"One of your current {item_type}s has been removed from your shelf and is "
        f"no longer available through {data.library_name}."
    )

    notification_data = dict(
        event_type=notification_type,
        loans_endpoint=f"{base_url}/{data.library_short_name}/loans",
        type=data.identifier.type,
        identifier=data.identifier.identifier,
        library=data.library_short_name,
    )

    if patron_external_id:
        notification_data["external_identifier"] = patron_external_id
    if patron_auth_id:
        notification_data["authorization_identifier"] = patron_auth_id

    log.info(
        f"Sending {item_type} removed notification for '{data.work_title}' to patron "
        f"{patron_auth_id or data.patron_id}. {len(device_tokens)} device tokens."
    )

    send_notifications(device_tokens, title, body, notification_data)


@shared_task(queue=QueueNames.default, bind=True)
def send_loan_removed_notification(
    task: Task,
    data: RemovedItemNotificationData,
) -> None:
    """
    Celery task to send a notification to a patron that their loan has been removed.

    This task is queued by the reaper after deleting loans from REMOVED license pools.

    :param task: The Celery task instance
    :param data: Notification data extracted before deletion
    """
    send_notifications = task.services.fcm.send_notifications
    base_url = task.services.config.sitewide.base_url()

    with task.session() as session:
        _send_removed_item_notification(
            session,
            send_notifications,
            base_url,
            data,
            "loan",
            NotificationType.LOAN_REMOVED,
        )


@shared_task(queue=QueueNames.default, bind=True)
def send_hold_removed_notification(
    task: Task,
    data: RemovedItemNotificationData,
) -> None:
    """
    Celery task to send a notification to a patron that their hold has been removed.

    This task is queued by the reaper after deleting holds from REMOVED license pools.

    :param task: The Celery task instance
    :param data: Notification data extracted before deletion
    """
    send_notifications = task.services.fcm.send_notifications
    base_url = task.services.config.sitewide.base_url()

    with task.session() as session:
        _send_removed_item_notification(
            session,
            send_notifications,
            base_url,
            data,
            "hold",
            NotificationType.HOLD_REMOVED,
        )


@shared_task(queue=QueueNames.default, bind=True)
def loan_expiration(
    task: Task,
    loan_expiration_days: list[int] | None = None,
    batch_size: int = 100,
) -> None:
    now = utc_now()

    send_notifications = task.services.fcm.send_notifications
    base_url = task.services.config.sitewide.base_url()
    with TaskLock(task).lock():
        if not loan_expiration_days:
            loan_expiration_days = [3]

        with task.session() as session:
            loans = get_expiring_loans(session, loan_expiration_days, batch_size)
            for loan in loans:
                # Because our query is based on loan.end being in a specific range, we should
                # never have a loan that doesn't have an end date. But mypy doesn't know this
                # so we check it anyway.
                assert loan.end is not None

                patron = loan.patron
                days = get_days_to_expiration(now, loan.end)

                task.log.info(
                    f"Patron {patron.authorization_identifier} has a loan on "
                    f"({loan.license_pool.identifier.urn}) expiring in {days} days. "
                )
                send_loan_expiry_notification(send_notifications, base_url, loan, days)

                # Update the patrons last notified date, so they don't get spammed with notifications,
                # and so we can filter them out of the next batch. The date gets updated even if we
                # didn't send a notification, so we don't keep trying to send the same notification
                # over and over.
                loan.patron_last_notified = now

                # We explicitly commit the transaction after each loan is processed, so we don't notify
                # the same patron multiple times if a later notification fails.
                session.commit()

    if len(loans) == batch_size:
        # We have more loans to process, requeue the task
        raise task.replace(
            loan_expiration.s(
                loan_expiration_days=loan_expiration_days, batch_size=batch_size
            )
        )

    task.log.info(f"Loan notifications complete")


@shared_task(queue=QueueNames.default, bind=True)
def hold_available(
    task: Task,
    batch_size: int = 100,
) -> None:
    now = utc_now()

    send_notifications = task.services.fcm.send_notifications
    base_url = task.services.config.sitewide.base_url()

    with TaskLock(task).lock():
        with task.session() as session:
            holds = get_available_holds(session, batch_size)
            for hold in holds:
                # Because our query has conditions on hold.end it should never be None, but mypy
                # doesn't know that, so we assert it to fail fast if somehow it happens.
                assert hold.end is not None

                patron = hold.patron

                task.log.info(
                    f"Patron {patron.authorization_identifier} has a hold that is available on "
                    f"({hold.license_pool.identifier.urn}) sending notification. "
                )
                send_hold_notification(send_notifications, base_url, hold)

                # Update the patrons last notified date, so they don't get spammed with notifications,
                # and so we can filter them out of the next batch. The date gets updated even if we
                # didn't send a notification, so we don't keep trying to send the same notification
                # over and over.
                hold.patron_last_notified = now

                # We explicitly commit the transaction after each hold is processed, so we don't notify
                # the same patron multiple times if a later notification fails.
                session.commit()

    if len(holds) == batch_size:
        # We have more holds to process, requeue the task
        raise task.replace(hold_available.s(batch_size=batch_size))

    task.log.info(f"Hold available notifications complete")
