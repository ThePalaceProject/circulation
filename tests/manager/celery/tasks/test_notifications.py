from __future__ import annotations

import datetime
from functools import partial
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from freezegun import freeze_time

from palace.manager.celery.tasks import notifications
from palace.manager.celery.tasks.notifications import NotificationType
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.model.patron import Hold, Loan, Patron
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture


class NotificationsFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ) -> None:
        self.db = db
        self.services_fixture = services_fixture
        self.services = services_fixture.services

        self.mock_app = MagicMock()
        self.mock_send_notifications = MagicMock()

        self.services.fcm.app.override(self.mock_app)
        self.services.fcm.send_notifications.override(self.mock_send_notifications)

    def create_loan(
        self,
        *,
        patron: Patron | None = None,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        last_notified: datetime.datetime | None = None,
    ) -> Loan:
        last_week = utc_now() - datetime.timedelta(days=7)
        if patron is None:
            patron = self.db.patron()
        work = self.db.work(with_license_pool=True)
        loan, _ = work.active_license_pool().loan_to(
            patron,
            start or last_week,
            end,
        )
        if last_notified:
            loan.patron_last_notified = last_notified
        return loan

    def create_hold(
        self,
        *,
        patron: Patron | None = None,
        start: datetime.datetime | None = None,
        end: datetime.datetime | None = None,
        last_notified: datetime.datetime | None = None,
        data_source_name: str | None = None,
        position: int | None = 0,
    ) -> Hold:
        last_week = utc_now() - datetime.timedelta(days=7)
        tomorrow = utc_now() + datetime.timedelta(days=1)
        if patron is None:
            patron = self.db.patron()

        work = self.db.work(with_license_pool=True, data_source_name=data_source_name)
        hold, _ = work.active_license_pool().on_hold_to(
            patron, position=position, start=start or last_week, end=end or tomorrow
        )

        if last_notified:
            hold.patron_last_notified = last_notified
        return hold

    def create_device_token(
        self, patron: Patron | None = None, token: str = "token"
    ) -> DeviceToken:
        if patron is None:
            patron = self.db.patron()

        device_token, _ = get_one_or_create(
            self.db.session,
            DeviceToken,
            device_token=token,
            token_type=DeviceTokenTypes.FCM_ANDROID,
            patron=patron,
        )

        return device_token


@pytest.fixture
def notifications_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> NotificationsFixture:
    return NotificationsFixture(db, services_fixture)


def test_get_expiring_loans(
    db: DatabaseTransactionFixture,
    notifications_fixture: NotificationsFixture,
) -> None:
    get_expiring_loans = partial(
        notifications.get_expiring_loans, db.session, batch_size=100
    )

    # Test error cases. We should always have a list of positive integers.
    with pytest.raises(
        ValueError, match="loan_expiration_days must be a list of positive integers"
    ):
        get_expiring_loans([])

    with pytest.raises(
        ValueError, match="loan_expiration_days must be a list of positive integers"
    ):
        get_expiring_loans([0, 5])

    # Create some loans with different expiration dates.
    now = utc_now()

    loan_expiring_5_days = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=4, hours=23, minutes=59)
    )
    # Since this loan has already been notified, it should not be included in the results
    loan_expiring_5_days_already_notified = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=4, hours=1),
        last_notified=now - datetime.timedelta(hours=23),
    )
    loan_expiring_4_days = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=3, hours=1)
    )
    # This loan has already been notified, but it was notified more than 24 hours ago, so it should be included
    # in the results
    loan_expiring_3_days = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=2, hours=21),
        last_notified=now - datetime.timedelta(days=2),
    )

    loans = get_expiring_loans([5, 3])
    assert loans == [
        loan_expiring_5_days,
        loan_expiring_3_days,
    ]

    loans = get_expiring_loans([3, 4])
    assert loans == [
        loan_expiring_4_days,
        loan_expiring_3_days,
    ]

    loans = get_expiring_loans([5])
    assert loans == [loan_expiring_5_days]

    loans = get_expiring_loans([4])
    assert loans == [loan_expiring_4_days]

    loans = get_expiring_loans([3])
    assert loans == [loan_expiring_3_days]

    loans = get_expiring_loans([2])
    assert loans == []

    loans = get_expiring_loans([1])
    assert loans == []

    # Test batch size.
    loans = get_expiring_loans([3, 5], batch_size=1)
    assert loans == [loan_expiring_5_days]

    # Mark that loan as notified and try again.
    loan_expiring_5_days.patron_last_notified = now
    loans = get_expiring_loans([3, 5], batch_size=1)
    assert loans == [loan_expiring_3_days]


@pytest.mark.parametrize(
    "delta, expected",
    [
        (datetime.timedelta(days=5), 5),
        (datetime.timedelta(days=4, hours=22), 5),
        (datetime.timedelta(days=4, seconds=1), 5),
        (datetime.timedelta(days=4), 4),
        (datetime.timedelta(days=3, hours=23, minutes=59), 4),
        (datetime.timedelta(days=3), 3),
        (datetime.timedelta(days=2), 2),
        (datetime.timedelta(days=1), 1),
        (datetime.timedelta(hours=23), 1),
        (datetime.timedelta(seconds=1), 1),
        (datetime.timedelta(days=0), 0),
    ],
)
def test_get_days_to_expiration(delta: datetime.timedelta, expected: int) -> None:
    now = utc_now()
    assert notifications.get_days_to_expiration(now, now + delta) == expected


def test_send_loan_expiry_notification(
    db: DatabaseTransactionFixture,
    notifications_fixture: NotificationsFixture,
    caplog: pytest.LogCaptureFixture,
) -> None:
    patron = db.patron(external_identifier="xyz1")
    patron.authorization_identifier = "abc1"
    device_token = notifications_fixture.create_device_token(patron)
    loan = notifications_fixture.create_loan(patron=patron)

    mock_send_notifications = MagicMock()
    base_url = "http://test.cm"
    notifications.send_loan_expiry_notification(
        mock_send_notifications, base_url, loan, 1
    )

    library = loan.library
    work = loan.license_pool.work
    assert library is not None

    mock_send_notifications.assert_called_once_with(
        [device_token],
        "Only 1 day left on your loan!",
        f'Your loan for "{work.presentation_edition.title}" at {library.name} is expiring soon',
        dict(
            event_type=NotificationType.LOAN_EXPIRY,
            loans_endpoint=f"{base_url}/{library.short_name}/loans",
            external_identifier=patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
            identifier=work.presentation_edition.primary_identifier.identifier,
            type=work.presentation_edition.primary_identifier.type,
            library=library.short_name,
            days_to_expiry="1",
        ),
    )

    # Test with no edition
    caplog.clear()
    caplog.set_level(LogLevel.error)
    mock_send_notifications.reset_mock()
    loan.license_pool.presentation_edition = None
    notifications.send_loan_expiry_notification(
        mock_send_notifications, base_url, loan, 1
    )
    mock_send_notifications.assert_not_called()
    assert (
        f"Failed to send loan expiry notification because the edition is missing"
        in caplog.text
    )

    # Test with no device tokens
    caplog.clear()
    caplog.set_level(LogLevel.info)
    mock_send_notifications.reset_mock()
    loan.patron.device_tokens = []
    notifications.send_loan_expiry_notification(
        mock_send_notifications, base_url, loan, 1
    )
    mock_send_notifications.assert_not_called()
    assert (
        f"Patron {loan.patron.authorization_identifier} has no device tokens"
        in caplog.text
    )


@pytest.mark.parametrize(
    "position, end_delta, last_notified_delta, expected",
    [
        pytest.param(
            1,
            datetime.timedelta(days=2),
            None,
            False,
            id="patron in position 1",
        ),
        pytest.param(
            0,
            datetime.timedelta(days=2),
            datetime.timedelta(days=-1),
            True,
            id="patron notified yesterday",
        ),
        pytest.param(
            0,
            datetime.timedelta(days=2),
            None,
            True,
            id="patron never notified",
        ),
        pytest.param(
            None,
            datetime.timedelta(days=2),
            None,
            False,
            id="no hold position",
        ),
        pytest.param(
            0,
            datetime.timedelta(days=2),
            datetime.timedelta(days=0),
            False,
            id="already notified today",
        ),
        pytest.param(
            0,
            datetime.timedelta(days=-1),
            datetime.timedelta(days=-1),
            False,
            id="hold expired",
        ),
    ],
)
def test_get_available_holds(
    db: DatabaseTransactionFixture,
    notifications_fixture: NotificationsFixture,
    position: int | None,
    end_delta: datetime.timedelta | None,
    last_notified_delta: datetime.timedelta | None,
    expected: bool,
) -> None:
    now = utc_now()

    end = now + end_delta if end_delta is not None else None
    patron_last_notified = (
        now + last_notified_delta if last_notified_delta is not None else None
    )

    hold = notifications_fixture.create_hold(
        position=position,
        end=end,
        last_notified=patron_last_notified,
    )

    # Only position 0 holds, that haven't bene notified today, should be queried for
    holds = notifications.get_available_holds(db.session, 100)
    if expected:
        assert holds == [hold]
    else:
        assert holds == []


def test_get_available_holds_ignores_overdrive(
    db: DatabaseTransactionFixture,
    notifications_fixture: NotificationsFixture,
) -> None:

    patron = db.patron()
    od_work = db.work(with_license_pool=True, data_source_name=DataSource.OVERDRIVE)
    od_hold, _ = od_work.active_license_pool().on_hold_to(
        patron, position=0, end=utc_now() + datetime.timedelta(days=1)
    )

    end_date = utc_now() + datetime.timedelta(days=1)

    od_hold = notifications_fixture.create_hold(
        position=0, end=end_date, data_source_name=DataSource.OVERDRIVE
    )
    non_od_hold = notifications_fixture.create_hold(position=0, end=end_date)

    assert notifications.get_available_holds(db.session, 100) == [non_od_hold]


def test_send_hold_notification(
    db: DatabaseTransactionFixture,
    notifications_fixture: NotificationsFixture,
    caplog: pytest.LogCaptureFixture,
):
    patron = db.patron()
    patron.authorization_identifier = "auth1"
    token1 = notifications_fixture.create_device_token(patron, "test-token-1")
    token2 = notifications_fixture.create_device_token(patron, "test-token-2")

    hold = notifications_fixture.create_hold(patron=patron)
    mock_send_notifications = MagicMock()
    base_url = "http://test.cm"

    send_hold_notification = partial(
        notifications.send_hold_notification, mock_send_notifications, base_url, hold
    )

    send_hold_notification()

    work = hold.work
    library = hold.library

    mock_send_notifications.assert_called_once_with(
        [token1, token2],
        "Your hold is available!",
        f'Your hold on "{work.title}" is available at {hold.library.name}!',
        dict(
            event_type=NotificationType.HOLD_AVAILABLE,
            loans_endpoint=f"{base_url}/{library.short_name}/loans",
            identifier=hold.license_pool.identifier.identifier,
            type=hold.license_pool.identifier.type,
            library=hold.patron.library.short_name,
            external_identifier=hold.patron.external_identifier,
            authorization_identifier=patron.authorization_identifier,
        ),
    )

    # Work with no title
    caplog.clear()
    caplog.set_level(LogLevel.error)
    mock_send_notifications.reset_mock()
    hold.work.presentation_edition = None
    assert send_hold_notification() == []
    mock_send_notifications.assert_not_called()
    assert (
        f"Failed to send hold available notification because title is missing"
        in caplog.text
    )

    # Hold with no work
    caplog.clear()
    caplog.set_level(LogLevel.error)
    mock_send_notifications.reset_mock()
    hold.license_pool.work = None
    assert send_hold_notification() == []
    mock_send_notifications.assert_not_called()
    assert (
        f"Failed to send hold available notification because the work is missing"
        in caplog.text
    )

    # No device tokens
    caplog.clear()
    caplog.set_level(LogLevel.info)
    mock_send_notifications.reset_mock()
    hold.patron.device_tokens = []
    assert send_hold_notification() == []
    mock_send_notifications.assert_not_called()
    assert (
        f"Patron {hold.patron.authorization_identifier} has no device tokens"
        in caplog.text
    )


@freeze_time()
def test_loan_expiration_task(
    notifications_fixture: NotificationsFixture,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    services_fixture: ServicesFixture,
) -> None:
    # Do a full run of the loan expiration task, including sending notifications.
    now = utc_now()

    # Two loans needing notifications
    loan1 = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=2, hours=23)
    )
    loan2 = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=2, hours=22)
    )

    # Since we set the batch size to 1, we should only process one loan per batch. The task should
    # requeue itself until there are no loans left to process.
    with patch.object(
        notifications, "send_loan_expiry_notification"
    ) as mock_send_loan_expiry:
        notifications.loan_expiration.delay(batch_size=1).wait()

    # We should have called send_notifications twice, once for each loan.
    assert mock_send_loan_expiry.call_count == 2
    mock_send_loan_expiry.assert_has_calls(
        [
            call(
                ANY,
                ANY,
                loan1,
                3,
            ),
            call(
                ANY,
                ANY,
                loan2,
                3,
            ),
        ]
    )

    # We should have updated the patron_last_notified field for both loans
    assert loan1.patron_last_notified == now
    assert loan2.patron_last_notified == now


@freeze_time()
def test_loan_expiration_task_exception(
    notifications_fixture: NotificationsFixture,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    services_fixture: ServicesFixture,
) -> None:
    # If there is an exception, any notifications send should have their patron_last_notified
    # field updated.
    now = utc_now()

    # Two loans needing notifications
    loan1 = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=2, hours=23)
    )
    loan2 = notifications_fixture.create_loan(
        end=now + datetime.timedelta(days=2, hours=22)
    )

    # Both loans should be processed in a single batch, with the second loan causing an exception.
    with patch.object(
        notifications, "send_loan_expiry_notification"
    ) as mock_send_loan_expiry:
        mock_send_loan_expiry.side_effect = [True, Exception("Test exception")]
        with pytest.raises(Exception, match="Test exception"):
            notifications.loan_expiration.delay().wait()

    # We should have called send_notifications twice, once for each loan.
    assert mock_send_loan_expiry.call_count == 2

    # We should have updated the patron_last_notified field for the processed loan
    assert loan1.patron_last_notified == now
    assert loan2.patron_last_notified is None


@freeze_time()
def test_hold_available_task(
    notifications_fixture: NotificationsFixture,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    services_fixture: ServicesFixture,
) -> None:
    now = utc_now()

    # Two holds needing notifications
    hold1 = notifications_fixture.create_hold()
    hold2 = notifications_fixture.create_hold()

    with patch.object(
        notifications, "send_hold_notification"
    ) as mock_send_hold_notification:
        notifications.hold_available.delay(batch_size=1).wait()

    assert mock_send_hold_notification.call_count == 2
    mock_send_hold_notification.assert_has_calls(
        [
            call(
                ANY,
                ANY,
                hold1,
            ),
            call(
                ANY,
                ANY,
                hold2,
            ),
        ]
    )

    # We should have updated patron_last_notified
    assert hold1.patron_last_notified == now
    assert hold2.patron_last_notified == now


def test_hold_available_task_exception(
    notifications_fixture: NotificationsFixture,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    services_fixture: ServicesFixture,
) -> None:
    now = utc_now()

    hold1 = notifications_fixture.create_hold()
    hold2 = notifications_fixture.create_hold()
    hold3 = notifications_fixture.create_hold()

    with patch.object(
        notifications, "send_hold_notification"
    ) as mock_send_hold_notification:
        mock_send_hold_notification.side_effect = [True, True, Exception("Bang")]
        with pytest.raises(Exception, match="Bang"):
            notifications.hold_available.delay(batch_size=10).wait()

    # We should have called mock_send_hold_notification three times, once for each hold.
    assert mock_send_hold_notification.call_count == 3

    # We should have updated the patron_last_notified field for successfully processed holds
    assert hold1.patron_last_notified is not None
    assert hold2.patron_last_notified is not None
    assert hold3.patron_last_notified is None
