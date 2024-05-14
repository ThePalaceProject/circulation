from __future__ import annotations

import datetime
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest
from freezegun import freeze_time

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.scripts.notifications.loan import LoanNotificationsScript
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.notifications import PushNotifications
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class TestLoanNotificationsScript:
    TEST_NOTIFICATION_DAYS = [5, 3]
    PER_DAY_NOTIFICATION_EXPECTATIONS = (
        # These days should NOT trigger a notification.
        (7, False),
        (6, False),
        (4, False),
        (2, False),
        (1, False),
        # These days SHOULD trigger a notification.
        (5, True),
        (3, True),
    )
    PARAMETRIZED_POSSIBLE_NOTIFICATION_DAYS = (
        "days_remaining, is_notification_expected",
        PER_DAY_NOTIFICATION_EXPECTATIONS,
    )

    def _setup_method(self, db: DatabaseTransactionFixture):
        self.mock_notifications = create_autospec(PushNotifications)
        self.script = LoanNotificationsScript(
            _db=db.session,
            notifications=self.mock_notifications,
            loan_expiration_days=self.TEST_NOTIFICATION_DAYS,
        )
        self.patron: Patron = db.patron()
        self.work: Work = db.work(with_license_pool=True)
        self.device_token, _ = get_one_or_create(
            db.session,
            DeviceToken,
            patron=self.patron,
            token_type=DeviceTokenTypes.FCM_ANDROID,
            device_token="atesttoken",
        )

    @pytest.mark.parametrize(*PARAMETRIZED_POSSIBLE_NOTIFICATION_DAYS)
    def test_loan_notification(
        self,
        db: DatabaseTransactionFixture,
        days_remaining: int,
        is_notification_expected: bool,
    ):
        self._setup_method(db)
        p = self.work.active_license_pool()

        # `mypy` thinks `p` is an `Optional[LicensePool]`, so let's clear that up.
        assert isinstance(p, LicensePool)

        loan, _ = p.loan_to(
            self.patron,
            utc_now(),
            utc_now() + datetime.timedelta(days=days_remaining, hours=1),
        )
        self.script.process_loan(loan)

        expected_call_count = 1 if is_notification_expected else 0
        expected_call_args = (
            [(loan, days_remaining, [self.device_token])]
            if is_notification_expected
            else None
        )

        assert (
            self.mock_notifications.send_loan_expiry_message.call_count
            == expected_call_count
        ), f"Unexpected call count for {days_remaining} day(s) remaining."
        assert (
            self.mock_notifications.send_loan_expiry_message.call_args
            == expected_call_args
        ), f"Unexpected call args for {days_remaining} day(s) remaining."

    def test_send_all_notifications(self, db: DatabaseTransactionFixture):
        self._setup_method(db)
        p = self.work.active_license_pool()

        # `mypy` thinks `p` is an `Optional[LicensePool]`, so let's clear that up.
        assert isinstance(p, LicensePool)

        loan_start_time = utc_now()
        loan_end_time = loan_start_time + datetime.timedelta(days=21)
        loan, _ = p.loan_to(self.patron, loan_start_time, loan_end_time)

        # Simulate multiple days of notification checks on a single loan, counting down to loan expiration.
        # This needs to happen within the same test, so that we use the same loan each time.
        for days_remaining, expect_notification in sorted(
            self.PER_DAY_NOTIFICATION_EXPECTATIONS, reverse=True
        ):
            with freeze_time(loan_end_time - datetime.timedelta(days=days_remaining)):
                self.mock_notifications.send_loan_expiry_message.reset_mock()
                self.script.process_loan(loan)

                expected_call_count = 1 if expect_notification else 0
                expected_call_args = (
                    [(loan, days_remaining, [self.device_token])]
                    if expect_notification
                    else None
                )

                assert (
                    self.mock_notifications.send_loan_expiry_message.call_count
                    == expected_call_count
                ), f"Unexpected call count for {days_remaining} day(s) remaining."
                assert (
                    self.mock_notifications.send_loan_expiry_message.call_args
                    == expected_call_args
                ), f"Unexpected call args for {days_remaining} day(s) remaining."

    def test_do_run(self, db: DatabaseTransactionFixture):
        now = utc_now()
        self._setup_method(db)
        pool = self.work.active_license_pool()
        assert pool is not None
        loan, _ = pool.loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=1, hours=1),
        )

        work2 = db.work(with_license_pool=True)
        pool2 = work2.active_license_pool()
        assert pool2 is not None
        loan2, _ = pool2.loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=2, hours=1),
        )

        work3 = db.work(with_license_pool=True)
        p = work3.active_license_pool()
        loan3, _ = p.loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=1, hours=1),
        )
        # loan 3 was notified today already, so should get skipped
        loan3.patron_last_notified = now.date()

        work4 = db.work(with_license_pool=True)
        p = work4.active_license_pool()
        loan4, _ = p.loan_to(
            self.patron,
            now,
            now + datetime.timedelta(days=1, hours=1),
        )
        # loan 4 was notified yesterday, so should NOT get skipped
        loan4.patron_last_notified = now.date() - datetime.timedelta(days=1)

        self.script.process_loan = MagicMock()
        self.script.BATCH_SIZE = 1
        self.script.do_run()

        assert self.script.process_loan.call_count == 3
        assert self.script.process_loan.call_args_list == [
            call(loan),
            call(loan2),
            call(loan4),
        ]

    def test_constructor(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        """Test that the constructor sets up the script correctly."""
        services_fixture.set_base_url("http://test-circulation-manager")
        mock_app = MagicMock()
        services_fixture.services.fcm.app.override(mock_app)
        with patch(
            "palace.manager.core.scripts.PushNotifications", autospec=True
        ) as mock_notifications:
            script = LoanNotificationsScript(
                db.session, services=services_fixture.services
            )
        assert script.BATCH_SIZE == 100
        assert (
            script.loan_expiration_days
            == LoanNotificationsScript.DEFAULT_LOAN_EXPIRATION_DAYS
        )
        assert script.notifications == mock_notifications.return_value
        mock_notifications.assert_called_once_with(
            "http://test-circulation-manager", mock_app
        )

        with patch(
            "palace.manager.core.scripts.PushNotifications", autospec=True
        ) as mock_notifications:
            script = LoanNotificationsScript(
                db.session,
                services=services_fixture.services,
                loan_expiration_days=[-2, 0, 220],
            )
        assert script.BATCH_SIZE == 100
        assert script.loan_expiration_days == [-2, 0, 220]
        assert script.notifications == mock_notifications.return_value
        mock_notifications.assert_called_once_with(
            "http://test-circulation-manager", mock_app
        )

        # Make sure we get an exception if the base_url is not set.
        services_fixture.set_base_url(None)
        with pytest.raises(CannotLoadConfiguration) as excinfo:
            LoanNotificationsScript(db.session, services=services_fixture.services)

        assert "Missing required environment variable: PALACE_BASE_URL" in str(
            excinfo.value
        )
