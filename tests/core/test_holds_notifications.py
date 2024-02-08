import datetime
from unittest.mock import call, create_autospec, patch

import pytest

from core.jobs.holds_notification import HoldsNotificationMonitor
from core.util.datetime_helpers import utc_now
from core.util.notifications import PushNotifications
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class HoldsNotificationFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.mock_notifications = create_autospec(PushNotifications)
        self.monitor = HoldsNotificationMonitor(
            self.db.session, notifications=self.mock_notifications
        )


@pytest.fixture(scope="function")
def holds_fixture(db: DatabaseTransactionFixture) -> HoldsNotificationFixture:
    return HoldsNotificationFixture(db)


class TestHoldsNotifications:
    def test_item_query(self, holds_fixture: HoldsNotificationFixture):
        db = holds_fixture.db
        patron1 = db.patron()
        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        work3 = db.work(with_license_pool=True)
        work4 = db.work(with_license_pool=True)
        work5 = db.work(with_license_pool=True)
        hold1, _ = work1.active_license_pool().on_hold_to(patron1, position=1)
        hold2, _ = work2.active_license_pool().on_hold_to(patron1, position=0)
        hold3, _ = work3.active_license_pool().on_hold_to(patron1, position=0)
        hold4, _ = work4.active_license_pool().on_hold_to(patron1, position=None)
        hold5, _ = work5.active_license_pool().on_hold_to(patron1, position=0)
        hold5.patron_last_notified = utc_now().date()
        hold2.patron_last_notified = utc_now().date() - datetime.timedelta(days=1)

        # Only position 0 holds, that haven't bene notified today, should be queried for
        assert holds_fixture.monitor.item_query().all() == [hold2, hold3]

    def test_script_run(self, holds_fixture: HoldsNotificationFixture):
        db = holds_fixture.db
        patron1 = db.patron()
        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        hold1, _ = work1.active_license_pool().on_hold_to(patron1, position=0)
        hold2, _ = work2.active_license_pool().on_hold_to(patron1, position=0)

        holds_fixture.monitor.run()
        assert holds_fixture.mock_notifications.send_holds_notifications.call_count == 1
        assert (
            holds_fixture.mock_notifications.send_holds_notifications.call_args_list
            == [call([hold1, hold2])]
        )

    def test_constructor(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        services_fixture.set_base_url("http://test-circulation-manager")
        with patch(
            "core.jobs.holds_notification.PushNotifications", autospec=True
        ) as mock_notifications:
            monitor = HoldsNotificationMonitor(db.session)
        assert monitor.notifications == mock_notifications.return_value
        mock_notifications.assert_called_once_with("http://test-circulation-manager")
