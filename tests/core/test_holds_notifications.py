import datetime
from unittest.mock import call, patch

import pytest

from core.config import Configuration, ConfigurationConstants
from core.jobs.holds_notification import HoldsNotificationMonitor
from core.model.configuration import ConfigurationSetting
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class HoldsNotificationFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.monitor = HoldsNotificationMonitor(self.db.session)


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

        with patch("core.jobs.holds_notification.PushNotifications") as mock_notf:
            holds_fixture.monitor.run()
            assert mock_notf.send_holds_notifications.call_count == 1
            assert mock_notf.send_holds_notifications.call_args_list == [
                call([hold1, hold2])
            ]

            # Sitewide notifications are turned off
            mock_notf.send_holds_notifications.reset_mock()
            ConfigurationSetting.sitewide(
                db.session, Configuration.PUSH_NOTIFICATIONS_STATUS
            ).value = ConfigurationConstants.FALSE
            holds_fixture.monitor.run()
            assert mock_notf.send_holds_notifications.call_count == 0
