from unittest.mock import call, patch

from core.jobs.holds_notification import HoldsNotificationMonitor
from core.testing import DatabaseTest


class TestHoldsNotifications(DatabaseTest):
    def setup_method(self):
        super().setup_method()
        self.monitor = HoldsNotificationMonitor(self._db)

    def test_item_query(self):
        patron1 = self._patron()
        work1 = self._work(with_license_pool=True)
        work2 = self._work(with_license_pool=True)
        work3 = self._work(with_license_pool=True)
        work4 = self._work(with_license_pool=True)
        hold1, _ = work1.active_license_pool().on_hold_to(patron1, position=1)
        hold2, _ = work2.active_license_pool().on_hold_to(patron1, position=0)
        hold3, _ = work3.active_license_pool().on_hold_to(patron1, position=0)
        hold4, _ = work4.active_license_pool().on_hold_to(patron1, position=None)

        # Only position 0 holds should be queried for
        assert self.monitor.item_query().all() == [hold2, hold3]

    @patch("core.jobs.holds_notification.PushNotifications")
    def test_script_run(self, mock_notf):
        patron1 = self._patron()
        work1 = self._work(with_license_pool=True)
        work2 = self._work(with_license_pool=True)
        hold1, _ = work1.active_license_pool().on_hold_to(patron1, position=0)
        hold2, _ = work2.active_license_pool().on_hold_to(patron1, position=0)
        self.monitor.run()
        assert mock_notf.send_holds_notifications.call_count == 1
        assert mock_notf.send_holds_notifications.call_args_list == [
            call([hold1, hold2])
        ]
