from datetime import timedelta
from unittest.mock import MagicMock, call, patch

from core.jobs.patron_activity_sync import PatronActivitySyncNotificationScript
from core.model import create
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.licensing import LicensePool
from core.model.patron import Patron
from core.model.work import Work
from core.testing import DatabaseTest
from core.util.datetime_helpers import utc_now


class TestPatronActivitySync(DatabaseTest):
    def setup_method(self):
        super().setup_method()
        self.monitor = PatronActivitySyncNotificationScript(self._db, batch_size=100)

    def test_item_query(self):
        work: Work = self._work(with_license_pool=True, with_open_access_download=True)
        pool: LicensePool = work.active_license_pool()
        patron1: Patron = self._patron()  # 0 loans, holds or tokens
        patron2: Patron = self._patron()  # 0 loan, holds, 1 token
        patron3: Patron = self._patron()  # 1 loan, 0 holds, 1 token
        patron4: Patron = self._patron()  # 0 loan, 1 holds, 1 token
        patron5: Patron = self._patron()  # 1 loan, 1 holds, 0 token

        for info in [
            dict(patron=patron1, loan=False, hold=False, token=False),
            dict(patron=patron2, loan=False, hold=False, token=True),
            dict(patron=patron3, loan=True, hold=False, token=True),
            dict(patron=patron4, loan=False, hold=True, token=True),
            dict(patron=patron5, loan=True, hold=True, token=False),
        ]:
            patron: Patron = info["patron"]
            if info["loan"]:
                pool.loan_to(patron)
            if info["hold"]:
                pool.on_hold_to(patron)
            if info["token"]:
                create(
                    self._db,
                    DeviceToken,
                    patron=patron,
                    token_type=DeviceTokenTypes.FCM_ANDROID,
                    device_token=f"test-token-{patron.id}",
                )
            patron._last_loan_activity_sync = utc_now() - timedelta(days=30)

        # Only patrons with loans/holds and tokens will appear
        patrons = self.monitor.item_query().all()
        assert sorted(patrons, key=lambda p: p.id) == [patron3, patron4]

        # Patron3 was synced recently, so should not appear
        patron3.last_loan_activity_sync = utc_now()
        assert self.monitor.item_query().all() == [patron4]

    @patch("core.jobs.patron_activity_sync.PushNotifications")
    def test_run(self, mock_notf: MagicMock):
        work = self._work(with_license_pool=True)
        patron = self._patron()
        patron2 = self._patron()  # no loans, should not be processed
        pool: LicensePool = work.active_license_pool()
        pool.loan_to(patron)

        create(
            self._db,
            DeviceToken,
            patron=patron,
            token_type=DeviceTokenTypes.FCM_ANDROID,
            device_token=f"test-token-{patron.id}",
        )

        self.monitor.run()
        assert mock_notf.send_activity_sync_message.call_count == 1
        assert mock_notf.send_activity_sync_message.call_args == call([patron])
