from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest

from core.jobs.patron_activity_sync import PatronActivitySyncNotificationScript
from core.model import create
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.licensing import LicensePool
from core.model.patron import Patron
from core.model.work import Work
from core.util.datetime_helpers import utc_now
from core.util.notifications import PushNotifications
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class PatronSyncFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.mock_notifications = create_autospec(PushNotifications)
        self.monitor = PatronActivitySyncNotificationScript(
            self.db.session, batch_size=100, notifications=self.mock_notifications
        )


@pytest.fixture(scope="function")
def sync_fixture(db: DatabaseTransactionFixture):
    return PatronSyncFixture(db)


class TestPatronActivitySync:
    def test_item_query(self, sync_fixture: PatronSyncFixture):
        db = sync_fixture.db

        work: Work | None = db.work(
            with_license_pool=True, with_open_access_download=True
        )
        assert work is not None

        pool: LicensePool | None = work.active_license_pool()
        assert pool is not None

        patron1: Patron = db.patron()  # 0 loans, holds or tokens
        patron2: Patron = db.patron()  # 0 loan, holds, 1 token
        patron3: Patron = db.patron()  # 1 loan, 0 holds, 1 token
        patron4: Patron = db.patron()  # 0 loan, 1 holds, 1 token
        patron5: Patron = db.patron()  # 1 loan, 1 holds, 0 token

        for info in [
            dict(patron=patron1, loan=False, hold=False, token=False),
            dict(patron=patron2, loan=False, hold=False, token=True),
            dict(patron=patron3, loan=True, hold=False, token=True),
            dict(patron=patron4, loan=False, hold=True, token=True),
            dict(patron=patron5, loan=True, hold=True, token=False),
        ]:
            patron: Patron = cast(Patron, info["patron"])
            if info["loan"]:
                pool.loan_to(patron)
            if info["hold"]:
                pool.on_hold_to(patron)
            if info["token"]:
                create(
                    db.session,
                    DeviceToken,
                    patron=patron,
                    token_type=DeviceTokenTypes.FCM_ANDROID,
                    device_token=f"test-token-{patron.id}",
                )
            patron._last_loan_activity_sync = utc_now() - timedelta(days=30)

        # Only patrons with loans/holds and tokens will appear
        patrons = sync_fixture.monitor.item_query().all()
        assert sorted(patrons, key=lambda p: p.id) == [patron3, patron4]

        # Patron3 was synced recently, so should not appear
        patron3.last_loan_activity_sync = utc_now()
        assert sync_fixture.monitor.item_query().all() == [patron4]

    def test_run(self, sync_fixture: PatronSyncFixture):
        db = sync_fixture.db
        work = db.work(with_license_pool=True)
        patron = db.patron()
        patron2 = db.patron()  # no loans, should not be processed
        pool: LicensePool = work.active_license_pool()
        pool.loan_to(patron)

        create(
            db.session,
            DeviceToken,
            patron=patron,
            token_type=DeviceTokenTypes.FCM_ANDROID,
            device_token=f"test-token-{patron.id}",
        )

        sync_fixture.monitor.run()
        assert (
            sync_fixture.mock_notifications.send_activity_sync_message.call_count == 1
        )
        assert (
            sync_fixture.mock_notifications.send_activity_sync_message.call_args
            == call([patron])
        )

    def test_constructor(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        services_fixture.set_base_url("http://test-circulation-manager")
        mock_app = MagicMock()
        services_fixture.services.fcm.app.override(mock_app)

        with patch(
            "core.jobs.patron_activity_sync.PushNotifications", autospec=True
        ) as mock_notifications:
            monitor = PatronActivitySyncNotificationScript(db.session)
        assert monitor.notifications == mock_notifications.return_value
        mock_notifications.assert_called_once_with(
            "http://test-circulation-manager", mock_app
        )
