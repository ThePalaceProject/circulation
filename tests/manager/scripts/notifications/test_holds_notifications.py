import datetime
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest

from palace.manager.scripts.notifications.holds_notification import (
    HoldsNotificationMonitor,
)
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.notifications import PushNotifications
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
    @pytest.mark.parametrize(
        "position, end_delta, last_notified_delta, expected",
        [
            pytest.param(
                1, datetime.timedelta(days=2), None, False, id="patron in position 1"
            ),
            pytest.param(
                0,
                datetime.timedelta(days=2),
                datetime.timedelta(days=-1),
                True,
                id="patron notified yesterday",
            ),
            pytest.param(
                0, datetime.timedelta(days=2), None, True, id="patron never notified"
            ),
            pytest.param(
                None, datetime.timedelta(days=2), None, False, id="no hold position"
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
    def test_item_query(
        self,
        holds_fixture: HoldsNotificationFixture,
        position: int | None,
        end_delta: datetime.timedelta | None,
        last_notified_delta: datetime.timedelta | None,
        expected: bool,
    ):
        now = utc_now()

        end = now + end_delta if end_delta is not None else None
        patron_last_notified = (
            now.date() + last_notified_delta
            if last_notified_delta is not None
            else None
        )

        db = holds_fixture.db
        patron = db.patron()
        work = db.work(with_license_pool=True)
        hold, _ = work.active_license_pool().on_hold_to(
            patron, position=position, end=end
        )
        hold.patron_last_notified = patron_last_notified

        # Only position 0 holds, that haven't bene notified today, should be queried for
        query = holds_fixture.monitor.item_query()
        if expected:
            assert query.all() == [hold]
        else:
            assert query.all() == []

    def test_item_query_ignores_overdrive(
        self, holds_fixture: HoldsNotificationFixture
    ):
        db = holds_fixture.db
        patron = db.patron()
        od_work = db.work(with_license_pool=True, data_source_name=DataSource.OVERDRIVE)
        od_hold, _ = od_work.active_license_pool().on_hold_to(
            patron, position=0, end=utc_now() + datetime.timedelta(days=1)
        )

        non_od_work = db.work(
            with_license_pool=True, data_source_name=DataSource.AXIS_360
        )
        non_od_hold, _ = non_od_work.active_license_pool().on_hold_to(
            patron, position=0, end=utc_now() + datetime.timedelta(days=1)
        )

        query = holds_fixture.monitor.item_query()
        assert query.all() == [non_od_hold]

    def test_script_run(self, holds_fixture: HoldsNotificationFixture):
        db = holds_fixture.db
        patron1 = db.patron()
        work1 = db.work(with_license_pool=True)
        work2 = db.work(with_license_pool=True)
        tomorrow = utc_now() + datetime.timedelta(days=1)
        hold1, _ = work1.active_license_pool().on_hold_to(
            patron1, end=tomorrow, position=0
        )
        hold2, _ = work2.active_license_pool().on_hold_to(
            patron1, end=tomorrow, position=0
        )

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
        mock_app = MagicMock()
        services_fixture.services.fcm.app.override(mock_app)

        with patch(
            "palace.manager.scripts.notifications.holds_notification.PushNotifications",
            autospec=True,
        ) as mock_notifications:
            monitor = HoldsNotificationMonitor(db.session)
        assert monitor.notifications == mock_notifications.return_value
        mock_notifications.assert_called_once_with(
            "http://test-circulation-manager", mock_app
        )
