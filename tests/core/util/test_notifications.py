from unittest import mock

from core.model import get_one_or_create
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.work import Work
from core.testing import DatabaseTest
from core.util.notifications import PushNotifications


class TestPushNotifications(DatabaseTest):
    def setup_method(self):
        PushNotifications.TESTING_MODE = True
        return super().setup_method()

    @mock.patch("core.util.notifications.PushNotifications.fcm_app")
    @mock.patch("core.util.notifications.messaging")
    def test_send_loan_notification(self, messaging, mock_fcm):
        patron = self._patron()
        device_token, _ = get_one_or_create(
            self._db,
            DeviceToken,
            device_token="atoken",
            token_type=DeviceTokenTypes.FCM_ANDROID,
            patron=patron,
        )
        work: Work = self._work(with_license_pool=True)
        loan, _ = work.active_license_pool().loan_to(patron)

        PushNotifications.send_loan_expiry_message(loan, 1, [device_token])

        assert messaging.Message.call_count == 1
        assert messaging.Message.call_args_list[0] == [
            (),
            {
                "token": "atoken",
                "data": dict(
                    title="Only 1 day left on your loan!",
                    body=f"Your loan on {work.presentation_edition.title} is expiring soon",
                    identifier=work.presentation_edition.primary_identifier.identifier,
                    type=work.presentation_edition.primary_identifier.type,
                    library=loan.library.short_name,
                    days_to_expiry=1,
                ),
            },
        ]
        assert messaging.send.call_count == 1
        assert messaging.send.call_args_list[0] == [
            (messaging.Message(),),
            {"dry_run": True, "app": mock_fcm},
        ]
        assert PushNotifications._fcm_app == mock_fcm
