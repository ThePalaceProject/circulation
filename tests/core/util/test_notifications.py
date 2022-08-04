from unittest import mock

from core.config import Configuration
from core.model import create, get_one_or_create
from core.model.configuration import ConfigurationSetting
from core.model.constants import NotificationConstants
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.work import Work
from core.testing import DatabaseTest
from core.util.notifications import PushNotifications


class TestPushNotifications(DatabaseTest):
    def setup_method(self):
        super().setup_method()
        PushNotifications.TESTING_MODE = True
        setting = ConfigurationSetting.sitewide(self._db, Configuration.BASE_URL_KEY)
        setting.value = "http://localhost"

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
                    event_type=NotificationConstants.LOAN_EXPIRY_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron.external_identifier,
                    authorization_identifier=patron.authorization_identifier,
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

    @mock.patch("core.util.notifications.PushNotifications.fcm_app")
    @mock.patch("core.util.notifications.messaging")
    def test_send_activity_sync(
        self, messaging: mock.MagicMock, fcm_app: mock.MagicMock
    ):
        patron1 = self._patron()
        patron2 = self._patron()
        patron3 = self._patron()

        tokens = []
        for patron in (patron1, patron2, patron3):
            t, _ = create(
                self._db,
                DeviceToken,
                patron=patron,
                device_token=f"ios-token-{patron.id}",
                token_type=DeviceTokenTypes.FCM_IOS,
            )
            tokens.append(t)
            t, _ = create(
                self._db,
                DeviceToken,
                patron=patron,
                device_token=f"android-token-{patron.id}",
                token_type=DeviceTokenTypes.FCM_ANDROID,
            )
            tokens.append(t)

        # Notify 2 patrons of 3 total
        PushNotifications.send_activity_sync_message([patron1, patron2])
        assert messaging.Message.call_count == 4
        assert messaging.Message.call_args_list == [
            mock.call(
                token=tokens[0].device_token,
                data=dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron1.external_identifier,
                    authorization_identifier=patron1.authorization_identifier,
                ),
            ),
            mock.call(
                token=tokens[1].device_token,
                data=dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron1.external_identifier,
                    authorization_identifier=patron1.authorization_identifier,
                ),
            ),
            mock.call(
                token=tokens[2].device_token,
                data=dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron2.external_identifier,
                    authorization_identifier=patron2.authorization_identifier,
                ),
            ),
            mock.call(
                token=tokens[3].device_token,
                data=dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron2.external_identifier,
                    authorization_identifier=patron2.authorization_identifier,
                ),
            ),
        ]

        assert messaging.send_all.call_count == 1
