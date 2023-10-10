import re
from unittest import mock

import firebase_admin
import pytest
from firebase_admin import messaging
from google.auth import credentials
from requests_mock import Mocker

from core.config import Configuration
from core.model import create, get_one_or_create
from core.model.configuration import ConfigurationSetting
from core.model.constants import NotificationConstants
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from core.model.work import Work
from core.util.notifications import PushNotifications
from tests.fixtures.database import DatabaseTransactionFixture


# Mock credential classes pulled directly from the fcm test repository
# https://github.com/firebase/firebase-admin-python/blob/master/tests/testutils.py
class MockGoogleCredential(credentials.Credentials):
    """A mock Google authentication credential."""

    def refresh(self, request):
        self.token = "mock-token"


class MockCredential(firebase_admin.credentials.Base):
    """A mock Firebase credential implementation."""

    def __init__(self):
        self._g_credential = MockGoogleCredential()

    def get_credential(self):
        return self._g_credential


@pytest.fixture
def mock_app() -> firebase_admin.App:
    app = firebase_admin.initialize_app(
        MockCredential(), options=dict(projectId="mock-app-1"), name="testapp"
    )
    yield app
    firebase_admin.delete_app(app)


def assert_send_message(msg: messaging.Message, expected: dict):
    assert expected["token"] == msg.token
    assert expected["data"] == msg.data
    if "notification" in expected:
        assert expected["notification"].title == msg.notification.title
        assert expected["notification"].body == msg.notification.body


class PushNotificationsFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        PushNotifications.TESTING_MODE = True
        setting = ConfigurationSetting.sitewide(
            self.db.session, Configuration.BASE_URL_KEY
        )
        setting.value = "http://localhost"


@pytest.fixture(scope="function")
def push_notf_fixture(db: DatabaseTransactionFixture) -> PushNotificationsFixture:
    return PushNotificationsFixture(db)


class TestPushNotifications:
    def test_send_loan_notification(
        self, push_notf_fixture: PushNotificationsFixture, mock_app: firebase_admin.App
    ):
        db = push_notf_fixture.db
        patron = db.patron(external_identifier="xyz1")
        patron.authorization_identifier = "abc1"

        device_token, _ = get_one_or_create(
            db.session,
            DeviceToken,
            device_token="atoken",
            token_type=DeviceTokenTypes.FCM_ANDROID,
            patron=patron,
        )
        work: Work = db.work(with_license_pool=True)
        loan, _ = work.active_license_pool().loan_to(patron)  # type: ignore

        # Ensure data structuring has no errors
        with mock.patch(
            "core.util.notifications.PushNotifications.fcm_app"
        ) as mock_fcm, Mocker() as mocker:
            mocker.post(
                re.compile("https://fcm.googleapis.com"), json=dict(name="mid-mock")
            )
            mock_fcm.return_value = mock_app
            assert PushNotifications.send_loan_expiry_message(
                loan, 1, [device_token]
            ) == ["mid-mock"]

        # Mock the send method to test actual values
        with mock.patch(
            "core.util.notifications.PushNotifications.fcm_app"
        ) as mock_fcm, mock.patch("core.util.notifications.messaging.send") as send:
            PushNotifications.send_loan_expiry_message(loan, 1, [device_token])

            assert send.call_count == 1
            message = send.call_args[0][0]
            assert message.token == "atoken"
            assert message.notification.title == "Only 1 day left on your loan!"
            assert (
                message.notification.body
                == f"Your loan on {work.presentation_edition.title} is expiring soon"
            )
            assert message.data == dict(
                title="Only 1 day left on your loan!",
                body=f"Your loan on {work.presentation_edition.title} is expiring soon",
                event_type=NotificationConstants.LOAN_EXPIRY_TYPE,
                loans_endpoint="http://localhost/default/loans",
                external_identifier=patron.external_identifier,
                authorization_identifier=patron.authorization_identifier,
                identifier=work.presentation_edition.primary_identifier.identifier,
                type=work.presentation_edition.primary_identifier.type,
                library=loan.library.short_name,
                days_to_expiry="1",
            )
            assert send.call_args[1] == {"dry_run": True, "app": mock_fcm()}

    def test_send_activity_sync(self, push_notf_fixture: PushNotificationsFixture):
        db = push_notf_fixture.db
        # Only patron 1 will get authorization identifiers
        patron1 = db.patron()
        patron1.authorization_identifier = "auth1"
        patron2 = db.patron()
        patron3 = db.patron()

        tokens = []
        for patron in (patron1, patron2, patron3):
            t, _ = create(
                db.session,
                DeviceToken,
                patron=patron,
                device_token=f"ios-token-{patron.id}",
                token_type=DeviceTokenTypes.FCM_IOS,
            )
            tokens.append(t)
            t, _ = create(
                db.session,
                DeviceToken,
                patron=patron,
                device_token=f"android-token-{patron.id}",
                token_type=DeviceTokenTypes.FCM_ANDROID,
            )
            tokens.append(t)

        with mock.patch(
            "core.util.notifications.PushNotifications.fcm_app"
        ) as fcm_app, mock.patch(
            "core.util.notifications.messaging.send_all"
        ) as send_all:
            # Notify 2 patrons of 3 total
            PushNotifications.send_activity_sync_message([patron1, patron2])
            messages = [
                dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron1.external_identifier,
                    authorization_identifier=patron1.authorization_identifier,
                ),
                dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron1.external_identifier,
                    authorization_identifier=patron1.authorization_identifier,
                ),
                dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron2.external_identifier,
                ),
                dict(
                    event_type=NotificationConstants.ACTIVITY_SYNC_TYPE,
                    loans_endpoint="http://localhost/default/loans",
                    external_identifier=patron2.external_identifier,
                ),
            ]

            msg: messaging.Message
            for ix, msg in enumerate(send_all.call_args[0]):
                assert msg[0].data == messages[ix]

            assert send_all.call_count == 1

    def test_holds_notification(self, push_notf_fixture: PushNotificationsFixture):
        db = push_notf_fixture.db
        # Only patron1 will get an identifier
        patron1 = db.patron()
        patron1.authorization_identifier = "auth1"
        patron2 = db.patron()
        DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "test-token-1", patron1
        )
        DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "test-token-2", patron1
        )
        DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_IOS, "test-token-3", patron2
        )

        work1: Work = db.work(with_license_pool=True)
        work2: Work = db.work(with_license_pool=True)
        p1 = work1.active_license_pool()
        p2 = work2.active_license_pool()
        if p1 and p2:  # mypy complains if we don't do this
            hold1, _ = p1.on_hold_to(patron1, position=0)
            hold2, _ = p2.on_hold_to(patron2, position=0)

        with mock.patch(
            "core.util.notifications.PushNotifications.fcm_app"
        ) as fcm_app, mock.patch(
            "core.util.notifications.messaging.send_all"
        ) as send_all:
            PushNotifications.send_holds_notifications([hold1, hold2])

        loans_api = "http://localhost/default/loans"
        assert send_all.call_count == 1
        messages = [
            dict(
                token="test-token-1",
                notification=messaging.Notification(
                    title=f'Your hold on "{work1.title}" is available!',
                ),
                data=dict(
                    title=f'Your hold on "{work1.title}" is available!',
                    event_type=NotificationConstants.HOLD_AVAILABLE_TYPE,
                    loans_endpoint=loans_api,
                    external_identifier=hold1.patron.external_identifier,
                    authorization_identifier=hold1.patron.authorization_identifier,
                    identifier=hold1.license_pool.identifier.identifier,
                    type=hold1.license_pool.identifier.type,
                    library=hold1.patron.library.short_name,
                ),
            ),
            dict(
                token="test-token-2",
                notification=messaging.Notification(
                    title=f'Your hold on "{work1.title}" is available!',
                ),
                data=dict(
                    title=f'Your hold on "{work1.title}" is available!',
                    event_type=NotificationConstants.HOLD_AVAILABLE_TYPE,
                    loans_endpoint=loans_api,
                    external_identifier=hold1.patron.external_identifier,
                    authorization_identifier=hold1.patron.authorization_identifier,
                    identifier=hold1.license_pool.identifier.identifier,
                    type=hold1.license_pool.identifier.type,
                    library=hold1.patron.library.short_name,
                ),
            ),
            dict(
                token="test-token-3",
                notification=messaging.Notification(
                    title=f'Your hold on "{work2.title}" is available!',
                ),
                data=dict(
                    title=f'Your hold on "{work2.title}" is available!',
                    event_type=NotificationConstants.HOLD_AVAILABLE_TYPE,
                    loans_endpoint=loans_api,
                    external_identifier=hold2.patron.external_identifier,
                    identifier=hold2.license_pool.identifier.identifier,
                    type=hold2.license_pool.identifier.type,
                    library=hold2.patron.library.short_name,
                ),
            ),
        ]

        for ix, msg in enumerate(send_all.call_args[0][0]):
            assert_send_message(msg, messages[ix])
