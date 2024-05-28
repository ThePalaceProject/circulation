import logging
import re
from collections.abc import Generator
from datetime import datetime
from typing import Any
from unittest import mock
from unittest.mock import MagicMock

import firebase_admin
import pytest
import pytz
from firebase_admin.exceptions import FirebaseError
from firebase_admin.messaging import UnregisteredError
from google.auth import credentials
from requests_mock import Mocker

from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one, get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.notifications import NotificationType, PushNotifications
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


class PushNotificationsFixture:
    def __init__(self, db: DatabaseTransactionFixture, app: firebase_admin.App) -> None:
        self.db = db
        self.app = app
        self.notifications = PushNotifications(
            base_url="http://localhost", fcm_app=app, testing_mode=True
        )


@pytest.fixture(scope="function")
def push_notf_fixture(
    db: DatabaseTransactionFixture,
) -> Generator[PushNotificationsFixture, None, None]:
    app = firebase_admin.initialize_app(
        MockCredential(), options=dict(projectId="mock-app-1"), name="testapp"
    )
    yield PushNotificationsFixture(db, app)
    firebase_admin.delete_app(app)


class TestPushNotifications:
    def test_send_loan_notification(self, push_notf_fixture: PushNotificationsFixture):
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

        # Test the data structuring down to the "send" method
        # If bad data is detected, the fcm "send" method will error out
        # If not, we are good
        with Mocker() as mocker:
            mocker.post(
                re.compile("https://fcm.googleapis.com"), json=dict(name="mid-mock")
            )
            assert push_notf_fixture.notifications.send_loan_expiry_message(
                loan, 1, [device_token]
            ) == ["mid-mock"]
            assert loan.patron_last_notified == utc_now().date()

        with mock.patch("palace.manager.util.notifications.messaging") as messaging:
            push_notf_fixture.notifications.send_loan_expiry_message(
                loan, 1, [device_token]
            )

            assert messaging.Message.call_count == 1
            assert messaging.Message.call_args_list[0] == [
                (),
                {
                    "token": "atoken",
                    "notification": messaging.Notification(
                        title="Only 1 day left on your loan!",
                        body=f"Your loan on {work.presentation_edition.title} is expiring soon",
                    ),
                    "data": dict(
                        title="Only 1 day left on your loan!",
                        body=f'Your loan for "{work.presentation_edition.title}" at {loan.library.name} is expiring soon',
                        event_type=NotificationType.LOAN_EXPIRY,
                        loans_endpoint="http://localhost/default/loans",
                        external_identifier=patron.external_identifier,
                        authorization_identifier=patron.authorization_identifier,
                        identifier=work.presentation_edition.primary_identifier.identifier,
                        type=work.presentation_edition.primary_identifier.type,
                        library=loan.library.short_name,
                        days_to_expiry="1",
                    ),
                },
            ]
            assert messaging.send.call_count == 1
            assert messaging.send.call_args_list[0] == [
                (messaging.Message(),),
                {"dry_run": True, "app": push_notf_fixture.app},
            ]

    def test_patron_last_notified_updated(
        self, push_notf_fixture: PushNotificationsFixture
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
        work2: Work = db.work(with_license_pool=True)
        hold, _ = work2.active_license_pool().on_hold_to(patron)  # type: ignore

        with mock.patch(
            "palace.manager.util.notifications.PushNotifications.send_messages"
        ) as mock_send, mock.patch(
            "palace.manager.util.notifications.utc_now"
        ) as mock_now:
            # Loan expiry
            # No messages sent
            mock_send.return_value = []
            responses = push_notf_fixture.notifications.send_loan_expiry_message(
                loan, 1, [device_token]
            )
            assert responses == []
            assert loan.patron_last_notified == None

            # One message sent
            mock_now.return_value = datetime(2020, 1, 1, tzinfo=pytz.UTC)
            mock_send.return_value = ["mock-mid"]
            responses = push_notf_fixture.notifications.send_loan_expiry_message(
                loan, 1, [device_token]
            )
            assert responses == ["mock-mid"]
            # last notified gets updated
            assert loan.patron_last_notified == datetime(2020, 1, 1).date()

            # Now hold expiry
            mock_send.return_value = []
            responses = push_notf_fixture.notifications.send_holds_notifications([hold])
            assert responses == []
            assert hold.patron_last_notified == None

            mock_send.return_value = ["mock-mid"]
            responses = push_notf_fixture.notifications.send_holds_notifications([hold])
            assert responses == ["mock-mid"]
            assert hold.patron_last_notified == datetime(2020, 1, 1).date()

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
        assert p1 is not None
        p2 = work2.active_license_pool()
        assert p2 is not None
        hold1, _ = p1.on_hold_to(patron1, position=0)
        hold2, _ = p2.on_hold_to(patron2, position=0)

        with mock.patch(
            "palace.manager.util.notifications.messaging"
        ) as mock_messaging:
            # Mock the notification method to return the kwargs passed to it
            # so that we can make sure we are making the expected calls
            mock_messaging.Notification.side_effect = lambda **kwargs: kwargs
            push_notf_fixture.notifications.send_holds_notifications([hold1, hold2])

        assert (
            hold1.patron_last_notified == hold2.patron_last_notified == utc_now().date()
        )
        loans_api = "http://localhost/default/loans"

        def assert_message_call(
            actual: Any,
            token: str,
            work: Work,
            hold: Hold,
            include_auth_id: bool = True,
        ) -> None:
            data = dict(
                title="Your hold is available!",
                body=f'Your hold on "{work.title}" is available at {hold.library.name}!',
                event_type=NotificationType.HOLD_AVAILABLE,
                loans_endpoint=loans_api,
                identifier=hold.license_pool.identifier.identifier,
                type=hold.license_pool.identifier.type,
                library=hold.patron.library.short_name,
                external_identifier=hold.patron.external_identifier,
            )

            if include_auth_id:
                data["authorization_identifier"] = hold.patron.authorization_identifier

            notification = dict(
                title=data["title"],
                body=data["body"],
            )

            assert actual == mock.call(
                token=token,
                notification=notification,
                data=data,
            )

        # We should have sent 3 messages, one for each token
        assert mock_messaging.Message.call_count == 3

        # We should have sent 2 notifications, one for each patron.
        # Because patron1 has 2 tokens, they will get the same notification for
        # each token.
        assert mock_messaging.Notification.call_count == 2

        [
            message_call1,
            message_call2,
            message_call3,
        ] = mock_messaging.Message.call_args_list
        assert_message_call(message_call1, "test-token-1", work1, hold1)
        assert_message_call(message_call2, "test-token-2", work1, hold1)
        assert_message_call(
            message_call3, "test-token-3", work2, hold2, include_auth_id=False
        )

    def test_holds_notification_errors(
        self,
        push_notf_fixture: PushNotificationsFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        db = push_notf_fixture.db

        # Hold lacking patron
        hold1 = Hold()

        # Hold with patron, but lacking other required data
        hold2 = Hold()
        hold2.patron = db.patron()
        hold2.patron.authorization_identifier = "12345"

        # Complete hold
        work = db.work(with_license_pool=True)
        license_pool = work.active_license_pool()
        assert license_pool is not None
        patron = db.patron()
        DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "test-token-1", patron
        )
        hold3, _ = license_pool.on_hold_to(patron, position=0)

        # mock the send_messages method to avoid actual calls
        mock_send_messages = MagicMock()
        push_notf_fixture.notifications.send_messages = mock_send_messages

        # We should log errors, but continue on and send the successful notifications
        caplog.set_level(logging.INFO)
        push_notf_fixture.notifications.send_holds_notifications([hold1, hold2, hold3])

        error1, error2, success = caplog.records
        assert error1.levelname == error2.levelname == "ERROR"
        assert f"Failed to send notification for hold {hold1.id}" in error1.message
        assert (
            f"Failed to send notification for hold {hold2.id} to patron {hold2.patron.authorization_identifier}"
            in error2.message
        )

        assert success.levelname == "INFO"
        assert "Notifying patron" in success.message

        assert mock_send_messages.call_count == 1

    def test_send_messages(
        self,
        push_notf_fixture: PushNotificationsFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        db = push_notf_fixture.db
        patron1 = db.patron()
        token = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_IOS, "test-token", patron1
        )

        caplog.set_level(logging.WARNING)
        with mock.patch("palace.manager.util.notifications.messaging") as messaging:
            push_notf_fixture.notifications.send_messages(
                [token],
                None,
                dict(test_none=None, test_str="test", test_int=1, test_bool=True),  # type: ignore[dict-item]
            )
            assert messaging.Message.call_count == 1
            assert messaging.Message.call_args.kwargs["data"] == dict(
                test_str="test", test_int="1", test_bool="True"
            )

        assert len(caplog.records) == 3
        assert (
            "Removing test_none from notification data because it is None"
            in caplog.messages
        )
        assert "Converting test_int from <class 'int'> to str" in caplog.messages
        assert "Converting test_bool from <class 'bool'> to str" in caplog.messages

    def test_send_messages_unregistered_error(
        self,
        push_notf_fixture: PushNotificationsFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        db = push_notf_fixture.db
        patron1 = db.patron()
        patron1.authorization_identifier = "auth1"
        token = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_IOS, "test-token", patron1
        )
        caplog.set_level(logging.INFO)
        # When a token causes an UnregisteredError, it should be deleted
        with mock.patch("palace.manager.util.notifications.messaging") as messaging:
            messaging.send.side_effect = UnregisteredError("test")
            push_notf_fixture.notifications.send_messages([token], None, {})
            assert messaging.Message.call_count == 1
            assert messaging.send.call_count == 1

        assert get_one(db.session, DeviceToken, device_token="test-token") is None
        assert (
            "Device token test-token for patron auth1 is no longer registered, deleting"
            in caplog.text
        )

    def test_send_messages_firebase_error(
        self,
        push_notf_fixture: PushNotificationsFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # When a token causes an FirebaseError, we should log it and move on
        mock_token = MagicMock(spec=DeviceToken)
        mock_token.patron.authorization_identifier = "12345"

        caplog.set_level(logging.ERROR)
        with mock.patch("palace.manager.util.notifications.messaging") as messaging:
            messaging.send.side_effect = FirebaseError("", "")
            push_notf_fixture.notifications.send_messages([mock_token], None, {})
            assert messaging.Message.call_count == 1
            assert messaging.send.call_count == 1

        # We logged the error
        assert "Failed to send notification for patron 12345" in caplog.text

        # And the log contains a traceback
        assert "Traceback" in caplog.text
