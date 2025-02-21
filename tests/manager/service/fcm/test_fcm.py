import json
import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import firebase_admin
import pytest
from firebase_admin.exceptions import FirebaseError
from firebase_admin.messaging import UnregisteredError
from requests_mock import Mocker

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.fcm import fcm
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from palace.manager.sqlalchemy.util import get_one
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import FilesFixture


@pytest.fixture()
def fcm_files_fixture() -> FilesFixture:
    """Provides access to fcm test files."""
    return FilesFixture("service/fcm")


def test_fcm_credentials(fcm_files_fixture: FilesFixture):
    invalid_json = "{ this is invalid JSON }"
    valid_credentials_json = fcm_files_fixture.sample_text(
        "fcm-credentials-valid-json.json"
    )
    valid_credentials_object = json.loads(valid_credentials_json)

    # No FCM credentials set
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"FCM Credentials configuration environment variable not defined.",
    ):
        fcm.credentials(None, None)

    # Non-existent file.
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"The FCM credentials file .* does not exist.",
    ):
        fcm.credentials(Path("filedoesnotexist.deleteifitdoes"), None)

    # Invalid JSON file.
    fcm_file = Path(fcm_files_fixture.sample_path("not-valid-json.txt"))
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"Cannot parse contents of FCM credentials file .* as JSON.",
    ):
        fcm.credentials(fcm_file, None)

    # Valid JSON file.
    fcm_file = Path(fcm_files_fixture.sample_path("fcm-credentials-valid-json.json"))
    assert valid_credentials_object == fcm.credentials(fcm_file, None)

    # Setting more than one FCM credentials environment variable is not valid.
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"Both JSON .* and file-based .* FCM Credential environment variables are defined, but only one is allowed.",
    ):
        fcm.credentials(fcm_file, valid_credentials_json)

    # Down to just the JSON FCM credentials environment variable.
    assert valid_credentials_object == fcm.credentials(None, valid_credentials_json)

    # But we should get an exception if the JSON is invalid.
    with pytest.raises(
        CannotLoadConfiguration,
        match=r"Cannot parse value of FCM credential environment variable .* as JSON.",
    ):
        fcm.credentials(None, invalid_json)


class TestSendNotifications:

    def test_send(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        patron1 = db.patron()
        token = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_IOS, "test-token", patron1
        )

        caplog.set_level(LogLevel.warning)
        with patch.object(fcm, "messaging") as mock_messaging:
            fcm.send_notifications(
                [token],
                "test title",
                "test body",
                dict(test_none=None, test_str="test", test_int=1, test_bool=True),  # type: ignore[dict-item]
                app=MagicMock(),
            )
            assert mock_messaging.Message.call_count == 1
            assert mock_messaging.Message.call_args.kwargs["data"] == dict(
                test_str="test",
                test_int="1",
                test_bool="True",
                title="test title",
                body="test body",
            )

        assert len(caplog.records) == 3
        assert (
            "Removing test_none from notification data because it is None"
            in caplog.messages
        )
        assert "Converting test_int from <class 'int'> to str" in caplog.messages
        assert "Converting test_bool from <class 'bool'> to str" in caplog.messages

    def test_unregistered_error(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        patron1 = db.patron()
        patron1.authorization_identifier = "auth1"
        token = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_IOS, "test-token", patron1
        )
        caplog.set_level(LogLevel.info)
        # When a token causes an UnregisteredError, it should be deleted
        with patch.object(fcm, "messaging") as mock_messaging:
            mock_messaging.send.side_effect = UnregisteredError("test")
            fcm.send_notifications([token], "title", "body", {}, app=MagicMock())
            assert mock_messaging.Message.call_count == 1
            assert mock_messaging.send.call_count == 1

        assert get_one(db.session, DeviceToken, device_token="test-token") is None
        assert (
            "Device token test-token for patron auth1 is no longer registered, deleting"
            in caplog.text
        )

    def test_firebase_error(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # When a token causes an FirebaseError, we should log it and move on
        mock_token = MagicMock(spec=DeviceToken)
        mock_token.patron.authorization_identifier = "12345"

        caplog.set_level(LogLevel.error)
        with patch.object(fcm, "messaging") as mock_messaging:
            mock_messaging.send.side_effect = FirebaseError("", "")
            fcm.send_notifications([mock_token], "title", "body", {}, app=MagicMock())
            assert mock_messaging.Message.call_count == 1
            assert mock_messaging.send.call_count == 1

        # We logged the error
        assert "Failed to send notification for patron 12345" in caplog.text

        # And the log contains a traceback
        assert "Traceback" in caplog.text

    def test_data(self, requests_mock: Mocker) -> None:
        token = MagicMock(spec=DeviceToken)
        token.device_token = "test-token"

        app = firebase_admin.initialize_app(
            MagicMock(spec=firebase_admin.credentials.Base),
            options=dict(projectId="mock-app-1"),
        )

        # Test the data structuring down to the "send" method
        # If bad data is detected, the fcm "send" method will
        # raise an error.
        requests_mock.post(
            re.compile("https://fcm.googleapis.com"), json=dict(name="mid-mock")
        )

        assert fcm.send_notifications(
            [token],
            "test title",
            "test body",
            dict(test_none=None, test_str="test", test_int=1, test_bool=True),  # type: ignore[dict-item]
            app=app,
        ) == ["mid-mock"]

        firebase_admin.delete_app(app)
