from unittest.mock import MagicMock, patch

import pytest

from api.controller.device_tokens import DeviceTokensController
from api.problem_details import DEVICE_TOKEN_NOT_FOUND, DEVICE_TOKEN_TYPE_INVALID
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> DeviceTokensController:
    mock_manager = MagicMock()
    mock_manager._db = db.session
    return DeviceTokensController(mock_manager)


@patch("api.controller.device_tokens.flask")
class TestDeviceTokens:
    def test_create_invalid_type(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        request = MagicMock()
        request.patron = db.patron()
        request.json = {"device_token": "xx", "token_type": "aninvalidtoken"}
        flask.request = request
        detail = controller.create_patron_device()

        assert detail is DEVICE_TOKEN_TYPE_INVALID
        assert detail.status_code == 400

    def test_create_token(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        request = MagicMock()
        request.patron = db.patron()
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_ANDROID,
        }
        flask.request = request
        response = controller.create_patron_device()

        assert response[1] == 201

        devices = (
            db.session.query(DeviceToken)
            .filter(DeviceToken.patron_id == request.patron.id)
            .all()
        )

        assert len(devices) == 1
        device = devices[0]
        assert device.device_token == "xxx"
        assert device.token_type == DeviceTokenTypes.FCM_ANDROID

    def test_get_token(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        request = MagicMock()
        request.patron = patron
        request.args = {"device_token": "xx"}
        flask.request = request
        response = controller.get_patron_device()

        assert response[1] == 200
        assert response[0]["token_type"] == DeviceTokenTypes.FCM_ANDROID
        assert response[0]["device_token"] == "xx"

    def test_get_token_not_found(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        request = MagicMock()
        request.patron = patron
        request.args = {"device_token": "xxs"}
        flask.request = request
        detail = controller.get_patron_device()

        assert detail == DEVICE_TOKEN_NOT_FOUND

    def test_get_token_different_patron(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        request = MagicMock()
        request.patron = db.patron()
        request.args = {"device_token": "xx"}
        flask.request = request
        detail = controller.get_patron_device()

        assert detail == DEVICE_TOKEN_NOT_FOUND

    def test_create_duplicate_token(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        patron = db.patron()
        device = DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxx", patron)

        # Same patron same token
        request = MagicMock()
        request.patron = patron
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_ANDROID,
        }
        flask.request = request
        nested = db.session.begin_nested()  # rollback only affects device create
        response = controller.create_patron_device()
        assert response == (dict(exists=True), 200)

        # different patron same token
        patron1 = db.patron()
        request = MagicMock()
        request.patron = patron1
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_ANDROID,
        }
        flask.request = request
        response = controller.create_patron_device()

        assert response[1] == 201

    def test_delete_token(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        patron = db.patron()
        device = DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxx", patron)

        request = MagicMock()
        request.patron = patron
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_IOS,
        }
        flask.request = request

        response = controller.delete_patron_device()
        db.session.commit()

        assert response.status_code == 204
        assert db.session.query(DeviceToken).get(device.id) == None

    def test_delete_no_token(
        self, flask, controller: DeviceTokensController, db: DatabaseTransactionFixture
    ):
        patron = db.patron()
        device = DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxx", patron)

        request = MagicMock()
        request.patron = patron
        request.json = {
            "device_token": "xxxy",
            "token_type": DeviceTokenTypes.FCM_IOS,
        }
        flask.request = request

        response = controller.delete_patron_device()
        assert response == DEVICE_TOKEN_NOT_FOUND
