from unittest.mock import MagicMock

import pytest

from palace.manager.api.controller.device_tokens import DeviceTokensController
from palace.manager.api.problem_details import (
    DEVICE_TOKEN_NOT_FOUND,
    DEVICE_TOKEN_TYPE_INVALID,
)
from palace.manager.sqlalchemy.model.devicetokens import DeviceToken, DeviceTokenTypes
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


@pytest.fixture
def controller(db: DatabaseTransactionFixture) -> DeviceTokensController:
    mock_manager = MagicMock()
    mock_manager._db = db.session
    return DeviceTokensController(mock_manager)


class TestDeviceTokens:
    def test_create_invalid_type(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        with flask_app_fixture.test_request_context(
            json={"device_token": "xx", "token_type": "aninvalidtoken"},
            patron=db.patron(),
        ):
            detail = controller.create_patron_device()

        assert detail is DEVICE_TOKEN_TYPE_INVALID
        assert detail.status_code == 400

    def test_create_token(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        with flask_app_fixture.test_request_context(
            json={
                "device_token": "xxx",
                "token_type": DeviceTokenTypes.FCM_ANDROID,
            },
            patron=patron,
        ):
            response = controller.create_patron_device()

        assert response[1] == 201

        devices = (
            db.session.query(DeviceToken)
            .filter(DeviceToken.patron_id == patron.id)
            .all()
        )

        assert len(devices) == 1
        device = devices[0]
        assert device.device_token == "xxx"
        assert device.token_type == DeviceTokenTypes.FCM_ANDROID

    def test_get_token(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        with flask_app_fixture.test_request_context(
            path="/?device_token=xx", patron=patron
        ):
            response = controller.get_patron_device()

        assert response[1] == 200
        assert response[0]["token_type"] == DeviceTokenTypes.FCM_ANDROID
        assert response[0]["device_token"] == "xx"

    def test_get_token_not_found(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        with flask_app_fixture.test_request_context(
            path="/?device_token=xxs", patron=patron
        ):
            detail = controller.get_patron_device()

        assert detail == DEVICE_TOKEN_NOT_FOUND

    def test_get_token_different_patron(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        with flask_app_fixture.test_request_context(
            path="/?device_token=xx", patron=db.patron()
        ):
            detail = controller.get_patron_device()

        assert detail == DEVICE_TOKEN_NOT_FOUND

    def test_create_duplicate_token(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        device = DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxx", patron)
        json_data = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_ANDROID,
        }

        # Same patron same token
        with flask_app_fixture.test_request_context(json=json_data, patron=patron):
            response = controller.create_patron_device()
        assert response == (dict(exists=True), 200)

        # different patron same token
        with flask_app_fixture.test_request_context(json=json_data, patron=db.patron()):
            response = controller.create_patron_device()

        assert response[1] == 201

    def test_delete_token(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        device = DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxx", patron)
        json_data = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_IOS,
        }

        assert db.session.get(DeviceToken, device.id) is not None

        with flask_app_fixture.test_request_context(json=json_data, patron=patron):
            response = controller.delete_patron_device()

        assert response.status_code == 204
        assert db.session.get(DeviceToken, device.id) is None

    def test_delete_no_token(
        self,
        flask_app_fixture: FlaskAppFixture,
        controller: DeviceTokensController,
        db: DatabaseTransactionFixture,
    ):
        patron = db.patron()
        device = DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxx", patron)
        json_data = {
            "device_token": "xxxy",
            "token_type": DeviceTokenTypes.FCM_IOS,
        }

        with flask_app_fixture.test_request_context(json=json_data, patron=patron):
            response = controller.delete_patron_device()
        assert response == DEVICE_TOKEN_NOT_FOUND
