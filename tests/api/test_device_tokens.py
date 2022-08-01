from unittest.mock import MagicMock, patch

from api.problem_details import DEVICE_TOKEN_NOT_FOUND, DEVICE_TOKEN_TYPE_INVALID
from core.model.devicetokens import DeviceToken, DeviceTokenTypes
from tests.api.test_controller import ControllerTest


@patch("api.controller.flask")
class TestDeviceTokens(ControllerTest):
    def test_create_invalid_type(self, flask):
        request = MagicMock()
        request.patron = self._patron()
        request.json = {"device_token": "xx", "token_type": "aninvalidtoken"}
        flask.request = request
        detail = self.app.manager.patron_devices.create_patron_device()

        assert detail is DEVICE_TOKEN_TYPE_INVALID
        assert detail.status_code == 400

    def test_create_token(self, flask):
        request = MagicMock()
        request.patron = self._patron()
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_ANDROID,
        }
        flask.request = request
        response = self.app.manager.patron_devices.create_patron_device()

        assert response[1] == 201

        devices = (
            self._db.query(DeviceToken)
            .filter(DeviceToken.patron_id == request.patron.id)
            .all()
        )

        assert len(devices) == 1
        device = devices[0]
        assert device.device_token == "xxx"
        assert device.token_type == DeviceTokenTypes.FCM_ANDROID

    def test_get_token(self, flask):
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        request = MagicMock()
        request.patron = patron
        request.args = {"device_token": "xx"}
        flask.request = request
        response = self.app.manager.patron_devices.get_patron_device()

        assert response[1] == 200
        assert response[0]["token_type"] == DeviceTokenTypes.FCM_ANDROID
        assert response[0]["device_token"] == "xx"

    def test_get_token_not_found(self, flask):
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        request = MagicMock()
        request.patron = patron
        request.args = {"device_token": "xxs"}
        flask.request = request
        detail = self.app.manager.patron_devices.get_patron_device()

        assert detail == DEVICE_TOKEN_NOT_FOUND

    def test_get_token_different_patron(self, flask):
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xx", patron
        )

        request = MagicMock()
        request.patron = self._patron()
        request.args = {"device_token": "xx"}
        flask.request = request
        detail = self.app.manager.patron_devices.get_patron_device()

        assert detail == DEVICE_TOKEN_NOT_FOUND

    def test_create_duplicate_token(self, flask):
        patron = self._patron()
        device = DeviceToken.create(self._db, DeviceTokenTypes.FCM_IOS, "xxx", patron)

        patron1 = self._patron()
        request = MagicMock()
        request.patron = patron1
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_ANDROID,
        }
        flask.request = request
        response = self.app.manager.patron_devices.create_patron_device()

        assert response == (dict(exists=True), 200)

    def test_delete_token(self, flask):
        patron = self._patron()
        device = DeviceToken.create(self._db, DeviceTokenTypes.FCM_IOS, "xxx", patron)

        request = MagicMock()
        request.patron = patron
        request.json = {
            "device_token": "xxx",
            "token_type": DeviceTokenTypes.FCM_IOS,
        }
        flask.request = request

        response = self.app.manager.patron_devices.delete_patron_device()
        self._db.commit()

        assert response.status_code == 204
        assert self._db.query(DeviceToken).get(device.id) == None

    def test_delete_no_token(self, flask):
        patron = self._patron()
        device = DeviceToken.create(self._db, DeviceTokenTypes.FCM_IOS, "xxx", patron)

        request = MagicMock()
        request.patron = patron
        request.json = {
            "device_token": "xxxy",
            "token_type": DeviceTokenTypes.FCM_IOS,
        }
        flask.request = request

        response = self.app.manager.patron_devices.delete_patron_device()
        assert response == DEVICE_TOKEN_NOT_FOUND
