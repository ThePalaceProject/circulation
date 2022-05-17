import pytest

from core.model.devicetokens import (
    DeviceToken,
    DeviceTokenTypes,
    DuplicateDeviceTokenError,
    InvalidTokenTypeError,
)
from core.testing import DatabaseTest


class TestDeviceToken(DatabaseTest):
    def test_create(self):
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        new_device = self._db.query(DeviceToken).get(device.id)
        assert new_device.device_token == "xxxx"
        assert new_device.token_type == DeviceTokenTypes.FCM_ANDROID

        # assert relationships
        assert new_device.patron == patron
        assert patron.device_tokens == [new_device]

    def test_create_duplicate(self):
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        with pytest.raises(DuplicateDeviceTokenError):
            DeviceToken.create(self._db, DeviceTokenTypes.FCM_IOS, "xxxx", patron)

    def test_invalid_type(self):
        patron = self._patron()
        with pytest.raises(InvalidTokenTypeError):
            DeviceToken.create(self._db, "invalidtype", "xxxx", patron)
