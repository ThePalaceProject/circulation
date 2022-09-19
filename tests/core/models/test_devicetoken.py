import pytest
from sqlalchemy.exc import InvalidRequestError

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

    def test_duplicate_different_patron(self):
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )
        patron1 = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron1
        )
        assert device.id is not None

    def test_invalid_type(self):
        patron = self._patron()
        with pytest.raises(InvalidTokenTypeError):
            DeviceToken.create(self._db, "invalidtype", "xxxx", patron)

    def test_cascade(self):
        """Ensure the devicetoken is deleted after a patron is deleted
        Else need to run 20220701-add-devicetoken-cascade.sql"""
        patron = self._patron()
        device = DeviceToken.create(
            self._db, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        self._db.delete(patron)
        self._db.commit()
        with pytest.raises(InvalidRequestError):
            self._db.refresh(device)
