import pytest
from sqlalchemy.exc import InvalidRequestError

from core.model.devicetokens import (
    DeviceToken,
    DeviceTokenTypes,
    DuplicateDeviceTokenError,
    InvalidTokenTypeError,
)
from tests.fixtures.database import DatabaseTransactionFixture


class TestDeviceToken:
    def test_create(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        new_device = db.session.query(DeviceToken).get(device.id)
        assert isinstance(new_device, DeviceToken)
        assert new_device.device_token == "xxxx"
        assert new_device.token_type == DeviceTokenTypes.FCM_ANDROID

        # assert relationships
        assert new_device.patron == patron
        assert patron.device_tokens == [new_device]

    def test_create_duplicate(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        with pytest.raises(DuplicateDeviceTokenError):
            DeviceToken.create(db.session, DeviceTokenTypes.FCM_IOS, "xxxx", patron)

    def test_duplicate_different_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )
        patron1 = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron1
        )
        assert device.id is not None

    def test_invalid_type(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        with pytest.raises(InvalidTokenTypeError):
            DeviceToken.create(db.session, "invalidtype", "xxxx", patron)

    def test_cascade(self, db: DatabaseTransactionFixture):
        """Ensure the devicetoken is deleted after a patron is deleted
        Else need to run 20220701-add-devicetoken-cascade.sql"""
        patron = db.patron()
        device = DeviceToken.create(
            db.session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        db.session.delete(patron)
        db.session.commit()
        with pytest.raises(InvalidRequestError):
            db.session.refresh(device)
