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
    def test_create(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()

        patron = database_transaction.patron()
        device = DeviceToken.create(
            session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        new_device = session.query(DeviceToken).get(device.id)
        assert new_device.device_token == "xxxx"
        assert new_device.token_type == DeviceTokenTypes.FCM_ANDROID

        # assert relationships
        assert new_device.patron == patron
        assert patron.device_tokens == [new_device]

    def test_create_duplicate(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()

        patron = database_transaction.patron()
        device = DeviceToken.create(
            session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        with pytest.raises(DuplicateDeviceTokenError):
            DeviceToken.create(session, DeviceTokenTypes.FCM_IOS, "xxxx", patron)

    def test_duplicate_different_patron(
        self, database_transaction: DatabaseTransactionFixture
    ):
        session = database_transaction.session()

        patron = database_transaction.patron()
        device = DeviceToken.create(
            session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )
        patron1 = database_transaction.patron()
        device = DeviceToken.create(
            session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron1
        )
        assert device.id is not None

    def test_invalid_type(self, database_transaction: DatabaseTransactionFixture):
        session = database_transaction.session()

        patron = database_transaction.patron()
        with pytest.raises(InvalidTokenTypeError):
            DeviceToken.create(session, "invalidtype", "xxxx", patron)

    def test_cascade(self, database_transaction: DatabaseTransactionFixture):
        """Ensure the devicetoken is deleted after a patron is deleted
        Else need to run 20220701-add-devicetoken-cascade.sql"""
        session = database_transaction.session()

        patron = database_transaction.patron()
        device = DeviceToken.create(
            session, DeviceTokenTypes.FCM_ANDROID, "xxxx", patron
        )

        session.delete(patron)
        session.commit()
        with pytest.raises(InvalidRequestError):
            session.refresh(device)
