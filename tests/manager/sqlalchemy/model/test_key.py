import functools
import uuid
from datetime import timedelta
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time
from sqlalchemy import delete, select

from palace.manager.sqlalchemy.model.key import Key, KeyType
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.uuid import uuid_encode
from tests.fixtures.database import DatabaseTransactionFixture


class KeyFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db
        self.create_func = MagicMock(return_value="test_key")
        self.create_key = functools.partial(
            Key.create_key,
            self.db.session,
            KeyType.BEARER_TOKEN_SIGNING,
            self.create_func,
        )
        self.get_key = functools.partial(
            Key.get_key, self.db.session, KeyType.BEARER_TOKEN_SIGNING
        )
        self.delete_old_keys = functools.partial(
            Key.delete_old_keys, self.db.session, KeyType.BEARER_TOKEN_SIGNING
        )

        # remove any existing keys before running tests
        self.db.session.execute(delete(Key))


@pytest.fixture
def key_fixture(db: DatabaseTransactionFixture) -> KeyFixture:
    return KeyFixture(db)


class TestKey:
    def test_create_key(
        self, db: DatabaseTransactionFixture, key_fixture: KeyFixture
    ) -> None:
        with freeze_time("2020-01-01 00:00:00"):
            key = key_fixture.create_key()

        assert key.id is not None
        with freeze_time("2020-01-01 00:00:00"):
            assert key.created == utc_now()
        assert key.value == key_fixture.create_func.return_value
        key_fixture.create_func.assert_called_once_with(key.id)

        db.session.expire_all()
        assert key == db.session.execute(select(Key)).scalar_one()

    def test_get_key(self, key_fixture: KeyFixture) -> None:
        key1 = key_fixture.create_key()
        key2 = key_fixture.create_key()

        # If called without a key id, it should return the key that was created last
        assert key_fixture.get_key() == key2

        # If called with a key id, it should return the key with that id
        assert key_fixture.get_key(key1.id) == key1

        # The key id can also be passed in as an encoded uuid string
        assert isinstance(key1.id, uuid.UUID)
        assert key_fixture.get_key(uuid_encode(key1.id)) == key1

        # Or as a UUID hex string
        assert key_fixture.get_key(key1.id.hex) == key1

        # If a key id is not found, it should return None
        assert key_fixture.get_key("0000000000000000000000") is None

        # Unless raise_exception is True
        with pytest.raises(ValueError):
            key_fixture.get_key(
                "0000000000000000000000",
                raise_exception=True,
            )

    def test_create_admin_secret_key(self, db: DatabaseTransactionFixture) -> None:
        key = Key.create_admin_secret_key(db.session)
        assert key.type == KeyType.ADMIN_SECRET_KEY
        assert key.value is not None
        assert len(key.value) == 48

        # If we already have an admin secret key, we should not create a new one
        key2 = Key.create_admin_secret_key(db.session)
        assert key2 == key

    def test_create_bearer_token_signing_key(
        self, db: DatabaseTransactionFixture
    ) -> None:
        key = Key.create_bearer_token_signing_key(db.session)
        assert key.type == KeyType.BEARER_TOKEN_SIGNING
        assert key.value is not None
        assert len(key.value) == 48

        key2 = Key.create_bearer_token_signing_key(db.session)
        assert key2 == key

    def test_delete_old_keys(
        self, db: DatabaseTransactionFixture, key_fixture: KeyFixture
    ) -> None:
        one_day_ago = utc_now() - timedelta(days=1)
        two_days_ago = utc_now() - timedelta(days=2)
        three_days_ago = utc_now() - timedelta(days=3)

        # If there are no keys, nothing should happen
        assert (
            Key.delete_old_keys(
                db.session, KeyType.BEARER_TOKEN_SIGNING, keep=1, older_than=utc_now()
            )
            == 0
        )

        # If keep is negative, we raise an error
        with pytest.raises(ValueError):
            Key.delete_old_keys(
                db.session, KeyType.BEARER_TOKEN_SIGNING, keep=-1, older_than=utc_now()
            )

        # Create some keys
        key1 = key_fixture.create_key()
        key1.created = three_days_ago
        key2 = key_fixture.create_key()
        key2.created = two_days_ago
        key3 = key_fixture.create_key()
        key3.created = one_day_ago

        # We always keep the number of keys specified in the keep parameter, even if they are older than the
        # older_than parameter
        assert key_fixture.delete_old_keys(keep=3, older_than=utc_now()) == 0

        # If all the keys are newer than the older_than parameter, nothing should happen
        assert (
            key_fixture.delete_old_keys(
                keep=0,
                older_than=three_days_ago,
            )
            == 0
        )

        # If we keep 2 keys, the oldest key should be deleted
        assert key_fixture.delete_old_keys(keep=2, older_than=utc_now()) == 1
        assert db.session.execute(
            select(Key).order_by(Key.created)
        ).scalars().all() == [key2, key3]

        # If we keep 1 key, another key should be deleted
        assert key_fixture.delete_old_keys(keep=1, older_than=utc_now()) == 1
        assert db.session.execute(
            select(Key).order_by(Key.created)
        ).scalars().all() == [key3]
