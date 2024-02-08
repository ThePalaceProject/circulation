from datetime import timedelta

from freezegun import freeze_time
from sqlalchemy import delete, select

from api.authentication.access_token import PatronJWEAccessTokenProvider
from core.jobs.rotate_jwe_key import RotateJWEKeyScript
from core.model import Key
from core.model.key import KeyType
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestRotateJWEKeyScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        script = RotateJWEKeyScript(db.session)
        current = PatronJWEAccessTokenProvider.create_key(db.session)
        script.do_run()
        new_key = PatronJWEAccessTokenProvider.get_key(db.session)

        assert current is not None
        assert new_key is not None
        assert current.id != new_key.id

    def test_do_run_no_current_key(self, db: DatabaseTransactionFixture):
        db.session.execute(delete(Key).where(Key.type == KeyType.AUTH_TOKEN_JWE))
        assert Key.get_key(db.session, KeyType.AUTH_TOKEN_JWE) is None
        script = RotateJWEKeyScript(db.session)
        script.do_run()
        created_key = PatronJWEAccessTokenProvider.get_key(db.session)
        assert isinstance(created_key, Key)

    @freeze_time()
    def test_do_run_remove_expired(self, db: DatabaseTransactionFixture):
        db.session.execute(delete(Key).where(Key.type == KeyType.AUTH_TOKEN_JWE))
        script = RotateJWEKeyScript(db.session)

        key1 = PatronJWEAccessTokenProvider.create_key(db.session)
        key1.created = utc_now() - timedelta(days=2, hours=4)
        key2 = PatronJWEAccessTokenProvider.create_key(db.session)
        key2.created = utc_now() - timedelta(days=3)
        key3 = PatronJWEAccessTokenProvider.create_key(db.session)
        key3.created = utc_now() - timedelta(days=4)
        key4 = PatronJWEAccessTokenProvider.create_key(db.session)
        key4.created = utc_now() - timedelta(days=5)
        key5 = PatronJWEAccessTokenProvider.create_key(db.session)
        key5.created = utc_now() - timedelta(days=6)

        script.do_run()

        queried_keys = db.session.scalars(
            select(Key)
            .where(Key.type == KeyType.AUTH_TOKEN_JWE)
            .order_by(Key.created.desc())
        ).all()
        assert len(queried_keys) == 2
        [queried_key_1, queried_key_2] = queried_keys

        # The most recent key is the one that was created by the script
        assert queried_key_1.created == utc_now()
        assert queried_key_1.id != key1.id

        # key1 was kept, even though it's more than two days old, because we always keep
        # two keys, so that tokens created right before the key rotation can still be decrypted
        # until the tokens expire.
        assert queried_key_2.id == key1.id

        # The other keys were deleted
        assert Key.get_key(db.session, KeyType.AUTH_TOKEN_JWE, key_id=key2.id) is None
        assert Key.get_key(db.session, KeyType.AUTH_TOKEN_JWE, key_id=key3.id) is None
        assert Key.get_key(db.session, KeyType.AUTH_TOKEN_JWE, key_id=key4.id) is None
        assert Key.get_key(db.session, KeyType.AUTH_TOKEN_JWE, key_id=key5.id) is None
