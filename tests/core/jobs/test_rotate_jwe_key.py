from api.authenticator import PatronJWEAccessTokenProvider
from core.jobs.rotate_jwe_key import RotateJWEKeyScript
from tests.fixtures.database import DatabaseTransactionFixture


class TestRotateJWEKeyScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        script = RotateJWEKeyScript(db.session)
        current = PatronJWEAccessTokenProvider.get_current_key(db.session)
        script.do_run()
        new_key = PatronJWEAccessTokenProvider.get_current_key(db.session)

        assert current.key_id != new_key.key_id
        assert current.thumbprint() != new_key.thumbprint()
