import base64
import json
from datetime import datetime, timedelta

import pytest
from dateutil import tz
from freezegun import freeze_time
from jwcrypto import jwk

from api.authenticator import PatronData, PatronJWEAccessTokenProvider
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE
from api.sirsidynix_authentication_provider import SirsiDynixPatronData
from core.util.datetime_helpers import utc_now
from core.util.problem_detail import ProblemError
from tests.fixtures.database import DatabaseTransactionFixture


class TestJWEProvider:
    def test_generate_key(self):
        key = PatronJWEAccessTokenProvider.generate_key()
        assert type(key) == jwk.JWK
        assert key.key_type == "oct"

    def test_generate_token(self, db: DatabaseTransactionFixture):
        patron = db.patron()

        with pytest.raises(ProblemError) as raised:
            PatronJWEAccessTokenProvider.generate_token(db.session, patron, "")
            assert raised.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_NOT_POSSIBLE

        patron.patrondata = PatronData()
        t = utc_now()
        with freeze_time(t):
            token = PatronJWEAccessTokenProvider.generate_token(
                db.session, patron, "password"
            )
        header, _, _ = token.partition(".")
        header = json.loads(base64.b64decode(header + "==="))
        assert datetime.fromtimestamp(header["exp"], tz=tz.tzutc()) == t + timedelta(
            hours=1
        )
        assert header["typ"] == "JWE"
        assert (
            header["kid"]
            == PatronJWEAccessTokenProvider.get_current_key(db.session).key_id
        )

    def test_get_current_key(self, db: DatabaseTransactionFixture):
        key1 = PatronJWEAccessTokenProvider.get_current_key(db.session)
        key2 = PatronJWEAccessTokenProvider.get_current_key(db.session)
        assert key1 == key2

        with pytest.raises(ValueError):
            PatronJWEAccessTokenProvider.get_current_key(db.session, kid="not-the-kid")

        assert (
            PatronJWEAccessTokenProvider.get_current_key(db.session, kid=key1.key_id)
            == key1
        )

    def test_decode_token(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        patron.patrondata = PatronData()

        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password"
        )
        decoded = PatronJWEAccessTokenProvider.decode_token(db.session, token)
        assert decoded["id"] == patron.id
        assert decoded["pwd"] == "password"
        assert decoded["typ"] == "patron"

        # Additional changes for sirsidynix type patrondata
        patron.patrondata = SirsiDynixPatronData(session_token="session-token")
        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password"
        )
        decoded = PatronJWEAccessTokenProvider.decode_token(db.session, token)
        assert decoded["id"] == patron.id
        assert decoded["pwd"] == "password"
        assert decoded["typ"] == "sirsi"
        assert decoded["session_token"] == "session-token"

    def test_rotate_key(self, db: DatabaseTransactionFixture):
        key = PatronJWEAccessTokenProvider.rotate_key(db.session)
        integration = PatronJWEAccessTokenProvider.get_integration(db.session)
        assert (
            integration.setting(PatronJWEAccessTokenProvider.PATRON_AUTH_JWE_KEY).value
            == key.export()
        )

        key2 = PatronJWEAccessTokenProvider.rotate_key(db.session)
        assert key2.key_id != key.key_id
        assert key2.thumbprint() != key.thumbprint()
        assert (
            integration.setting(PatronJWEAccessTokenProvider.PATRON_AUTH_JWE_KEY).value
            == key2.export()
        )
