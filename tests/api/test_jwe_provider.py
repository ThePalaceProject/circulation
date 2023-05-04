import base64
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from dateutil import tz
from freezegun import freeze_time
from jwcrypto import jwk

from api.authentication.access_token import PatronJWEAccessTokenProvider
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestJWEProvider:
    def test_generate_key(self):
        key = PatronJWEAccessTokenProvider.generate_key()
        assert type(key) == jwk.JWK
        assert key.key_type == "oct"

    def test_generate_token(self, db: DatabaseTransactionFixture):
        patron = db.patron()
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
        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password"
        )
        decoded = PatronJWEAccessTokenProvider.decode_token(db.session, token)
        assert decoded["id"] == patron.id
        assert decoded["pwd"] == "password"
        assert decoded["typ"] == "patron"

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

    def test_is_access_token(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        # Happy path
        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password"
        )
        assert PatronJWEAccessTokenProvider.is_access_token(token) == True

        with patch.object(PatronJWEAccessTokenProvider, "_decode") as decode:
            # An incorrect type
            decode.return_value = MagicMock(
                objects=dict(protected=json.dumps(dict(typ="NotJWE")))
            )
            assert PatronJWEAccessTokenProvider.is_access_token(token) == False

            # Something failed during the decode
            decode.return_value = None
            assert PatronJWEAccessTokenProvider.is_access_token(token) == False

        # The token is not the right format
        assert PatronJWEAccessTokenProvider.is_access_token("not-a-token") == False
