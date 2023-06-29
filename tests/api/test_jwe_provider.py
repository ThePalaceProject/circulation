import base64
import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from dateutil import tz
from freezegun import freeze_time
from jwcrypto import jwk

from api.authentication.access_token import PatronJWEAccessTokenProvider
from api.problem_details import PATRON_AUTH_ACCESS_TOKEN_EXPIRED
from core.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture


class TestJWEProvider:
    def test_generate_key(self):
        key = PatronJWEAccessTokenProvider.generate_key()
        assert type(key) == jwk.JWK
        assert key.get("kty") == "oct"

    def test_generate_token(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        t = utc_now()
        with freeze_time(t):
            token = PatronJWEAccessTokenProvider.generate_token(
                db.session, patron, "password"
            )
        _header, _, _ = token.partition(".")
        header = json.loads(base64.b64decode(_header + "==="))
        assert datetime.fromtimestamp(header["exp"], tz=tz.tzutc()) == t + timedelta(
            hours=1
        )
        assert header["typ"] == "JWE"

        current_key = PatronJWEAccessTokenProvider.get_current_key(db.session)
        assert isinstance(current_key, jwk.JWK)
        assert header["kid"] == current_key.get("kid")

    def test_get_current_key(self, db: DatabaseTransactionFixture):
        key1 = PatronJWEAccessTokenProvider.get_current_key(db.session)
        key2 = PatronJWEAccessTokenProvider.get_current_key(db.session)
        assert key1 == key2

        with pytest.raises(ValueError):
            PatronJWEAccessTokenProvider.get_current_key(db.session, kid="not-the-kid")

        assert isinstance(key1, jwk.JWK)
        assert (
            PatronJWEAccessTokenProvider.get_current_key(
                db.session, kid=key1.get("kid")
            )
            == key1
        )

    def test_decode_token(self, db: DatabaseTransactionFixture):
        patron = db.patron()
        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password"
        )
        decoded = PatronJWEAccessTokenProvider.decode_token(db.session, token)
        assert isinstance(decoded, dict)
        assert decoded["id"] == patron.id
        assert decoded["pwd"] == "password"
        assert decoded["typ"] == "patron"

    def test_decode_token_errors(self, db: DatabaseTransactionFixture):
        patron = db.patron()

        with patch.object(PatronJWEAccessTokenProvider, "get_current_key") as mock_key:
            mock_key.return_value = jwk.JWK.generate(
                kty="oct", size=256, kid="some-kid"
            )
            token = PatronJWEAccessTokenProvider.generate_token(
                db.session, patron, "password", expires_in=1000
            )
        decoded = PatronJWEAccessTokenProvider.decode_token(db.session, token)
        assert decoded == PATRON_AUTH_ACCESS_TOKEN_EXPIRED

        token = PatronJWEAccessTokenProvider.generate_token(
            db.session, patron, "password", expires_in=-1
        )
        decoded = PatronJWEAccessTokenProvider.decode_token(db.session, token)
        assert decoded == PATRON_AUTH_ACCESS_TOKEN_EXPIRED

    def test_rotate_key(self, db: DatabaseTransactionFixture):
        key = PatronJWEAccessTokenProvider.rotate_key(db.session)
        stored_key = PatronJWEAccessTokenProvider.get_current_key(
            db.session, create=False
        )
        assert stored_key == key

        key2 = PatronJWEAccessTokenProvider.rotate_key(db.session)
        stored_key = PatronJWEAccessTokenProvider.get_current_key(
            db.session, create=False
        )
        assert key2.get("kid") != key.get("kid")
        assert key2.thumbprint() != key.thumbprint()
        assert stored_key == key2

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
