import base64
import functools
import json
import uuid
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from dateutil import tz
from freezegun import freeze_time
from jwcrypto import jwe, jwk
from pytest import LogCaptureFixture
from sqlalchemy import delete

from palace.manager.api.authentication.access_token import PatronJWEAccessTokenProvider
from palace.manager.api.problem_details import (
    PATRON_AUTH_ACCESS_TOKEN_EXPIRED,
    PATRON_AUTH_ACCESS_TOKEN_INVALID,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.key import Key, KeyType
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.problem_detail import ProblemDetailException
from palace.manager.util.uuid import uuid_encode
from tests.fixtures.database import DatabaseTransactionFixture


class JWEProviderFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.patron = db.patron()
        self.generate_token = functools.partial(
            PatronJWEAccessTokenProvider.generate_token,
            self.db.session,
            patron=self.patron,
            password="password",
        )
        self.key = PatronJWEAccessTokenProvider.create_key(self.db.session)
        self.jwk = PatronJWEAccessTokenProvider.get_jwk(self.key)
        assert self.key.id is not None
        self.kid = uuid_encode(self.key.id)
        self.one_hour_in_future = utc_now() + timedelta(hours=1, minutes=1)
        self.one_hour_ago = utc_now() - timedelta(hours=1, minutes=1)

    def create_token(self, plaintext: str = "blah blah", **kwargs: Any) -> str:
        headers: dict[str, Any] = {
            "alg": "dir",
            "enc": "A128CBC-HS256",
        }

        if "kid" not in kwargs:
            headers["kid"] = self.kid
        elif kwargs["kid"] is not None:
            headers["kid"] = kwargs["kid"]

        if "typ" not in kwargs:
            headers["typ"] = "JWE"
        elif kwargs["typ"] is not None:
            headers["typ"] = kwargs["typ"]

        if "cty" not in kwargs:
            headers["cty"] = PatronJWEAccessTokenProvider.CTY
        elif kwargs["cty"] is not None:
            headers["cty"] = kwargs["cty"]

        if "exp" not in kwargs:
            headers["exp"] = self.one_hour_in_future.timestamp()
        elif kwargs["exp"] is not None:
            headers["exp"] = kwargs["exp"]

        token = jwe.JWE(
            plaintext=plaintext,
            protected=headers,
            recipient=self.jwk,
        )
        return token.serialize(compact=True)


@pytest.fixture
def jwe_provider(db: DatabaseTransactionFixture) -> JWEProviderFixture:
    return JWEProviderFixture(db)


class TestJWEProvider:
    def test_generate_jwk(self):
        _id = uuid.uuid4()
        key = PatronJWEAccessTokenProvider.generate_jwk(_id)
        assert isinstance(key, str)
        jwk_key = jwk.JWK.from_json(key)
        assert jwk_key.get("kty") == "oct"
        assert jwk_key.get("kid") == uuid_encode(_id)

    @freeze_time("1990-05-05")
    def test_create_key(self, db: DatabaseTransactionFixture):
        key = PatronJWEAccessTokenProvider.create_key(db.session)
        assert key.created == utc_now()
        assert isinstance(key.id, uuid.UUID)
        jwk_key = PatronJWEAccessTokenProvider.get_jwk(key)
        assert isinstance(jwk_key, jwk.JWK)

    def test_get_key(self, db: DatabaseTransactionFixture, caplog: LogCaptureFixture):
        caplog.set_level(LogLevel.warning)

        # Remove any existing keys before running tests
        db.session.execute(delete(Key).where(Key.type == KeyType.AUTH_TOKEN_JWE))

        # If no key exists, we raise an exception
        with pytest.raises(ValueError):
            PatronJWEAccessTokenProvider.get_key(db.session)

        key = PatronJWEAccessTokenProvider.create_key(db.session)

        # If a key exists, it should return it
        assert PatronJWEAccessTokenProvider.get_key(db.session) == key

        # If a key exists, but it's too old, it should return it and log a warning
        key.created = utc_now() - timedelta(days=3)
        assert PatronJWEAccessTokenProvider.get_key(db.session) == key
        assert (
            "The most recently created AUTH_TOKEN_JWE key is more then two days old"
            in caplog.text
        )

        # If multiple keys exist, it should return the most recent one
        key2 = PatronJWEAccessTokenProvider.create_key(db.session)
        assert PatronJWEAccessTokenProvider.get_key(db.session) == key2

        # If a key id is passed in, it should return that key
        assert PatronJWEAccessTokenProvider.get_key(db.session, key.id) == key

    def test_generate_token(self, jwe_provider: JWEProviderFixture):
        t = utc_now()
        with freeze_time(t):
            token = jwe_provider.generate_token()
        _header, _, _ = token.partition(".")
        header = json.loads(base64.b64decode(_header + "==="))
        assert datetime.fromtimestamp(header["exp"], tz=tz.tzutc()) == t + timedelta(
            hours=1
        )
        assert header["typ"] == "JWE"
        assert header["kid"] == jwe_provider.kid

    def test_decode_token(self, jwe_provider: JWEProviderFixture):
        token = jwe_provider.generate_token()
        decoded = PatronJWEAccessTokenProvider.decode_token(token)
        assert isinstance(decoded, jwe.JWE)
        assert decoded.allowed_algs == ["dir", "A128CBC-HS256"]

    def test_decode_token_errors(self, jwe_provider: JWEProviderFixture):
        # Completely invalid token
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token("not-a-token")
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Expired token
        with freeze_time(jwe_provider.one_hour_ago):
            token = jwe_provider.generate_token()
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_EXPIRED

        # Token with no exp
        token = jwe_provider.create_token(exp=None)
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_EXPIRED

        # Token with no kid
        token = jwe_provider.create_token(kid=None)
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Token with no typ
        token = jwe_provider.create_token(typ=None)
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Token with wrong typ
        token = jwe_provider.create_token(typ="foo")
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Token with no cty
        token = jwe_provider.create_token(cty=None)
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Token with wrong cty
        token = jwe_provider.create_token(cty="foo")
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decode_token(token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

    def test_decrypt_token(
        self, db: DatabaseTransactionFixture, jwe_provider: JWEProviderFixture
    ):
        token = jwe_provider.generate_token()
        decoded = PatronJWEAccessTokenProvider.decode_token(token)
        decrypted = PatronJWEAccessTokenProvider.decrypt_token(db.session, decoded)
        assert decrypted.id == jwe_provider.patron.id
        assert decrypted.pwd == "password"

        # Decrypt can also directly take a token string
        decrypted = PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert decrypted.id == jwe_provider.patron.id
        assert decrypted.pwd == "password"

    def test_decrypt_token_bad_key(
        self, db: DatabaseTransactionFixture, jwe_provider: JWEProviderFixture
    ):
        token = jwe_provider.generate_token()
        decoded = PatronJWEAccessTokenProvider.decode_token(token)

        # No key
        db.session.delete(jwe_provider.key)
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, decoded)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

    def test_decrypt_token_errors(
        self, db: DatabaseTransactionFixture, jwe_provider: JWEProviderFixture
    ):
        # Bad kid
        token = jwe_provider.create_token(kid="fake")
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid token - Bad tag
        token = jwe_provider.create_token() + "B"
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid token - Bad enc type
        token = jwe_provider.create_token(enc="A256GCM")
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid payload - not json
        token = jwe_provider.create_token(plaintext="not-json")
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid payload - missing keys
        token = jwe_provider.create_token(plaintext="{}")
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid payload - missing id
        token = jwe_provider.create_token(plaintext=json.dumps({"pwd": "password"}))
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid payload - missing pwd
        token = jwe_provider.create_token(plaintext=json.dumps({"id": "1234"}))
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

        # Invalid payload - extra keys
        token = jwe_provider.create_token(
            plaintext=json.dumps({"id": "1234", "pwd": "password", "extra": "key"})
        )
        with pytest.raises(ProblemDetailException) as exc:
            PatronJWEAccessTokenProvider.decrypt_token(db.session, token)
        assert exc.value.problem_detail == PATRON_AUTH_ACCESS_TOKEN_INVALID

    @freeze_time()
    def test_delete_old_keys(self):
        mock_session = MagicMock()
        with patch("palace.manager.api.authentication.access_token.Key") as mock_key:
            PatronJWEAccessTokenProvider.delete_old_keys(mock_session)

        mock_key.delete_old_keys.assert_called_once_with(
            mock_session,
            KeyType.AUTH_TOKEN_JWE,
            keep=2,
            older_than=utc_now() - timedelta(days=2),
        )
