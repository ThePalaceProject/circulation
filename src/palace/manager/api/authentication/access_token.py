from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING

from jwcrypto import jwe, jwk

from palace.manager.api.problem_details import (
    PATRON_AUTH_ACCESS_TOKEN_EXPIRED,
    PATRON_AUTH_ACCESS_TOKEN_INVALID,
)
from palace.manager.sqlalchemy.model.key import Key, KeyType
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetailException
from palace.manager.util.uuid import uuid_encode

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@dataclass
class TokenPatronInfo:
    id: int
    pwd: str


class PatronJWEAccessTokenProvider(LoggerMixin):
    """Provide JWE based access tokens for patron auth"""

    CTY = "pv1"

    @classmethod
    def generate_jwk(cls, key_id: uuid.UUID) -> str:
        """Generate a new key compatible with the token encyption type"""
        kid = uuid_encode(key_id)
        generated_key = jwk.JWK.generate(kty="oct", size=256, kid=kid)
        return generated_key.export()  # type: ignore[no-any-return]

    @classmethod
    def create_key(cls, _db: Session) -> Key:
        """Create a new key in the DB"""
        key = Key.create_key(_db, KeyType.AUTH_TOKEN_JWE, cls.generate_jwk)
        return key

    @classmethod
    def get_jwk(cls, key: Key) -> jwk.JWK:
        """Get a JWK key from the DB"""
        jwk_obj = jwk.JWK.from_json(key.value)
        return jwk_obj

    @classmethod
    def get_key(cls, _db: Session, key_id: str | uuid.UUID | None = None) -> Key:
        """Get the most recently created AUTH_TOKEN_JWE key from the DB"""
        key = Key.get_key(
            _db, KeyType.AUTH_TOKEN_JWE, key_id=key_id, raise_exception=True
        )
        if (
            key_id is None
            and key.created is not None
            and key.created < utc_now() - timedelta(days=2)
        ):
            cls.logger().warning(
                "The most recently created AUTH_TOKEN_JWE key is more then two days old. "
                "This may indicate a problem with the key rotation."
            )
        return key

    @classmethod
    def generate_token(
        cls, _db: Session, patron: Patron, password: str, expires_in: int = 3600
    ) -> str:
        """Generate a JWE token for a patron"""
        key = cls.get_key(_db)
        jwk_obj = cls.get_jwk(key)
        token = jwe.JWE(
            plaintext=jwe.json_encode(dict(id=patron.id, pwd=password)),
            protected=dict(
                alg="dir",
                kid=uuid_encode(key.id),
                typ="JWE",
                enc="A128CBC-HS256",
                cty=cls.CTY,
                exp=(utc_now() + timedelta(seconds=expires_in)).timestamp(),
            ),
            recipient=jwk_obj,
        )
        return token.serialize(compact=True)  # type: ignore[no-any-return]

    @classmethod
    def decode_token(cls, token: str) -> jwe.JWE:
        """Decode the given token
        :param token: A serialized JWE token
        :return: The decrypted data dictionary from the token
        """
        jwe_token = jwe.JWE()

        # Set the allowed algorithms
        jwe_token.allowed_algs = ["dir", "A128CBC-HS256"]

        try:
            jwe_token.deserialize(token)
        except jwe.InvalidJWEData as ex:
            cls.logger().exception(f"Invalid JWE data was encountered: {ex}")
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        # Check expiry
        exp = jwe_token.jose_header.get("exp")
        if exp is None or utc_now().timestamp() > exp:
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_EXPIRED
            )

        # Make sure there is a kid
        kid = jwe_token.jose_header.get("kid")
        if kid is None:
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        # Make sure we have the token type
        typ = jwe_token.jose_header.get("typ")
        if typ != "JWE":
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        # Make sure we have the payload type
        cty = jwe_token.jose_header.get("cty")
        if cty != cls.CTY:
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        return jwe_token

    @classmethod
    def decrypt_token(cls, _db: Session, token: jwe.JWE | str) -> TokenPatronInfo:
        if isinstance(token, str):
            token = cls.decode_token(token)

        kid = token.jose_header.get("kid")
        try:
            key = cls.get_key(_db, kid)
        except ValueError:
            key = None

        if key is None:
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        try:
            token.decrypt(cls.get_jwk(key))
        except jwe.InvalidJWEData:
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        try:
            payload = jwe.json_decode(token.payload)
        except ValueError:
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        # Validate the payload
        if (
            not isinstance(payload, dict)
            or "id" not in payload
            or "pwd" not in payload
            or len(payload) != 2
        ):
            raise ProblemDetailException(
                problem_detail=PATRON_AUTH_ACCESS_TOKEN_INVALID
            )

        return TokenPatronInfo(**payload)

    @classmethod
    def delete_old_keys(cls, _db: Session) -> int:
        """Delete old keys from the DB

        We keep the two most recent keys in the DB. And delete any keys with a created date older than
        two days.
        """
        two_days_ago = utc_now() - timedelta(days=2)
        return Key.delete_old_keys(
            _db, KeyType.AUTH_TOKEN_JWE, keep=2, older_than=two_days_ago
        )
