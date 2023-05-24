from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Optional, Type, cast

from jwcrypto import jwe, jwk

from api.authentication.base import AuthProviderSettings
from api.problem_details import (
    PATRON_AUTH_ACCESS_TOKEN_EXPIRED,
    PATRON_AUTH_ACCESS_TOKEN_INVALID,
)
from core.integration.goals import Goals
from core.integration.settings import ConfigurationFormItem, FormField
from core.model import get_one_or_create
from core.model.configuration import ExternalIntegration
from core.model.integration import IntegrationConfiguration
from core.model.patron import Patron
from core.util.datetime_helpers import utc_now
from core.util.problem_detail import ProblemDetail, ProblemError
from core.util.string_helpers import random_string


class PatronAccessTokenProvider:
    """Provides access tokens for patron auth"""

    @classmethod
    def generate_token(
        cls, _db, patron: Patron, password: str, expires_in: int = 3600
    ) -> str:
        raise NotImplementedError()

    @classmethod
    def decode_token(cls, _db, token: str) -> dict | ProblemDetail:
        raise NotImplementedError()

    @classmethod
    def is_access_token(cls, token: str | None) -> bool:
        raise NotImplementedError()

    @classmethod
    def get_integration(cls, _db):
        raise NotImplementedError()


class PatronAccessTokenAuthenticationSettings(AuthProviderSettings):
    auth_key: Optional[str] = FormField(
        None,
        description="The JWE key used for the access token",
        form=ConfigurationFormItem("Auth Key"),
    )


class PatronJWEAccessTokenProvider(PatronAccessTokenProvider):
    """Provide JWE based access tokens for patron auth"""

    NAME = "Patron Access Token Provider"

    @classmethod
    def generate_key(cls) -> jwk.JWK:
        """Generate a new key compatible with the token encyption type"""
        kid = random_string(16)
        return jwk.JWK.generate(kty="oct", size=256, kid=kid)

    @classmethod
    def get_integration(cls, _db) -> IntegrationConfiguration:
        integration, _ = get_one_or_create(
            _db,
            IntegrationConfiguration,
            protocol=ExternalIntegration.PATRON_AUTH_JWE,
            goal=Goals.PATRON_AUTH_GOAL,
            name=cls.NAME,
        )
        return integration

    @classmethod
    def rotate_key(cls, _db) -> jwk.JWK:
        """Rotate the current JWK key in the DB"""
        key = cls.generate_key()
        integration = cls.get_integration(_db)

        # We must make a copy() because sqlalchemy does not think
        # a change in the same dict is a change at all.
        # So the dict must be a new dict to register the change.
        # https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#sqlalchemy.dialects.postgresql.JSONB
        settings_dict: dict = cast(dict, integration.settings.copy())
        settings_dict["auth_key"] = key.export()
        integration.settings = settings_dict
        return key

    @classmethod
    def get_current_key(cls, _db, kid=None, create=True) -> jwk.JWK:
        """Get the current JWK key for the CM
        :param kid: (Optional) If present, compare this value to the currently active kid,
                    raise a ValueError if found to be different
        """
        integration = cls.get_integration(_db)

        settings = PatronAccessTokenAuthenticationSettings(
            **cast(dict, integration.settings)
        )
        key: str | None = settings.auth_key
        # First time run, we don't have a value yet
        if key is None:
            if create:
                jwk_key = cls.rotate_key(_db)
            else:
                return None
        else:
            jwk_key = jwk.JWK.from_json(key)

        if kid is not None and kid != jwk_key.key_id:
            raise ValueError(
                "Current KID has changed, the key has probably been rotated"
            )

        return jwk_key

    @classmethod
    def generate_token(
        cls, _db, patron: Patron, password: str, expires_in: int = 3600
    ) -> str:
        """Generate a JWE token for a patron
        :param patron: Generate a token for this patron
        :param password: Encrypt this password within the token
        :param expires_in: Seconds after which this token will expire
        :return: A compacted JWE token
        """
        key = cls.get_current_key(_db)
        payload = dict(id=patron.id, pwd=password, typ="patron")

        token = jwe.JWE(
            jwe.json_encode(payload),
            dict(
                alg="dir",
                kid=key.key_id,
                typ="JWE",
                enc="A128CBC-HS256",
                exp=(utc_now() + timedelta(seconds=expires_in)).timestamp(),
            ),
            recipient=key,
        )
        return token.serialize(compact=True)

    @classmethod
    def decode_token(cls, _db, token: str) -> dict | ProblemDetail:
        """Decode the given token
        :param token: A serialized JWE token
        :return: The decrypted data dictionary from the token
        """
        jwe_token = cls._decode(token)

        # Check expiry
        exp = jwe.json_decode(jwe_token.objects["protected"])["exp"]
        if time.time() > exp:
            return PATRON_AUTH_ACCESS_TOKEN_EXPIRED

        try:
            key = cls.get_current_key(_db, jwe_token.jose_header.get("kid"))
        except ValueError:
            # The kid was incorrect, the key has probably rotated
            return PATRON_AUTH_ACCESS_TOKEN_EXPIRED

        try:
            jwe_token.decrypt(key)
        except jwe.InvalidJWEData:
            return PATRON_AUTH_ACCESS_TOKEN_INVALID

        return jwe.json_decode(jwe_token.payload)

    @classmethod
    def _decode(cls, token: str) -> jwe.JWE:
        """Decode a JWE token without decryption"""
        try:
            jwe_token = jwe.JWE.from_jose_token(token)
        except jwe.InvalidJWEData as ex:
            logging.getLogger(cls.__name__).error(
                f"Invalid JWE data was encountered: {ex}"
            )
            raise ProblemError(PATRON_AUTH_ACCESS_TOKEN_INVALID)
        return jwe_token

    @classmethod
    def is_access_token(cls, token: str | None) -> bool:
        """Test if the given token is a valid JWE token"""
        try:
            jwe_token = cls._decode(token) if token else None
        except Exception:
            return False

        if jwe_token is None:
            return False
        if jwe.json_decode(jwe_token.objects["protected"])["typ"] != "JWE":
            return False

        return True


AccessTokenProvider: Type[PatronAccessTokenProvider] = PatronJWEAccessTokenProvider
