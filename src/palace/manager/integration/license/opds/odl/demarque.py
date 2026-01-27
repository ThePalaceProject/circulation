"""
DeMarque WebReader integration for ODL streaming fulfillment.

This module provides integration with the DeMarque WebReader
(https://r.cantook.com). When an LSD contains a WebReader link and JWT
configuration is present, a signed JWT is generated and the URL template
is expanded with the token.
"""

from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import cast

from jwcrypto.jwk import JWK, InvalidJWKValue
from jwcrypto.jwt import JWT
from pydantic_settings import SettingsConfigDict

from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.opds.lcp.status import Link as LsdLink
from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)
from palace.manager.util.log import LoggerMixin

# DeMarque WebReader URL pattern used in LSD link relations
DEMARQUE_WEBREADER_REL = "https://r.cantook.com"


class DeMarqueWebReaderConfiguration(ServiceConfiguration, LoggerMixin):
    """
    Configuration for DeMarque WebReader JWT authentication.

    All fields are optional. If any required field is missing, JWT
    authentication will be disabled and the system will fall back
    to regular streaming fulfillment.
    """

    model_config = SettingsConfigDict(
        env_prefix="PALACE_DEMARQUE_WEBREADER_",
    )

    issuer_url: str | None = None
    """JWT issuer URL (must be whitelisted on WebReader)."""

    jwk_file: Path | None = None
    """Path to Ed25519 private key JWK file (JSON format)."""

    jwk: str | None = None
    """Inline Ed25519 private key JWK (JSON string)."""

    # WebReader display options
    language: str = "en"
    """Preferred UI language (BCP-47 format, e.g., 'en', 'fr')."""

    showcase_tts: bool = False
    """Display text-to-speech in primary actions."""

    allow_offline: bool = False
    """Enable offline reading mode."""

    def get_jwk(self) -> JWK | None:
        """
        Load, validate, and return the JWK.

        Prefers inline JWK over file path if both are provided.
        Validates that the JWK is an Ed25519 private key with a kid.

        :return: The validated JWK object, or None if not configured or invalid.
        """
        jwk_content: str | None = None
        jwk_file_resolved = self.jwk_file.resolve() if self.jwk_file else None

        if self.jwk:
            jwk_content = self.jwk
        elif jwk_file_resolved:
            if not jwk_file_resolved.exists():
                self.log.warning(
                    f"JWK file configured but not found: {jwk_file_resolved}"
                )
            else:
                jwk_content = jwk_file_resolved.read_text()
                if not jwk_content:
                    self.log.warning(
                        f"JWK file configured but empty: {jwk_file_resolved}"
                    )

        if jwk_content is None or jwk_content == "":
            return None

        try:
            jwk = JWK.from_json(jwk_content)
        except InvalidJWKValue:
            self.log.exception("Invalid JWK: Failed to parse key.")
            return None

        # Validate key type
        kty = jwk.get("kty")
        if kty != "OKP":
            self.log.error(
                f"Invalid JWK: Expected OKP key type for Ed25519, got kty='{kty}'."
            )
            return None

        # Validate curve
        crv = jwk.get("crv")
        if crv != "Ed25519":
            self.log.error(f"Invalid JWK: Expected Ed25519 curve, got crv='{crv}'.")
            return None

        # Validate kid is present
        kid = jwk.get("kid")
        if not kid:
            self.log.error("Invalid JWK: Missing required 'kid' (key ID) field.")
            return None

        # Validate private key component is present
        if "d" not in jwk:
            self.log.error("Invalid JWK: Missing required 'd' (private key) field.")
            return None

        return jwk


class DeMarqueWebReader:
    """
    Client for DeMarque WebReader authentication and URL generation.

    Handles JWT-based authentication for the DeMarque WebReader at
    https://r.cantook.com, generating Ed25519-signed tokens and
    expanding URL templates.

    Use the :meth:`create` factory method to instantiate. It returns None
    if the required configuration is not present.
    """

    AUDIENCE = "https://r.cantook.com"
    ALGORITHM = "EdDSA"

    def __init__(
        self,
        issuer_url: str,
        jwk_key: JWK,
        language: str,
        showcase_tts: bool,
        allow_offline: bool,
    ) -> None:
        """
        Initialize the WebReader client.

        Use :meth:`create` instead of calling this directly.

        :param issuer_url: JWT issuer URL.
        :param jwk_key: Ed25519 private key as a JWK object.
        :param language: Preferred UI language (BCP-47 format).
        :param showcase_tts: Display text-to-speech in primary actions.
        :param allow_offline: Enable offline reading mode.
        """
        self._issuer_url = issuer_url
        self._jwk_key = jwk_key
        self._key_id: str = cast(str, jwk_key.get("kid"))
        self._language = language
        self._showcase_tts = showcase_tts
        self._allow_offline = allow_offline

    @classmethod
    def create(
        cls, config: DeMarqueWebReaderConfiguration | None = None
    ) -> DeMarqueWebReader | None:
        """
        Create a WebReader client if configuration is complete.

        :param config: Configuration object. If None, loads from environment.
        :return: A configured client, or None if configuration is incomplete.
        """
        config = config or DeMarqueWebReaderConfiguration()

        if config.issuer_url is None:
            return None

        jwk_key = config.get_jwk()
        if jwk_key is None:
            return None

        return cls(
            issuer_url=config.issuer_url,
            jwk_key=jwk_key,
            language=config.language,
            showcase_tts=config.showcase_tts,
            allow_offline=config.allow_offline,
        )

    def generate_token(self, subject: str) -> str:
        """
        Generate a signed JWT for the DeMarque WebReader.

        :param subject: The subject claim (typically the publication identifier).
        :return: The signed JWT string.
        """
        header = {
            "alg": self.ALGORITHM,
            "kid": self._key_id,
        }

        claims = {
            "iss": self._issuer_url,
            "sub": subject,
            "iat": int(time.time()),
            "aud": self.AUDIENCE,
            "jti": str(uuid.uuid4()),
            "language": self._language,
            "showcaseTTS": self._showcase_tts,
            "allowOffline": self._allow_offline,
        }

        token = JWT(header=header, claims=claims)
        token.make_signed_token(self._jwk_key)
        return token.serialize()  # type: ignore[no-any-return]

    def fulfill_link(self, link: LsdLink) -> LsdLink:
        """
        Create a WebReader link suitable for fulfillment by a client.

        Generates a signed JWT and expands the URL template with the token.

        :param link: The WebReader link from the LSD containing the URL template
            and properties.identifier.
        :return: A new link with the authenticated URL.
        :raises CannotFulfill: If the link is missing the required identifier.
        """
        # Extract identifier from link properties
        identifier = link.properties.identifier
        if identifier is None:
            raise CannotFulfill()

        token = self.generate_token(identifier)
        return LsdLink(href=link.href_templated({"token": token}), type=link.type)
