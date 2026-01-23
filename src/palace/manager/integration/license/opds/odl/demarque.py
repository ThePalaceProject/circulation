"""
DeMarque WebReader integration for ODL streaming fulfillment.

This module provides integration with the DeMarque WebReader
(https://r.cantook.com). When an LSD contains a WebReader link and JWT
configuration is present, a signed JWT is generated and the URL template
is expanded with the token.
"""

from __future__ import annotations

import time
from pathlib import Path

import jwt
from pydantic_settings import SettingsConfigDict

from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.opds.lcp.status import Link as LsdLink
from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)

# DeMarque WebReader URL pattern used in LSD link relations
DEMARQUE_WEBREADER_REL = "https://r.cantook.com"


class DeMarqueWebReaderConfiguration(ServiceConfiguration):
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

    key_id: str | None = None
    """Key ID matching registered public key on WebReader."""

    private_key_file: Path | None = None
    """Path to Ed25519 private key PEM file."""

    private_key: str | None = None
    """Inline Ed25519 private key PEM."""

    def get_private_key(self) -> str | None:
        """
        Load and return the private key content.

        Prefers inline key over file path if both are provided.

        :return: The private key PEM content, or None if not configured.
        """
        if self.private_key:
            return self.private_key

        if self.private_key_file:
            if self.private_key_file.exists():
                return self.private_key_file.read_text()

        return None


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
        key_id: str,
        private_key: str,
    ) -> None:
        """
        Initialize the WebReader client.

        Use :meth:`create` instead of calling this directly.

        :param issuer_url: JWT issuer URL.
        :param key_id: Key ID for the JWT header.
        :param private_key: Ed25519 private key in PEM format.
        """
        self._issuer_url = issuer_url
        self._key_id = key_id
        self._private_key = private_key

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

        if config.issuer_url is None or config.key_id is None:
            return None

        private_key = config.get_private_key()
        if private_key is None:
            return None

        return cls(
            issuer_url=config.issuer_url,
            key_id=config.key_id,
            private_key=private_key,
        )

    def generate_token(self, subject: str) -> str:
        """
        Generate a signed JWT for the DeMarque WebReader.

        :param subject: The subject claim (typically the publication identifier).
        :return: The signed JWT string.
        """
        headers = {
            "alg": self.ALGORITHM,
            "kid": self._key_id,
        }

        payload = {
            "iss": self._issuer_url,
            "sub": subject,
            "iat": int(time.time()),
            "aud": self.AUDIENCE,
        }

        return jwt.encode(
            payload,
            self._private_key,
            algorithm=self.ALGORITHM,
            headers=headers,
        )

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
