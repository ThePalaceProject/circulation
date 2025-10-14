"""SAML Service Provider configuration."""

from __future__ import annotations

from pathlib import Path

from pydantic import field_validator
from pydantic_settings import SettingsConfigDict

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class SamlServiceProviderConfiguration(ServiceConfiguration):
    """SAML Service Provider configuration loaded from environment variables.

    Supports both file-based and inline content configuration for SP private key
    and metadata. Only one mechanism (file or inline) may be specified per type.

    Environment variables:
        PALACE_SAML_SP_PRIVATE_KEY_FILE: Path to SP private key file
        PALACE_SAML_SP_PRIVATE_KEY: Inline SP private key content
        PALACE_SAML_SP_METADATA_FILE: Path to SP metadata XML file
        PALACE_SAML_SP_METADATA: Inline SP metadata XML content
    """

    model_config = SettingsConfigDict(env_prefix="PALACE_SAML_SP_")

    # File path options (value is name of file containing content)
    private_key_file: str | None = None
    metadata_file: str | None = None

    # Inline content options (value is the content)
    private_key: str | None = None
    metadata: str | None = None

    @field_validator("private_key_file")
    @classmethod
    def validate_private_key_file(cls, v: str | None) -> str | None:
        """Validate that the private key file exists.

        :param v: File path to validate
        :return: Validated file path
        :raises ValueError: If file doesn't exist, is not a file
        """
        if v is None:
            return None

        path = Path(v)
        if not path.exists():
            raise ValueError(f"File does not exist: {v}")
        if not path.is_file():
            raise ValueError(f"Path is not a file: {v}")

        return v

    @field_validator("metadata_file")
    @classmethod
    def validate_metadata_file(cls, v: str | None) -> str | None:
        """Validate that the metadata file exists and is readable.

        :param v: File path to validate
        :return: Validated file path
        :raises ValueError: If file doesn't exist or is not a file
        """
        if v is None:
            return None

        path = Path(v)
        if not path.exists():
            raise ValueError(f"File does not exist: {v}")
        if not path.is_file():
            raise ValueError(f"Path is not a file: {v}")
        return v

    def get_private_key(self) -> str | None:
        """Get SP private key from file or environment variable.

        Other settings may take precedence over this configuration,
        so this method returns None if no environment config is present,
        allowing the calling code to make that decision.

        :return: SP private key value, or None if not configured
        :raises CannotLoadConfiguration: if both file and inline value are specified
        """
        if self.private_key_file and self.private_key:
            raise CannotLoadConfiguration(
                "Cannot specify both PALACE_SAML_SP_PRIVATE_KEY_FILE and "
                "PALACE_SAML_SP_PRIVATE_KEY. Use only one."
            )

        if self.private_key_file:
            return Path(self.private_key_file).read_text()
        if self.private_key:
            return self.private_key
        return None

    def get_metadata(self) -> str | None:
        """Get SP metadata from file or environment variable.

        Other settings may take precedence over this configuration,
        so this method returns None if no environment config is present,
        allowing the calling code to make that decision.

        :return: SP metadata XML content, or None if not configured
        :raises CannotLoadConfiguration: if both file and inline content are specified
        """
        if self.metadata_file and self.metadata:
            raise CannotLoadConfiguration(
                "Cannot specify both PALACE_SAML_SP_METADATA_FILE and "
                "PALACE_SAML_SP_METADATA. Use only one."
            )

        if self.metadata_file:
            return Path(self.metadata_file).read_text()
        if self.metadata:
            return self.metadata
        return None
