"""Tests for SAML Service Provider configuration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.integration.patron_auth.saml.configuration.service_provider import (
    SamlServiceProviderConfiguration,
)

if TYPE_CHECKING:
    from pyfakefs.fake_filesystem import FakeFilesystem
    from pytest import MonkeyPatch


@pytest.fixture
def sample_private_key() -> str:
    """Sample private key content for testing."""
    return """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC8pWs+oGRNfeM0
-----END PRIVATE KEY-----"""


@pytest.fixture
def sample_metadata() -> str:
    """Sample SP metadata XML content for testing."""
    return """<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata"
    entityID="https://example.com/saml-sp">
    <SPSSODescriptor/>
</EntityDescriptor>"""


class TestSAMLServiceProviderConfiguration:
    """Tests for SAMLServiceProviderConfiguration class."""

    def test_no_configuration(self, monkeypatch: MonkeyPatch) -> None:
        """Test configuration with no environment variables set."""
        # Clear any SAML environment variables
        for key in [
            "PALACE_SAML_SP_PRIVATE_KEY_FILE",
            "PALACE_SAML_SP_PRIVATE_KEY",
            "PALACE_SAML_SP_METADATA_FILE",
            "PALACE_SAML_SP_METADATA",
        ]:
            monkeypatch.delenv(key, raising=False)

        config = SamlServiceProviderConfiguration()
        assert config.get_private_key() is None
        assert config.get_metadata() is None

    def test_inline_private_key(
        self, monkeypatch: MonkeyPatch, sample_private_key: str
    ) -> None:
        """Test loading private key from inline environment variable."""
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY", sample_private_key)

        config = SamlServiceProviderConfiguration()
        assert config.get_private_key() == sample_private_key
        assert config.get_metadata() is None

    def test_inline_metadata(
        self, monkeypatch: MonkeyPatch, sample_metadata: str
    ) -> None:
        """Test loading metadata from inline environment variable."""
        monkeypatch.setenv("PALACE_SAML_SP_METADATA", sample_metadata)

        config = SamlServiceProviderConfiguration()
        assert config.get_metadata() == sample_metadata
        assert config.get_private_key() is None

    def test_file_private_key(
        self,
        monkeypatch: MonkeyPatch,
        fs: FakeFilesystem,
        sample_private_key: str,
    ) -> None:
        """Test loading private key from file path."""
        # Create a private key file with secure permissions
        fs.create_file("/config/sp_key.pem", contents=sample_private_key)

        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY_FILE", "/config/sp_key.pem")

        config = SamlServiceProviderConfiguration()
        assert config.get_private_key() == sample_private_key
        assert config.get_metadata() is None

    def test_file_metadata(
        self,
        monkeypatch: MonkeyPatch,
        fs: FakeFilesystem,
        sample_metadata: str,
    ) -> None:
        """Test loading metadata from file path."""
        fs.create_file("/config/sp_metadata.xml", contents=sample_metadata)

        monkeypatch.setenv("PALACE_SAML_SP_METADATA_FILE", "/config/sp_metadata.xml")

        config = SamlServiceProviderConfiguration()
        assert config.get_metadata() == sample_metadata
        assert config.get_private_key() is None

    def test_mixed_configuration(
        self,
        monkeypatch: MonkeyPatch,
        fs: FakeFilesystem,
        sample_private_key: str,
        sample_metadata: str,
    ) -> None:
        """Test using file for key and inline for metadata (mixed approach)."""
        fs.create_file("/config/sp_key.pem", contents=sample_private_key)
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY_FILE", "/config/sp_key.pem")
        monkeypatch.setenv("PALACE_SAML_SP_METADATA", sample_metadata)

        config = SamlServiceProviderConfiguration()
        assert config.get_private_key() == sample_private_key
        assert config.get_metadata() == sample_metadata

    def test_both_file_and_inline_private_key_raises_error(
        self,
        monkeypatch: MonkeyPatch,
        fs: FakeFilesystem,
        sample_private_key: str,
    ) -> None:
        """Test that specifying both file and inline for private key raises error."""
        fs.create_file("/config/sp_key.pem", contents=sample_private_key)
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY_FILE", "/config/sp_key.pem")
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY", sample_private_key)

        config = SamlServiceProviderConfiguration()
        with pytest.raises(
            CannotLoadConfiguration,
            match="Cannot specify both PALACE_SAML_SP_PRIVATE_KEY_FILE and "
            "PALACE_SAML_SP_PRIVATE_KEY",
        ):
            config.get_private_key()

    def test_both_file_and_inline_metadata_raises_error(
        self,
        monkeypatch: MonkeyPatch,
        fs: FakeFilesystem,
        sample_metadata: str,
    ) -> None:
        """Test that specifying both file and inline for metadata raises error."""
        fs.create_file("/config/sp_metadata.xml", contents=sample_metadata)

        monkeypatch.setenv("PALACE_SAML_SP_METADATA_FILE", "/config/sp_metadata.xml")
        monkeypatch.setenv("PALACE_SAML_SP_METADATA", sample_metadata)

        config = SamlServiceProviderConfiguration()
        with pytest.raises(
            CannotLoadConfiguration,
            match="Cannot specify both PALACE_SAML_SP_METADATA_FILE and "
            "PALACE_SAML_SP_METADATA",
        ):
            config.get_metadata()

    def test_private_key_file_not_exists(self, monkeypatch: MonkeyPatch) -> None:
        """Test that non-existent private key file raises validation error."""
        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY_FILE", "/nonexistent/sp_key.pem")

        with pytest.raises(
            CannotLoadConfiguration,
            match="PALACE_SAML_SP_PRIVATE_KEY_FILE:.*File does not exist",
        ):
            SamlServiceProviderConfiguration()

    def test_metadata_file_not_exists(self, monkeypatch: MonkeyPatch) -> None:
        """Test that non-existent metadata file raises validation error."""
        monkeypatch.setenv(
            "PALACE_SAML_SP_METADATA_FILE", "/nonexistent/sp_metadata.xml"
        )

        with pytest.raises(
            CannotLoadConfiguration,
            match="PALACE_SAML_SP_METADATA_FILE:.*File does not exist",
        ):
            SamlServiceProviderConfiguration()

    def test_private_key_file_is_directory(
        self, monkeypatch: MonkeyPatch, fs: FakeFilesystem
    ) -> None:
        """Test that directory path for private key file raises validation error."""
        fs.create_dir("/config")

        monkeypatch.setenv("PALACE_SAML_SP_PRIVATE_KEY_FILE", "/config")

        with pytest.raises(
            CannotLoadConfiguration,
            match="PALACE_SAML_SP_PRIVATE_KEY_FILE:.*Path is not a file",
        ):
            SamlServiceProviderConfiguration()

    def test_metadata_file_is_directory(
        self, monkeypatch: MonkeyPatch, fs: FakeFilesystem
    ) -> None:
        """Test that directory path for metadata file raises validation error."""
        fs.create_dir("/config")

        monkeypatch.setenv("PALACE_SAML_SP_METADATA_FILE", "/config")

        with pytest.raises(
            CannotLoadConfiguration,
            match="PALACE_SAML_SP_METADATA_FILE:.*Path is not a file",
        ):
            SamlServiceProviderConfiguration()
