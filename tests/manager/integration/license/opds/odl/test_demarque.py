"""Tests for DeMarque WebReader JWT integration."""

from __future__ import annotations

import json
import time
import uuid
from pathlib import Path
from unittest.mock import patch

import pytest
from freezegun import freeze_time
from jwcrypto.jwk import JWK
from jwcrypto.jwt import JWT

from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.integration.license.opds.odl.demarque import (
    DEMARQUE_WEBREADER_REL,
    DeMarqueWebReader,
    DeMarqueWebReaderConfiguration,
)
from palace.manager.opds.lcp.status import Link as LsdLink
from palace.manager.service.logging.configuration import LogLevel


class JwtKeysFixture:
    """Test JWT keys fixture."""

    def __init__(self) -> None:
        self.ed25519 = JWK(
            kty="OKP",
            crv="Ed25519",
            kid="test-key-id",
            x="11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo",
            d="nWGxne_9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A",
        )
        self.ed448 = JWK(
            kty="OKP",
            crv="Ed448",
            kid="test-ed448-key",
            x="X9dEm1m0Yf0s54fsYWrUah2hNCSFpw4fig6nXYDpZ3jt8SR2m0bHBhvWeD3x5Q9s0foavq_oJWGA",
            d="bIKlYsuAjRDWMr6JyFE-v2ySnzTd-oyfY8mWDvbjSKNSjIo_zC8ETjmj_FuUSS-PAy51SaIAmPlb",
        )
        self.rsa = JWK(
            kty="RSA",
            kid="test-rsa-key",
            n="0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tS"
            "oc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65"
            "YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyr"
            "dkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzK"
            "nqDKgw",
            e="AQAB",
            d="X4cTteJY_gn4FYPsXB8rdXix5vwsg1FLN5E3EaG6RJoVH-HLLKD9M7dx5oo7GURknchnrRweUkC7hT5fJLM0"
            "WbFAKNLWY2vv7B6NqXSzUvxT0_YSfqijwp3RTzlBaCxWp4doFk5N2o8Gy_nHNKroADIkJ46pRUohsXywbReA"
            "dYaMwFs9tv8d_cPVY3i07a3t8MN6TNwm0dSawm9v47UiCl3Sk5ZiG7xojPLu4sbg1U2jx4IBTNBznbJSzFHK"
            "66jT8bgkuqsk0GjskDJk19Z4qwjwbsnn4j2WBii3RL-Us2lGVkY8fkFzme1z0HbIkfz0Y6mqnOYtqc0X4jfc"
            "KoAC8Q",
        )

    def without(self, name: str) -> str:
        """Return a JWK JSON string without the specified field."""
        jwk = self.ed25519.export(as_dict=True)
        del jwk[name]
        return json.dumps(jwk)


@pytest.fixture(scope="module")
def jwt_keys_fixture() -> JwtKeysFixture:
    """Provide test keys for JWT testing."""
    return JwtKeysFixture()


class TestDeMarqueWebReaderConfiguration:
    """Tests for DeMarqueWebReaderConfiguration."""

    def test_default_values(self) -> None:
        """Test that configuration has expected default values."""
        config = DeMarqueWebReaderConfiguration()

        assert config.issuer_url is None
        assert config.jwk_file is None
        assert config.jwk is None
        assert config.language == "en"
        assert config.showcase_tts is False
        assert config.allow_offline is False

    def test_get_jwk_from_inline(self, jwt_keys_fixture: JwtKeysFixture) -> None:
        """Test loading JWK from inline string."""
        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com", jwk=jwt_keys_fixture.ed25519.export()
        )

        jwk = config.get_jwk()
        assert jwk is not None
        assert jwk.get("kid") == jwt_keys_fixture.ed25519["kid"]
        assert jwk.get("kty") == "OKP"
        assert jwk.get("crv") == "Ed25519"

    def test_get_jwk_from_file(
        self, jwt_keys_fixture: JwtKeysFixture, tmp_path: Path
    ) -> None:
        """Test loading JWK from file."""
        jwk_file = tmp_path / "test.jwk"
        jwk_file.write_text(jwt_keys_fixture.ed25519.export())

        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk_file=jwk_file,
        )

        jwk = config.get_jwk()
        assert jwk is not None
        assert jwk.get("kid") == jwt_keys_fixture.ed25519["kid"]

    def test_get_jwk_inline_takes_precedence(
        self, jwt_keys_fixture: JwtKeysFixture, tmp_path: Path
    ) -> None:
        """Test that inline JWK takes precedence over file."""
        # Create a file with a different key ID
        jwk_file = tmp_path / "test.jwk"
        file_jwk = jwt_keys_fixture.ed25519.export(as_dict=True)
        file_jwk["kid"] = "file-key-id"
        jwk_file.write_text(json.dumps(file_jwk))

        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk=jwt_keys_fixture.ed25519.export(),  # Has kid "test-key-id"
            jwk_file=jwk_file,
        )

        jwk = config.get_jwk()
        assert jwk is not None
        assert jwk.get("kid") == jwt_keys_fixture.ed25519["kid"]

    def test_get_jwk_file_not_found(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test get_jwk returns None and logs warning when file doesn't exist."""
        caplog.set_level(LogLevel.warning)
        jwk_file = tmp_path / "nonexistent.jwk"
        config = DeMarqueWebReaderConfiguration(
            jwk_file=jwk_file,
        )
        assert config.get_jwk() is None
        assert "JWK file configured but not found" in caplog.text
        assert str(jwk_file) in caplog.text

    def test_get_jwk_file_empty(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test get_jwk returns None and logs warning when file is empty."""
        caplog.set_level(LogLevel.warning)
        jwk_file = tmp_path / "empty.jwk"
        jwk_file.write_text("")
        config = DeMarqueWebReaderConfiguration(
            jwk_file=jwk_file,
        )
        assert config.get_jwk() is None
        assert "JWK file configured but empty" in caplog.text
        assert str(jwk_file) in caplog.text

    def test_get_jwk_invalid_json(self) -> None:
        """Test get_jwk returns None for invalid JSON."""
        config = DeMarqueWebReaderConfiguration(
            jwk="not valid json",
        )
        assert config.get_jwk() is None

    def test_get_jwk_missing_kid(self, jwt_keys_fixture: JwtKeysFixture) -> None:
        """Test get_jwk returns None when kid is missing."""
        config = DeMarqueWebReaderConfiguration(
            jwk=jwt_keys_fixture.without("kid"),
        )
        assert config.get_jwk() is None

    def test_get_jwk_missing_private_key(
        self, jwt_keys_fixture: JwtKeysFixture
    ) -> None:
        """Test get_jwk returns None when private key is missing."""
        config = DeMarqueWebReaderConfiguration(
            jwk=jwt_keys_fixture.without("d"),
        )
        assert config.get_jwk() is None

    def test_get_jwk_wrong_curve(self, jwt_keys_fixture: JwtKeysFixture) -> None:
        """Test get_jwk returns None for wrong curve."""
        config = DeMarqueWebReaderConfiguration(
            jwk=jwt_keys_fixture.ed448.export(),
        )
        assert config.get_jwk() is None

    def test_get_jwk_wrong_kty(self, jwt_keys_fixture: JwtKeysFixture) -> None:
        """Test get_jwk returns None for wrong key type."""
        config = DeMarqueWebReaderConfiguration(jwk=jwt_keys_fixture.rsa.export())
        assert config.get_jwk() is None


class TestDeMarqueWebReader:
    """Tests for DeMarqueWebReader."""

    @pytest.fixture
    def valid_config(
        self, jwt_keys_fixture: JwtKeysFixture
    ) -> DeMarqueWebReaderConfiguration:
        """Create a valid configuration for testing."""
        return DeMarqueWebReaderConfiguration(
            issuer_url="https://library.example.com",
            jwk=jwt_keys_fixture.ed25519.export(),
            language="nl",
            showcase_tts=True,
            allow_offline=True,
        )

    @pytest.fixture
    def webreader(
        self, valid_config: DeMarqueWebReaderConfiguration
    ) -> DeMarqueWebReader:
        """Create a DeMarqueWebReader instance for testing."""
        reader = DeMarqueWebReader.create(valid_config)
        assert reader is not None
        return reader

    def test_create_success(self, valid_config: DeMarqueWebReaderConfiguration) -> None:
        """Test successful creation of WebReader client."""
        reader = DeMarqueWebReader.create(valid_config)
        assert reader is not None

    def test_create_missing_issuer_url(self, jwt_keys_fixture: JwtKeysFixture) -> None:
        """Test create returns None when issuer_url is missing."""
        config = DeMarqueWebReaderConfiguration(
            jwk=jwt_keys_fixture.ed25519.export(),
        )
        assert DeMarqueWebReader.create(config) is None

    def test_create_missing_jwk(self) -> None:
        """Test create returns None when JWK is missing."""
        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
        )
        assert DeMarqueWebReader.create(config) is None

    def test_create_invalid_jwk(self) -> None:
        """Test create returns None when JWK is invalid."""
        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk="invalid",
        )
        assert DeMarqueWebReader.create(config) is None

    def test_create_default_config(
        self, jwt_keys_fixture: JwtKeysFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test create loads config from environment when not provided."""
        monkeypatch.setenv(
            "PALACE_DEMARQUE_WEBREADER_ISSUER_URL", "https://library.example.com"
        )
        monkeypatch.setenv(
            "PALACE_DEMARQUE_WEBREADER_JWK", jwt_keys_fixture.ed25519.export()
        )

        reader = DeMarqueWebReader.create()
        assert reader._jwk_key.export() == jwt_keys_fixture.ed25519.export()
        assert reader._key_id == jwt_keys_fixture.ed25519["kid"]
        assert reader._issuer_url == "https://library.example.com"

    @freeze_time()
    def test_generate_token(
        self,
        jwt_keys_fixture: JwtKeysFixture,
        webreader: DeMarqueWebReader,
        valid_config: DeMarqueWebReaderConfiguration,
    ) -> None:
        """Test that generated token has correct structure and claims."""
        token = webreader.generate_token("test-publication-id")

        # Parse and verify the token
        jwt = JWT(key=jwt_keys_fixture.ed25519, jwt=token)

        # Verify header
        header = json.loads(jwt.header)
        assert header["alg"] == "EdDSA"
        assert header["kid"] == jwt_keys_fixture.ed25519["kid"]

        # Verify claims
        claims = json.loads(jwt.claims)
        assert claims["allowOffline"] == valid_config.allow_offline
        assert claims["aud"] == "https://r.cantook.com"
        assert claims["iss"] == valid_config.issuer_url
        assert claims["language"] == valid_config.language
        assert claims["showcaseTTS"] == valid_config.showcase_tts
        assert claims["sub"] == "test-publication-id"
        assert claims["iat"] == int(time.time())
        assert "jti" in claims

    def test_generate_token_unique_jti(
        self, jwt_keys_fixture: JwtKeysFixture, webreader: DeMarqueWebReader
    ) -> None:
        """Test that each token has a unique jti claim."""
        token1 = webreader.generate_token("test-id")
        token2 = webreader.generate_token("test-id")

        jwt1 = JWT(key=jwt_keys_fixture.ed25519, jwt=token1)
        jwt2 = JWT(key=jwt_keys_fixture.ed25519, jwt=token2)

        claims1 = json.loads(jwt1.claims)
        claims2 = json.loads(jwt2.claims)

        # JTI is a valid uuid
        uuid.UUID(claims1["jti"])
        uuid.UUID(claims2["jti"])

        assert claims1["jti"] != claims2["jti"]

    def test_fulfill_link_success(self, webreader: DeMarqueWebReader) -> None:
        """Test successful link fulfillment."""
        link = LsdLink(
            href="https://r.cantook.com/read/{?token}",
            type="application/monster",
            templated=True,
            rel=["publication", DEMARQUE_WEBREADER_REL],
            properties={"identifier": "test-publication-id"},
        )

        with patch.object(
            webreader, "generate_token", return_value="generated_token%?&"
        ):
            result = webreader.fulfill_link(link)

        # The type is preserved
        assert result.type == "application/monster"
        # The token has been templated into the link and is properly escaped
        assert (
            result.href == "https://r.cantook.com/read/?token=generated_token%25%3F%26"
        )

    def test_fulfill_link_missing_identifier(
        self, webreader: DeMarqueWebReader
    ) -> None:
        """Test that fulfill_link raises CannotFulfill when identifier is missing."""
        link = LsdLink(
            href="https://r.cantook.com/read/{?token}",
            type="text/html",
            templated=True,
        )

        with pytest.raises(CannotFulfill):
            webreader.fulfill_link(link)
