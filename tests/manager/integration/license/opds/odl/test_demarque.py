"""Tests for DeMarque WebReader JWT integration."""

from __future__ import annotations

import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from frozendict import frozendict
from jwcrypto.jwk import JWK
from jwcrypto.jwt import JWT

from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.integration.license.opds.odl.demarque import (
    DEMARQUE_WEBREADER_REL,
    DeMarqueWebReader,
    DeMarqueWebReaderConfiguration,
)
from palace.manager.opds.lcp.status import Link as LsdLink


class Ed25519TestKey:
    """Test Ed25519 key fixture for JWT testing."""

    def __init__(self) -> None:
        # Valid Ed25519 private key JWK with kid
        self.jwk = frozendict(
            {
                "kty": "OKP",
                "crv": "Ed25519",
                "kid": "test-key-id",
                "x": "11qYAYKxCrfVS_7TyWQHOg7hcvPapiMlrwIaaPcHURo",
                "d": "nWGxne_9WmC6hEr0kuwsxERJxWl7MmkZcDusAxyuf2A",
            }
        )

    @property
    def kid(self) -> str:
        """Return the key ID."""
        return self.jwk["kid"]

    def as_jwk(self) -> JWK:
        """Return the key as a JWK object."""
        return JWK(**self.jwk)

    def as_json(self) -> str:
        """Return the key as a JSON string."""
        return json.dumps(self.jwk)

    def without(self, name: str) -> str:
        """Return a JWK JSON string without the specified field"""
        jwk = dict(self.jwk)
        del jwk[name]
        return json.dumps(jwk)

    @staticmethod
    def wrong_curve() -> str:
        """Return a JWK JSON string with wrong curve (P-256 instead of Ed25519)."""
        return json.dumps(
            {
                "kty": "EC",
                "crv": "P-256",
                "kid": "test-key-id",
                "x": "f83OJ3D2xF1Bg8vub9tLe1gHMzV76e8Tus9uPHvRVEU",
                "y": "x_FEzRu9m36HLN_tue659LNpXW6pCyStikYjKIWI5a0",
                "d": "jpsQnnGQmL-YBIffH1136cspYG6-0iY7X1fCE9-E9LI",
            }
        )

    @staticmethod
    def wrong_kty() -> str:
        """Return a JWK JSON string with wrong key type (RSA instead of OKP)."""
        return json.dumps(
            {
                "kty": "RSA",
                "kid": "test-key-id",
                "n": "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
                "e": "AQAB",
                "d": "X4cTteJY_gn4FYPsXB8rdXix5vwsg1FLN5E3EaG6RJoVH-HLLKD9M7dx5oo7GURknchnrRweUkC7hT5fJLM0WbFAKNLWY2vv7B6NqXSzUvxT0_YSfqijwp3RTzlBaCxWp4doFk5N2o8Gy_nHNKroADIkJ46pRUohsXywbReAdYaMwFs9tv8d_cPVY3i07a3t8MN6TNwm0dSawm9v47UiCl3Sk5ZiG7xojPLu4sbg1U2jx4IBTNBznbJSzFHK66jT8bgkuqsk0GjskDJk19Z4qwjwbsnn4j2WBii3RL-Us2lGVkY8fkFzme1z0HbIkfz0Y6mqnOYtqc0X4jfcKoAC8Q",
            }
        )


@pytest.fixture
def ed25519_test_key() -> Ed25519TestKey:
    """Provide an Ed25519 test key for JWT testing."""
    return Ed25519TestKey()


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

    def test_get_jwk_from_inline(self, ed25519_test_key: Ed25519TestKey) -> None:
        """Test loading JWK from inline string."""
        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk=ed25519_test_key.as_json(),
        )

        jwk = config.get_jwk()
        assert jwk is not None
        assert jwk.get("kid") == ed25519_test_key.kid
        assert jwk.get("kty") == "OKP"
        assert jwk.get("crv") == "Ed25519"

    def test_get_jwk_from_file(
        self, ed25519_test_key: Ed25519TestKey, tmp_path: Path
    ) -> None:
        """Test loading JWK from file."""
        jwk_file = tmp_path / "test.jwk"
        jwk_file.write_text(ed25519_test_key.as_json())

        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk_file=jwk_file,
        )

        jwk = config.get_jwk()
        assert jwk is not None
        assert jwk.get("kid") == ed25519_test_key.kid

    def test_get_jwk_inline_takes_precedence(
        self, ed25519_test_key: Ed25519TestKey, tmp_path: Path
    ) -> None:
        """Test that inline JWK takes precedence over file."""
        # Create a file with a different key ID
        jwk_file = tmp_path / "test.jwk"
        file_jwk = dict(ed25519_test_key.jwk)
        file_jwk["kid"] = "file-key-id"
        jwk_file.write_text(json.dumps(file_jwk))

        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk=ed25519_test_key.as_json(),  # Has kid "test-key-id"
            jwk_file=jwk_file,
        )

        jwk = config.get_jwk()
        assert jwk is not None
        assert jwk.get("kid") == ed25519_test_key.kid  # Inline value, not file

    def test_get_jwk_file_not_found(self, tmp_path: Path) -> None:
        """Test get_jwk returns None when file doesn't exist."""
        config = DeMarqueWebReaderConfiguration(
            jwk_file=tmp_path / "nonexistent.jwk",
        )
        assert config.get_jwk() is None

    def test_get_jwk_invalid_json(self) -> None:
        """Test get_jwk returns None for invalid JSON."""
        config = DeMarqueWebReaderConfiguration(
            jwk="not valid json",
        )
        assert config.get_jwk() is None

    def test_get_jwk_missing_kid(self, ed25519_test_key: Ed25519TestKey) -> None:
        """Test get_jwk returns None when kid is missing."""
        config = DeMarqueWebReaderConfiguration(
            jwk=ed25519_test_key.without("kid"),
        )
        assert config.get_jwk() is None

    def test_get_jwk_missing_private_key(
        self, ed25519_test_key: Ed25519TestKey
    ) -> None:
        """Test get_jwk returns None when private key is missing."""
        config = DeMarqueWebReaderConfiguration(
            jwk=ed25519_test_key.without("d"),
        )
        assert config.get_jwk() is None

    def test_get_jwk_wrong_curve(self) -> None:
        """Test get_jwk returns None for wrong curve."""
        config = DeMarqueWebReaderConfiguration(
            jwk=Ed25519TestKey.wrong_curve(),
        )
        assert config.get_jwk() is None

    def test_get_jwk_wrong_kty(self) -> None:
        """Test get_jwk returns None for wrong key type."""
        config = DeMarqueWebReaderConfiguration(
            jwk=Ed25519TestKey.wrong_kty(),
        )
        assert config.get_jwk() is None


class TestDeMarqueWebReader:
    """Tests for DeMarqueWebReader."""

    @pytest.fixture
    def valid_config(
        self, ed25519_test_key: Ed25519TestKey
    ) -> DeMarqueWebReaderConfiguration:
        """Create a valid configuration for testing."""
        return DeMarqueWebReaderConfiguration(
            issuer_url="https://library.example.com",
            jwk=ed25519_test_key.as_json(),
            language="fr",
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

    def test_create_missing_issuer_url(self, ed25519_test_key: Ed25519TestKey) -> None:
        """Test create returns None when issuer_url is missing."""
        config = DeMarqueWebReaderConfiguration(
            jwk=ed25519_test_key.as_json(),
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
        self, ed25519_test_key: Ed25519TestKey, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test create loads config from environment when not provided."""
        monkeypatch.setenv(
            "PALACE_DEMARQUE_WEBREADER_ISSUER_URL", "https://library.example.com"
        )
        monkeypatch.setenv("PALACE_DEMARQUE_WEBREADER_JWK", ed25519_test_key.as_json())

        reader = DeMarqueWebReader.create()
        assert reader is not None

    def test_generate_token_structure(
        self, ed25519_test_key: Ed25519TestKey, webreader: DeMarqueWebReader
    ) -> None:
        """Test that generated token has correct structure."""
        token = webreader.generate_token("test-publication-id")

        # Parse and verify the token
        jwt = JWT()
        jwt.deserialize(token)

        # Verify header
        header = json.loads(jwt.token.objects["protected"])
        assert header["alg"] == "EdDSA"
        assert header["kid"] == ed25519_test_key.kid

    def test_generate_token_claims(
        self, ed25519_test_key: Ed25519TestKey, webreader: DeMarqueWebReader
    ) -> None:
        """Test that generated token has correct claims."""
        before = int(time.time())
        token = webreader.generate_token("test-publication-id")
        after = int(time.time())

        # Decode and verify claims
        jwt = JWT(
            key=ed25519_test_key.as_jwk(),
            jwt=token,
        )
        claims = json.loads(jwt.claims)

        assert claims["iss"] == "https://library.example.com"
        assert claims["sub"] == "test-publication-id"
        assert claims["aud"] == "https://r.cantook.com"
        assert before <= claims["iat"] <= after
        assert "jti" in claims  # UUID should be present

        # Verify display options
        assert claims["language"] == "fr"
        assert claims["showcaseTTS"] is True
        assert claims["allowOffline"] is True

    def test_generate_token_default_display_options(
        self, ed25519_test_key: Ed25519TestKey
    ) -> None:
        """Test that default display options are included in token."""
        config = DeMarqueWebReaderConfiguration(
            issuer_url="https://example.com",
            jwk=ed25519_test_key.as_json(),
            # Using defaults: language="en", showcase_tts=False, allow_offline=False
        )
        reader = DeMarqueWebReader.create(config)
        assert reader is not None

        token = reader.generate_token("test-id")
        jwt = JWT(key=ed25519_test_key.as_jwk(), jwt=token)
        claims = json.loads(jwt.claims)

        assert claims["language"] == "en"
        assert claims["showcaseTTS"] is False
        assert claims["allowOffline"] is False

    def test_generate_token_unique_jti(
        self, ed25519_test_key: Ed25519TestKey, webreader: DeMarqueWebReader
    ) -> None:
        """Test that each token has a unique jti claim."""
        token1 = webreader.generate_token("test-id")
        token2 = webreader.generate_token("test-id")

        jwt1 = JWT(key=ed25519_test_key.as_jwk(), jwt=token1)
        jwt2 = JWT(key=ed25519_test_key.as_jwk(), jwt=token2)

        claims1 = json.loads(jwt1.claims)
        claims2 = json.loads(jwt2.claims)

        assert claims1["jti"] != claims2["jti"]

    def test_fulfill_link_success(self, webreader: DeMarqueWebReader) -> None:
        """Test successful link fulfillment."""
        link = LsdLink(
            href="https://r.cantook.com/read/{?token}",
            type="text/html",
            templated=True,
            rel=["publication", DEMARQUE_WEBREADER_REL],
            properties={"identifier": "test-publication-id"},
        )

        result = webreader.fulfill_link(link)

        assert result.type == "text/html"
        assert result.href.startswith("https://r.cantook.com/read/?token=")
        # Verify the token is present and properly URL-encoded
        assert "token=" in result.href

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

    def test_fulfill_link_preserves_type(self, webreader: DeMarqueWebReader) -> None:
        """Test that fulfill_link preserves the link type."""
        link = LsdLink(
            href="https://r.cantook.com/read/{?token}",
            type="application/xhtml+xml",
            templated=True,
            properties={"identifier": "test-id"},
        )

        result = webreader.fulfill_link(link)
        assert result.type == "application/xhtml+xml"

    def test_fulfill_link_token_is_valid_jwt(
        self, ed25519_test_key: Ed25519TestKey, webreader: DeMarqueWebReader
    ) -> None:
        """Test that the token in the fulfilled link is a valid JWT."""
        link = LsdLink(
            href="https://r.cantook.com/read/{?token}",
            type="text/html",
            templated=True,
            properties={"identifier": "my-publication"},
        )

        result = webreader.fulfill_link(link)

        # Extract token from URL
        parsed = urlparse(result.href)
        token = parse_qs(parsed.query)["token"][0]

        # Verify it's a valid JWT with correct subject
        jwt = JWT(key=ed25519_test_key.as_jwk(), jwt=token)
        claims = json.loads(jwt.claims)
        assert claims["sub"] == "my-publication"
