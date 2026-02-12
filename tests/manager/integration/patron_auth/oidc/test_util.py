"""Unit tests for OIDC utility functions."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode
from unittest.mock import Mock, patch

import pytest

from palace.manager.integration.patron_auth.oidc.util import (
    OIDCDiscoveryError,
    OIDCStateValidationError,
    OIDCUtility,
    OIDCUtilityError,
)
from palace.manager.util.http.exception import RequestNetworkException

# Test constants
TEST_SECRET_KEY = "test-secret-key-for-hmac-signing"
TEST_ISSUER_URL = "https://oidc.test.example.com"
PKCE_VERIFIER_MIN_LENGTH = 43
PKCE_VERIFIER_MAX_LENGTH = 128
NONCE_MIN_LENGTH = 16


class TestOIDCUtilityNonce:
    """Tests for nonce generation."""

    def test_generate_nonce_default_length(self):
        nonce = OIDCUtility.generate_nonce()

        assert isinstance(nonce, str)
        assert len(nonce) >= NONCE_MIN_LENGTH

    def test_generate_nonce_custom_length(self):
        custom_length = 64
        nonce = OIDCUtility.generate_nonce(length=custom_length)

        assert isinstance(nonce, str)
        decoded = urlsafe_b64decode(nonce + "==")
        assert len(decoded) == custom_length

    def test_generate_nonce_uniqueness(self):
        nonces = {OIDCUtility.generate_nonce() for _ in range(100)}
        assert len(nonces) == 100


class TestOIDCUtilityPKCE:
    """Tests for PKCE generation."""

    def test_generate_pkce_returns_tuple(self):
        code_verifier, code_challenge = OIDCUtility.generate_pkce()

        assert isinstance(code_verifier, str)
        assert isinstance(code_challenge, str)

    def test_generate_pkce_verifier_length(self):
        code_verifier, _ = OIDCUtility.generate_pkce()

        assert len(code_verifier) >= PKCE_VERIFIER_MIN_LENGTH
        assert len(code_verifier) <= PKCE_VERIFIER_MAX_LENGTH

    def test_generate_pkce_challenge_is_sha256_of_verifier(self):
        code_verifier, code_challenge = OIDCUtility.generate_pkce()

        expected_hash = hashlib.sha256(code_verifier.encode("utf-8")).digest()
        expected_challenge = (
            urlsafe_b64encode(expected_hash).decode("utf-8").rstrip("=")
        )

        assert code_challenge == expected_challenge

    def test_generate_pkce_uniqueness(self):
        results = [OIDCUtility.generate_pkce() for _ in range(100)]
        verifiers = {v for v, _ in results}
        challenges = {c for _, c in results}

        assert len(verifiers) == 100
        assert len(challenges) == 100


class TestOIDCUtilityState:
    """Tests for state parameter generation and validation."""

    def test_generate_state_creates_signed_token(self):
        data = {
            "library_short_name": "TESTLIB",
            "provider_name": "Test Provider",
            "redirect_uri": "app://auth/callback",
        }

        state = OIDCUtility.generate_state(data, TEST_SECRET_KEY)

        assert isinstance(state, str)
        assert "." in state
        signature, encoded_data = state.split(".", 1)
        assert len(signature) > 0
        assert len(encoded_data) > 0

    def test_generate_state_adds_timestamp(self):
        data = {"library_short_name": "TESTLIB"}

        state = OIDCUtility.generate_state(data, TEST_SECRET_KEY)
        _, encoded_data = state.split(".", 1)

        decoded_data = json.loads(urlsafe_b64decode(encoded_data))
        assert "timestamp" in decoded_data
        assert decoded_data["timestamp"] <= int(time.time())

    def test_validate_state_success(self):
        data = {
            "library_short_name": "TESTLIB",
            "provider_name": "Test Provider",
        }

        state = OIDCUtility.generate_state(data, TEST_SECRET_KEY)
        decoded = OIDCUtility.validate_state(state, TEST_SECRET_KEY)

        assert decoded["library_short_name"] == "TESTLIB"
        assert decoded["provider_name"] == "Test Provider"
        assert "timestamp" not in decoded

    def test_validate_state_invalid_signature(self):
        data = {"library_short_name": "TESTLIB"}
        state = OIDCUtility.generate_state(data, TEST_SECRET_KEY)

        wrong_secret = "wrong-secret-key"

        with pytest.raises(
            OIDCStateValidationError, match="signature verification failed"
        ):
            OIDCUtility.validate_state(state, wrong_secret)

    @pytest.mark.parametrize(
        "timestamp_offset,error_match",
        [
            pytest.param(-700, "State expired", id="expired"),
            pytest.param(100, "timestamp is in the future", id="future-timestamp"),
            pytest.param(None, "missing timestamp", id="missing-timestamp"),
        ],
    )
    def test_validate_state_timestamp_validation(self, timestamp_offset, error_match):
        """Test state validation with different timestamp scenarios."""
        data = {"library_short_name": "TESTLIB"}

        if timestamp_offset is not None:
            data["timestamp"] = int(time.time()) + timestamp_offset

        json_data = json.dumps(data, separators=(",", ":"))
        encoded_data = urlsafe_b64encode(json_data.encode("utf-8")).decode("utf-8")

        signature = hmac.new(
            TEST_SECRET_KEY.encode("utf-8"),
            encoded_data.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        encoded_signature = urlsafe_b64encode(signature).decode("utf-8")

        state = f"{encoded_signature}.{encoded_data}"

        with pytest.raises(OIDCStateValidationError, match=error_match):
            OIDCUtility.validate_state(state, TEST_SECRET_KEY)

    @pytest.mark.parametrize(
        "invalid_state",
        [
            pytest.param("no-dot-separator", id="no-dot"),
            pytest.param("", id="empty"),
            pytest.param("only.one.part", id="one-part"),
            pytest.param("invalid-base64!@#$.data", id="invalid-base64"),
        ],
    )
    def test_validate_state_invalid_format(self, invalid_state):
        """Test state validation with invalid format."""
        with pytest.raises(OIDCStateValidationError):
            OIDCUtility.validate_state(invalid_state, TEST_SECRET_KEY)

    def test_validate_state_custom_max_age(self):
        custom_max_age = 120
        data = {"library_short_name": "TESTLIB"}

        state = OIDCUtility.generate_state(data, TEST_SECRET_KEY)
        time.sleep(1)

        decoded = OIDCUtility.validate_state(
            state, TEST_SECRET_KEY, max_age=custom_max_age
        )
        assert decoded["library_short_name"] == "TESTLIB"


class TestOIDCUtilityDiscovery:
    """Tests for OIDC discovery document fetching."""

    def test_discover_oidc_configuration_success(
        self, mock_discovery_document, redis_fixture
    ):
        discovery_url = f"{TEST_ISSUER_URL}/.well-known/openid-configuration"

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)
            result = utility.discover_oidc_configuration(TEST_ISSUER_URL)

            assert result == mock_discovery_document
            mock_get.assert_called_once()
            assert discovery_url in str(mock_get.call_args)

    def test_discover_oidc_configuration_caching(
        self, mock_discovery_document, redis_fixture
    ):
        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            result1 = utility.discover_oidc_configuration(TEST_ISSUER_URL)
            result2 = utility.discover_oidc_configuration(TEST_ISSUER_URL)

            assert result1 == result2
            mock_get.assert_called_once()

    def test_discover_oidc_configuration_cache_disabled(
        self, mock_discovery_document, redis_fixture
    ):
        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            result1 = utility.discover_oidc_configuration(
                TEST_ISSUER_URL, use_cache=False
            )
            result2 = utility.discover_oidc_configuration(
                TEST_ISSUER_URL, use_cache=False
            )

            assert result1 == result2
            assert mock_get.call_count == 2

    def test_discover_oidc_configuration_missing_required_fields(self, redis_fixture):
        incomplete_document = {
            "issuer": TEST_ISSUER_URL,
            "authorization_endpoint": "https://example.com/auth",
        }

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = incomplete_document
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            with pytest.raises(OIDCDiscoveryError, match="missing required fields"):
                utility.discover_oidc_configuration(TEST_ISSUER_URL)

    def test_discover_oidc_configuration_http_error(self, redis_fixture):
        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_get.side_effect = RequestNetworkException(
                f"{TEST_ISSUER_URL}/.well-known/openid-configuration", "404 Not Found"
            )

            utility = OIDCUtility(redis_client=redis_fixture.client)

            with pytest.raises(OIDCDiscoveryError, match="Failed to fetch"):
                utility.discover_oidc_configuration(TEST_ISSUER_URL)

    def test_discover_oidc_configuration_invalid_json(self, redis_fixture):
        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.side_effect = json.JSONDecodeError("error", "doc", 0)
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            with pytest.raises(OIDCDiscoveryError, match="Invalid JSON"):
                utility.discover_oidc_configuration(TEST_ISSUER_URL)


class TestOIDCUtilityJWKS:
    """Tests for JWKS fetching."""

    def test_fetch_jwks_success(self, mock_jwks, redis_fixture):
        jwks_uri = f"{TEST_ISSUER_URL}/.well-known/jwks.json"

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_jwks
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)
            result = utility.fetch_jwks(jwks_uri)

            assert result == mock_jwks
            assert "keys" in result
            mock_get.assert_called_once()

    def test_fetch_jwks_caching(self, mock_jwks, redis_fixture):
        jwks_uri = f"{TEST_ISSUER_URL}/.well-known/jwks.json"

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_jwks
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            result1 = utility.fetch_jwks(jwks_uri)
            result2 = utility.fetch_jwks(jwks_uri)

            assert result1 == result2
            mock_get.assert_called_once()

    def test_fetch_jwks_cache_disabled(self, mock_jwks, redis_fixture):
        jwks_uri = f"{TEST_ISSUER_URL}/.well-known/jwks.json"

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_jwks
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            result1 = utility.fetch_jwks(jwks_uri, use_cache=False)
            result2 = utility.fetch_jwks(jwks_uri, use_cache=False)

            assert result1 == result2
            assert mock_get.call_count == 2

    def test_fetch_jwks_invalid_structure(self, redis_fixture):
        jwks_uri = f"{TEST_ISSUER_URL}/.well-known/jwks.json"
        invalid_jwks = {"invalid": "structure"}

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = invalid_jwks
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            with pytest.raises(OIDCUtilityError, match="must contain a 'keys' array"):
                utility.fetch_jwks(jwks_uri)

    def test_fetch_jwks_http_error(self, redis_fixture):
        jwks_uri = f"{TEST_ISSUER_URL}/.well-known/jwks.json"

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_get.side_effect = RequestNetworkException(
                jwks_uri, "500 Internal Server Error"
            )

            utility = OIDCUtility(redis_client=redis_fixture.client)

            with pytest.raises(OIDCUtilityError, match="Failed to fetch JWKS"):
                utility.fetch_jwks(jwks_uri)

    def test_fetch_jwks_invalid_json(self, redis_fixture):
        jwks_uri = f"{TEST_ISSUER_URL}/.well-known/jwks.json"

        with patch(
            "palace.manager.integration.patron_auth.oidc.util.HTTP.get_with_timeout"
        ) as mock_get:
            mock_response = Mock()
            mock_response.json.side_effect = json.JSONDecodeError("error", "doc", 0)
            mock_get.return_value = mock_response

            utility = OIDCUtility(redis_client=redis_fixture.client)

            with pytest.raises(OIDCUtilityError, match="Invalid JSON in JWKS"):
                utility.fetch_jwks(jwks_uri)


class TestOIDCUtilityPKCEStorage:
    """Tests for PKCE storage and retrieval in Redis."""

    def test_store_pkce_success(self, redis_fixture):
        state_token = "test-state-token"
        code_verifier = "test-code-verifier-abc123"

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_pkce(state_token, code_verifier)

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.PKCE_KEY_PREFIX}{state_token}"
        )
        cached = redis_fixture.client.get(cache_key)

        assert cached is not None
        data = json.loads(cached)
        assert data["code_verifier"] == code_verifier
        assert "timestamp" in data

    def test_store_pkce_with_metadata(self, redis_fixture):
        state_token = "test-state-token"
        code_verifier = "test-code-verifier"
        metadata = {"extra_field": "extra_value"}

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_pkce(state_token, code_verifier, metadata=metadata)

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.PKCE_KEY_PREFIX}{state_token}"
        )
        cached = redis_fixture.client.get(cache_key)

        data = json.loads(cached)
        assert data["code_verifier"] == code_verifier
        assert data["extra_field"] == "extra_value"

    def test_store_pkce_without_redis_raises_error(self):
        utility = OIDCUtility(redis_client=None)

        with pytest.raises(OIDCUtilityError, match="Redis client required"):
            utility.store_pkce("state", "verifier")

    def test_retrieve_pkce_success(self, redis_fixture):
        state_token = "test-state-token"
        code_verifier = "test-code-verifier"

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_pkce(state_token, code_verifier)

        retrieved = utility.retrieve_pkce(state_token, delete=False)

        assert retrieved is not None
        assert retrieved["code_verifier"] == code_verifier

    def test_retrieve_pkce_with_delete(self, redis_fixture):
        state_token = "test-state-token"
        code_verifier = "test-code-verifier"

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_pkce(state_token, code_verifier)

        retrieved1 = utility.retrieve_pkce(state_token, delete=True)
        assert retrieved1 is not None

        retrieved2 = utility.retrieve_pkce(state_token, delete=False)
        assert retrieved2 is None

    def test_retrieve_pkce_not_found(self, redis_fixture):
        utility = OIDCUtility(redis_client=redis_fixture.client)

        retrieved = utility.retrieve_pkce("nonexistent-state")

        assert retrieved is None

    def test_retrieve_pkce_without_redis_raises_error(self):
        utility = OIDCUtility(redis_client=None)

        with pytest.raises(OIDCUtilityError, match="Redis client required"):
            utility.retrieve_pkce("state")

    def test_retrieve_pkce_corrupted_json(self, redis_fixture):
        state_token = "test-state-token"

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.PKCE_KEY_PREFIX}{state_token}"
        )
        redis_fixture.client.set(cache_key, "invalid-json{{{", ex=600)

        utility = OIDCUtility(redis_client=redis_fixture.client)
        retrieved = utility.retrieve_pkce(state_token)

        assert retrieved is None


class TestOIDCUtilityLogoutState:
    """Tests for logout state storage and retrieval."""

    def test_store_logout_state(self, redis_fixture):
        state_token = "test-logout-state-token"
        redirect_uri = "https://app.example.com/logout/callback"
        metadata = {"extra": "data"}

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_logout_state(state_token, redirect_uri, metadata)

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.LOGOUT_STATE_KEY_PREFIX}{state_token}"
        )
        cached = redis_fixture.client.get(cache_key)

        assert cached is not None
        data = json.loads(cached)
        assert data["redirect_uri"] == redirect_uri
        assert data["extra"] == "data"
        assert "timestamp" in data

    def test_store_logout_state_without_metadata(self, redis_fixture):
        state_token = "test-logout-state-token"
        redirect_uri = "https://app.example.com/logout/callback"

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_logout_state(state_token, redirect_uri)

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.LOGOUT_STATE_KEY_PREFIX}{state_token}"
        )
        cached = redis_fixture.client.get(cache_key)

        assert cached is not None
        data = json.loads(cached)
        assert data["redirect_uri"] == redirect_uri
        assert "timestamp" in data

    def test_store_logout_state_requires_redis(self):
        utility = OIDCUtility(redis_client=None)

        with pytest.raises(OIDCUtilityError, match="Redis client required"):
            utility.store_logout_state("state", "https://example.com")

    def test_retrieve_logout_state_with_delete(self, redis_fixture):
        state_token = "test-logout-state-token"
        redirect_uri = "https://app.example.com/logout/callback"

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_logout_state(state_token, redirect_uri)

        retrieved = utility.retrieve_logout_state(state_token, delete=True)

        assert retrieved is not None
        assert retrieved["redirect_uri"] == redirect_uri
        assert "timestamp" in retrieved

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.LOGOUT_STATE_KEY_PREFIX}{state_token}"
        )
        assert redis_fixture.client.get(cache_key) is None

    def test_retrieve_logout_state_without_delete(self, redis_fixture):
        state_token = "test-logout-state-token"
        redirect_uri = "https://app.example.com/logout/callback"

        utility = OIDCUtility(redis_client=redis_fixture.client)
        utility.store_logout_state(state_token, redirect_uri)

        retrieved = utility.retrieve_logout_state(state_token, delete=False)

        assert retrieved is not None
        assert retrieved["redirect_uri"] == redirect_uri

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.LOGOUT_STATE_KEY_PREFIX}{state_token}"
        )
        assert redis_fixture.client.get(cache_key) is not None

    def test_retrieve_logout_state_not_found(self, redis_fixture):
        utility = OIDCUtility(redis_client=redis_fixture.client)
        retrieved = utility.retrieve_logout_state("nonexistent-state")

        assert retrieved is None

    def test_retrieve_logout_state_requires_redis(self):
        utility = OIDCUtility(redis_client=None)

        with pytest.raises(OIDCUtilityError, match="Redis client required"):
            utility.retrieve_logout_state("state")

    def test_retrieve_logout_state_corrupted_json(self, redis_fixture):
        state_token = "test-logout-state-token"

        cache_key = redis_fixture.client.get_key(
            f"{OIDCUtility.LOGOUT_STATE_KEY_PREFIX}{state_token}"
        )
        redis_fixture.client.set(cache_key, "invalid-json{{{", ex=600)

        utility = OIDCUtility(redis_client=redis_fixture.client)
        retrieved = utility.retrieve_logout_state(state_token)

        assert retrieved is None

    def test_delete_logout_state(self, redis_fixture):
        """Test delete_logout_state removes entry from cache."""
        state = "test_state_token"
        redirect_uri = "https://example.com/callback"

        utility = OIDCUtility(redis_client=redis_fixture.client)

        # Store state
        utility.store_logout_state(state, redirect_uri)

        # Verify it's stored
        retrieved = utility.retrieve_logout_state(state, delete=False)
        assert retrieved is not None
        assert retrieved["redirect_uri"] == redirect_uri

        # Delete it
        utility.delete_logout_state(state)

        # Verify it's gone
        retrieved_after = utility.retrieve_logout_state(state, delete=False)
        assert retrieved_after is None

    def test_delete_logout_state_requires_redis(self):
        """Test delete_logout_state raises error without Redis."""
        utility = OIDCUtility(redis_client=None)

        with pytest.raises(OIDCUtilityError, match="Redis client is required"):
            utility.delete_logout_state("state")
