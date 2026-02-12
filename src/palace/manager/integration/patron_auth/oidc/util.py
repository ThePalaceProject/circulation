"""OIDC Utility Functions.

This module provides utility functions for OIDC authentication including:
- PKCE (Proof Key for Code Exchange) generation
- State parameter generation and validation
- OIDC discovery document fetching
- JWKS (JSON Web Key Set) fetching and caching
- Nonce generation
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any, cast

import httpx
from pydantic import HttpUrl

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.service.redis.redis import Redis
from palace.manager.util.log import LoggerMixin


class OIDCUtilityError(BasePalaceException):
    """Base exception for OIDC utility errors."""


class OIDCDiscoveryError(OIDCUtilityError):
    """Raised when OIDC discovery fails."""


class OIDCStateValidationError(OIDCUtilityError):
    """Raised when state parameter validation fails."""


class OIDCUtility(LoggerMixin):
    """Utility class for OIDC operations."""

    # Cache TTLs
    DISCOVERY_CACHE_TTL = 86400  # 24 hours
    JWKS_CACHE_TTL = 86400  # 24 hours
    PKCE_CACHE_TTL = 600  # 10 minutes
    STATE_MAX_AGE = 600  # 10 minutes
    LOGOUT_STATE_CACHE_TTL = 600  # 10 minutes
    LOGOUT_STATE_MAX_AGE = 600  # 10 minutes

    # Cache key prefixes
    DISCOVERY_KEY_PREFIX = "oidc:discovery:"
    JWKS_KEY_PREFIX = "oidc:jwks:"
    PKCE_KEY_PREFIX = "oidc:pkce:"
    LOGOUT_STATE_KEY_PREFIX = "oidc:logout_state:"

    def __init__(self, redis_client: Redis | None = None):
        """Initialize OIDC utility.

        :param redis_client: Optional Redis client for caching
        """
        self._redis = redis_client

    @staticmethod
    def generate_nonce(length: int = 32) -> str:
        """Generate a cryptographically random nonce.

        :param length: Length of the nonce in characters
        :return: Base64url-encoded random string
        """
        random_bytes = secrets.token_bytes(length)
        return base64.urlsafe_b64encode(random_bytes).decode("utf-8").rstrip("=")

    @staticmethod
    def generate_pkce() -> tuple[str, str]:
        """Generate PKCE code verifier and challenge.

        Implements RFC 7636 - Proof Key for Code Exchange.

        :return: Tuple of (code_verifier, code_challenge)
        """
        # Generate code_verifier: random string of 43-128 characters
        # Using 96 bytes of randomness -> 128 base64url characters
        verifier_bytes = secrets.token_bytes(96)
        code_verifier = (
            base64.urlsafe_b64encode(verifier_bytes).decode("utf-8").rstrip("=")
        )

        # Generate code_challenge: SHA256(code_verifier)
        verifier_hash = hashlib.sha256(code_verifier.encode("utf-8")).digest()
        code_challenge = (
            base64.urlsafe_b64encode(verifier_hash).decode("utf-8").rstrip("=")
        )

        return code_verifier, code_challenge

    @staticmethod
    def generate_state(data: dict[str, Any], secret: str) -> str:
        """Generate HMAC-signed state parameter.

        :param data: Dictionary of state data to encode
        :param secret: Secret key for HMAC signing
        :return: Base64url-encoded signed state token
        """
        # Add timestamp for replay protection
        state_data = {**data, "timestamp": int(time.time())}

        # JSON encode the data
        json_data = json.dumps(state_data, separators=(",", ":"))
        encoded_data = base64.urlsafe_b64encode(json_data.encode("utf-8")).decode(
            "utf-8"
        )

        # Generate HMAC signature
        signature = hmac.new(
            secret.encode("utf-8"), encoded_data.encode("utf-8"), hashlib.sha256
        ).digest()
        encoded_signature = base64.urlsafe_b64encode(signature).decode("utf-8")

        # Combine: {signature}.{data}
        return f"{encoded_signature}.{encoded_data}"

    @classmethod
    def validate_state(
        cls, state: str, secret: str, max_age: int | None = None
    ) -> dict[str, Any]:
        """Validate and decode HMAC-signed state parameter.

        :param state: Signed state token
        :param secret: Secret key for HMAC verification
        :param max_age: Maximum age in seconds (default: STATE_MAX_AGE)
        :raises OIDCStateValidationError: If validation fails
        :return: Decoded state data
        """
        if max_age is None:
            max_age = cls.STATE_MAX_AGE

        try:
            # Split signature and data
            encoded_signature, encoded_data = state.split(".", 1)

            # Verify HMAC signature
            expected_signature = hmac.new(
                secret.encode("utf-8"), encoded_data.encode("utf-8"), hashlib.sha256
            ).digest()
            expected_encoded = base64.urlsafe_b64encode(expected_signature).decode(
                "utf-8"
            )

            if not hmac.compare_digest(encoded_signature, expected_encoded):
                raise OIDCStateValidationError("State signature verification failed")

            # Decode data
            json_data = base64.urlsafe_b64decode(encoded_data).decode("utf-8")
            state_data = cast(dict[str, Any], json.loads(json_data))

            # Validate timestamp
            timestamp = state_data.get("timestamp")
            if timestamp is None:
                raise OIDCStateValidationError("State missing timestamp")

            age = int(time.time()) - timestamp
            if age > max_age:
                raise OIDCStateValidationError(
                    f"State expired (age: {age}s, max: {max_age}s)"
                )

            if age < 0:
                raise OIDCStateValidationError("State timestamp is in the future")

            # Remove timestamp from returned data
            del state_data["timestamp"]
            return state_data

        except (ValueError, KeyError, json.JSONDecodeError) as e:
            cls.logger().exception("Failed to decode state parameter")
            raise OIDCStateValidationError(
                f"Invalid state parameter format: {str(e)}"
            ) from e

    def discover_oidc_configuration(
        self, issuer_url: HttpUrl, use_cache: bool = True
    ) -> dict[str, Any]:
        """Fetch OIDC discovery document.

        Retrieves the OIDC provider's configuration from the well-known
        discovery endpoint: {issuer}/.well-known/openid-configuration

        :param issuer_url: OIDC provider's issuer URL
        :param use_cache: Whether to use/update Redis cache
        :raises OIDCDiscoveryError: If discovery fails
        :return: Discovery document dictionary
        """
        issuer_str = str(issuer_url).rstrip("/")
        cache_key = None

        # Try cache first
        if use_cache and self._redis:
            cache_key = self._redis.get_key(
                self.DISCOVERY_KEY_PREFIX
                + hashlib.sha256(issuer_str.encode()).hexdigest()
            )
            cached = self._redis.get(cache_key)
            if cached:
                try:
                    return cast(dict[str, Any], json.loads(cached))
                except json.JSONDecodeError:
                    self.log.warning(
                        f"Failed to decode cached discovery document for {issuer_str}"
                    )

        # Fetch discovery document
        discovery_url = f"{issuer_str}/.well-known/openid-configuration"
        self.log.info(f"Fetching OIDC discovery document from {discovery_url}")

        try:
            response = httpx.get(discovery_url, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            document = cast(dict[str, Any], response.json())

            # Validate required fields
            required_fields = [
                "issuer",
                "authorization_endpoint",
                "token_endpoint",
                "jwks_uri",
            ]
            missing_fields = [f for f in required_fields if f not in document]
            if missing_fields:
                raise OIDCDiscoveryError(
                    f"Discovery document missing required fields: {', '.join(missing_fields)}"
                )

            # Cache the result
            if use_cache and self._redis and cache_key:
                self._redis.set(
                    cache_key, json.dumps(document), ex=self.DISCOVERY_CACHE_TTL
                )

            return document

        except httpx.HTTPError as e:
            self.log.exception(f"HTTP error fetching discovery document: {e}")
            raise OIDCDiscoveryError(
                f"Failed to fetch discovery document from {discovery_url}: {str(e)}"
            ) from e
        except json.JSONDecodeError as e:
            self.log.exception("Failed to decode discovery document JSON")
            raise OIDCDiscoveryError(
                f"Invalid JSON in discovery document: {str(e)}"
            ) from e

    def fetch_jwks(self, jwks_uri: HttpUrl, use_cache: bool = True) -> dict[str, Any]:
        """Fetch JSON Web Key Set from provider.

        :param jwks_uri: JWKS endpoint URL
        :param use_cache: Whether to use/update Redis cache
        :raises OIDCUtilityError: If fetching fails
        :return: JWKS dictionary
        """
        jwks_str = str(jwks_uri)
        cache_key = None

        # Try cache first
        if use_cache and self._redis:
            cache_key = self._redis.get_key(
                self.JWKS_KEY_PREFIX + hashlib.sha256(jwks_str.encode()).hexdigest()
            )
            cached = self._redis.get(cache_key)
            if cached:
                try:
                    return cast(dict[str, Any], json.loads(cached))
                except json.JSONDecodeError:
                    self.log.warning(f"Failed to decode cached JWKS for {jwks_str}")

        # Fetch JWKS
        self.log.info(f"Fetching JWKS from {jwks_str}")

        try:
            response = httpx.get(jwks_str, timeout=30.0, follow_redirects=True)
            response.raise_for_status()
            jwks = cast(dict[str, Any], response.json())

            # Validate structure
            if "keys" not in jwks or not isinstance(jwks["keys"], list):
                raise OIDCUtilityError("JWKS must contain a 'keys' array")

            # Cache the result
            if use_cache and self._redis and cache_key:
                self._redis.set(cache_key, json.dumps(jwks), ex=self.JWKS_CACHE_TTL)

            return jwks

        except httpx.HTTPError as e:
            self.log.exception(f"HTTP error fetching JWKS: {e}")
            raise OIDCUtilityError(
                f"Failed to fetch JWKS from {jwks_uri}: {str(e)}"
            ) from e
        except json.JSONDecodeError as e:
            self.log.exception("Failed to decode JWKS JSON")
            raise OIDCUtilityError(f"Invalid JSON in JWKS: {str(e)}") from e

    def store_pkce(
        self,
        state_token: str,
        code_verifier: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Store PKCE code_verifier in Redis cache.

        :param state_token: State token to use as cache key
        :param code_verifier: PKCE code verifier to store
        :param metadata: Optional additional metadata to store
        """
        if not self._redis:
            raise OIDCUtilityError("Redis client required for PKCE storage")

        data = {
            "code_verifier": code_verifier,
            "timestamp": int(time.time()),
        }
        if metadata:
            data.update(metadata)

        cache_key = self._redis.get_key(self.PKCE_KEY_PREFIX + state_token)
        self._redis.set(cache_key, json.dumps(data), ex=self.PKCE_CACHE_TTL)
        self.log.debug(f"Stored PKCE for state: {state_token[:16]}...")

    def retrieve_pkce(
        self, state_token: str, delete: bool = True
    ) -> dict[str, Any] | None:
        """Retrieve PKCE code_verifier from Redis cache.

        :param state_token: State token used as cache key
        :param delete: Whether to delete the entry after retrieval (one-time use)
        :return: Dictionary with code_verifier and metadata, or None if not found
        """
        if not self._redis:
            raise OIDCUtilityError("Redis client required for PKCE retrieval")

        cache_key = self._redis.get_key(self.PKCE_KEY_PREFIX + state_token)
        cached = self._redis.get(cache_key)

        if cached:
            if delete:
                self._redis.delete(cache_key)
                self.log.debug(
                    f"Retrieved and deleted PKCE for state: {state_token[:16]}..."
                )
            else:
                self.log.debug(f"Retrieved PKCE for state: {state_token[:16]}...")

            try:
                return cast(dict[str, Any], json.loads(cached))
            except json.JSONDecodeError:
                self.log.warning(
                    f"Failed to decode cached PKCE data for state: {state_token[:16]}..."
                )
                return None

        self.log.warning(f"No PKCE found for state: {state_token[:16]}...")
        return None

    def store_logout_state(
        self,
        state_token: str,
        redirect_uri: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Store logout state in Redis cache.

        :param state_token: State token to use as cache key
        :param redirect_uri: Client redirect URI after logout
        :param metadata: Optional additional metadata to store
        """
        if not self._redis:
            raise OIDCUtilityError("Redis client required for logout state storage")

        data = {
            "redirect_uri": redirect_uri,
            "timestamp": int(time.time()),
        }
        if metadata:
            data.update(metadata)

        cache_key = self._redis.get_key(self.LOGOUT_STATE_KEY_PREFIX + state_token)
        self._redis.set(cache_key, json.dumps(data), ex=self.LOGOUT_STATE_CACHE_TTL)
        self.log.debug(f"Stored logout state: {state_token[:16]}...")

    def retrieve_logout_state(
        self, state_token: str, delete: bool = True
    ) -> dict[str, Any] | None:
        """Retrieve logout state from Redis cache.

        :param state_token: State token used as cache key
        :param delete: Whether to delete the entry after retrieval (one-time use)
        :return: Dictionary with redirect_uri and metadata, or None if not found
        """
        if not self._redis:
            raise OIDCUtilityError("Redis client required for logout state retrieval")

        cache_key = self._redis.get_key(self.LOGOUT_STATE_KEY_PREFIX + state_token)
        cached = self._redis.get(cache_key)

        if cached:
            if delete:
                self._redis.delete(cache_key)
                self.log.debug(
                    f"Retrieved and deleted logout state: {state_token[:16]}..."
                )
            else:
                self.log.debug(f"Retrieved logout state: {state_token[:16]}...")

            try:
                return cast(dict[str, Any], json.loads(cached))
            except json.JSONDecodeError:
                self.log.warning(
                    f"Failed to decode cached logout state: {state_token[:16]}..."
                )
                return None

        self.log.warning(f"No logout state found: {state_token[:16]}...")
        return None
