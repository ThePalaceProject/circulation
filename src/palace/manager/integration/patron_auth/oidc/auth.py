"""OIDC Authentication Manager.

This module provides the core OIDC authentication flow management including:
- Provider metadata loading (discovery)
- Authorization URL building with PKCE
- Token exchange (authorization code -> tokens)
- ID token validation
- Refresh token handling
- UserInfo endpoint calls (optional)
"""

from __future__ import annotations

from typing import Any, cast
from urllib.parse import urlencode

import httpx
from pydantic import HttpUrl

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.util import (
    OIDCUtility,
)
from palace.manager.integration.patron_auth.oidc.validator import (
    OIDCTokenClaimsError,
    OIDCTokenValidator,
)
from palace.manager.service.redis.redis import Redis
from palace.manager.util.log import LoggerMixin


class OIDCAuthenticationError(BasePalaceException):
    """Base exception for OIDC authentication errors."""


class OIDCTokenExchangeError(OIDCAuthenticationError):
    """Raised when token exchange fails."""


class OIDCRefreshTokenError(OIDCAuthenticationError):
    """Raised when token refresh fails."""


class OIDCAuthenticationManager(LoggerMixin):
    """Manages OIDC authentication flow.

    Handles the complete OAuth 2.0 / OIDC authentication flow including:
    - Discovery of provider endpoints
    - Authorization URL generation with PKCE
    - Authorization code exchange for tokens
    - ID token validation
    - Token refresh
    """

    def __init__(
        self,
        settings: OIDCAuthSettings,
        redis_client: Redis | None = None,
        secret_key: str | None = None,
    ):
        """Initialize OIDC authentication manager.

        :param settings: OIDC authentication settings
        :param redis_client: Optional Redis client for caching
        :param secret_key: Secret key for state generation (required for auth flow)
        """
        self._settings = settings
        self._redis = redis_client
        self._secret_key = secret_key
        self._utility = OIDCUtility(redis_client)
        self._validator = OIDCTokenValidator()
        self._metadata: dict[str, Any] | None = None

    def get_provider_metadata(self, use_cache: bool = True) -> dict[str, Any]:
        """Get OIDC provider metadata.

        Uses discovery if issuer_url is configured, otherwise constructs
        metadata from manually configured endpoints.

        :param use_cache: Whether to use cached metadata
        :return: Provider metadata dictionary
        """
        # Return cached metadata if available
        if self._metadata is not None and use_cache:
            return self._metadata

        # Auto-discover if issuer_url provided
        if self._settings.issuer_url:
            self.log.info(
                f"Discovering OIDC provider metadata from {self._settings.issuer_url}"
            )
            self._metadata = self._utility.discover_oidc_configuration(
                self._settings.issuer_url, use_cache=use_cache
            )
            return self._metadata

        # Manual configuration
        self.log.info("Using manually configured OIDC endpoints")
        self._metadata = {
            "issuer": (
                str(self._settings.issuer_url)
                if self._settings.issuer_url
                else "manual"
            ),
            "authorization_endpoint": str(self._settings.authorization_endpoint),
            "token_endpoint": str(self._settings.token_endpoint),
            "jwks_uri": str(self._settings.jwks_uri),
        }

        # Add optional userinfo endpoint
        if self._settings.userinfo_endpoint:
            self._metadata["userinfo_endpoint"] = str(self._settings.userinfo_endpoint)

        return self._metadata

    def build_authorization_url(
        self,
        redirect_uri: str,
        state: str,
        nonce: str,
        code_challenge: str | None = None,
    ) -> str:
        """Build authorization URL with parameters.

        :param redirect_uri: Callback URI for authorization response
        :param state: State parameter for CSRF protection
        :param nonce: Nonce for ID token validation
        :param code_challenge: PKCE code challenge (if PKCE enabled)
        :return: Complete authorization URL
        """
        metadata = self.get_provider_metadata()
        auth_endpoint = metadata["authorization_endpoint"]

        # Build query parameters
        params = {
            "response_type": "code",
            "client_id": self._settings.client_id,
            "redirect_uri": redirect_uri,
            "scope": " ".join(self._settings.scopes),
            "state": state,
            "nonce": nonce,
        }

        # Add PKCE parameters if enabled
        if self._settings.use_pkce and code_challenge:
            params["code_challenge"] = code_challenge
            params["code_challenge_method"] = "S256"

        # Add access_type for refresh token support
        if self._settings.access_type:
            params["access_type"] = self._settings.access_type

        # Construct URL
        auth_url = f"{auth_endpoint}?{urlencode(params)}"
        self.log.debug(f"Built authorization URL: {auth_endpoint}?...")
        return auth_url

    def exchange_authorization_code(
        self,
        code: str,
        redirect_uri: str,
        code_verifier: str | None = None,
    ) -> dict[str, Any]:
        """Exchange authorization code for tokens.

        :param code: Authorization code from provider
        :param redirect_uri: Redirect URI used in authorization request
        :param code_verifier: PKCE code verifier (if PKCE enabled)
        :raises OIDCTokenExchangeError: If token exchange fails
        :return: Token response dictionary with access_token, id_token, etc.
        """
        metadata = self.get_provider_metadata()
        token_endpoint = metadata["token_endpoint"]

        # Build request body
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
        }

        # Add PKCE verifier if provided
        if code_verifier:
            data["code_verifier"] = code_verifier

        # Prepare authentication
        auth = None
        if self._settings.token_endpoint_auth_method == "client_secret_basic":
            # HTTP Basic Auth
            auth = (self._settings.client_id, self._settings.client_secret)
        else:
            # client_secret_post (default)
            data["client_id"] = self._settings.client_id
            data["client_secret"] = self._settings.client_secret

        # Exchange code for tokens
        self.log.info(f"Exchanging authorization code for tokens at {token_endpoint}")

        try:
            response = httpx.post(
                token_endpoint,
                data=data,
                auth=auth,
                headers={"Accept": "application/json"},
                timeout=30.0,
            )
            response.raise_for_status()
            tokens = cast(dict[str, Any], response.json())

            # Validate response
            if "access_token" not in tokens:
                raise OIDCTokenExchangeError("Token response missing access_token")
            if "id_token" not in tokens:
                raise OIDCTokenExchangeError("Token response missing id_token")

            self.log.info("Successfully exchanged authorization code for tokens")
            return tokens

        except httpx.HTTPError as e:
            self.log.exception("HTTP error during token exchange")
            error_detail = ""
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_data = e.response.json()
                    error_detail = f": {error_data.get('error', '')} - {error_data.get('error_description', '')}"
                except Exception:
                    error_detail = f": {e.response.text[:200]}"

            raise OIDCTokenExchangeError(
                f"Failed to exchange authorization code{error_detail}"
            ) from e
        except Exception as e:
            self.log.exception("Unexpected error during token exchange")
            raise OIDCTokenExchangeError(
                f"Unexpected error during token exchange: {str(e)}"
            ) from e

    def validate_id_token(
        self, id_token: str, nonce: str | None = None
    ) -> dict[str, Any]:
        """Validate ID token signature and claims.

        :param id_token: Raw ID token (JWT)
        :param nonce: Expected nonce value
        :raises OIDCTokenValidationError: If validation fails
        :return: Validated claims dictionary
        """
        metadata = self.get_provider_metadata()
        jwks_uri = metadata["jwks_uri"]

        # Fetch JWKS
        jwks = self._utility.fetch_jwks(HttpUrl(jwks_uri))

        # Determine expected issuer
        # Use issuer from metadata if available, otherwise use issuer_url or "manual"
        expected_issuer = metadata.get(
            "issuer", str(self._settings.issuer_url or "manual")
        )

        # Validate signature and claims
        claims = self._validator.validate_signature(id_token, jwks)
        self._validator.validate_claims(
            claims,
            expected_issuer=expected_issuer,
            expected_audience=self._settings.client_id,
            nonce=nonce,
        )

        return claims

    def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        """Refresh access token using refresh token.

        :param refresh_token: Refresh token from previous token response
        :raises OIDCRefreshTokenError: If refresh fails
        :return: New token response dictionary
        """
        metadata = self.get_provider_metadata()
        token_endpoint = metadata["token_endpoint"]

        # Build request body
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        # Prepare authentication
        auth = None
        if self._settings.token_endpoint_auth_method == "client_secret_basic":
            # HTTP Basic Auth
            auth = (self._settings.client_id, self._settings.client_secret)
        else:
            # client_secret_post (default)
            data["client_id"] = self._settings.client_id
            data["client_secret"] = self._settings.client_secret

        # Request new tokens
        self.log.info("Refreshing access token")

        try:
            response = httpx.post(
                token_endpoint,
                data=data,
                auth=auth,
                headers={"Accept": "application/json"},
                timeout=30.0,
            )
            response.raise_for_status()
            tokens = cast(dict[str, Any], response.json())

            # Validate response
            if "access_token" not in tokens:
                raise OIDCRefreshTokenError("Token response missing access_token")

            # Refresh response may include a new ID token
            self.log.info("Successfully refreshed access token")
            return tokens

        except httpx.HTTPError as e:
            self.log.exception("HTTP error during token refresh")
            error_detail = ""
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_data = e.response.json()
                    error_detail = f": {error_data.get('error', '')} - {error_data.get('error_description', '')}"
                except Exception:
                    error_detail = f": {e.response.text[:200]}"

            raise OIDCRefreshTokenError(
                f"Failed to refresh access token{error_detail}"
            ) from e
        except Exception as e:
            self.log.exception("Unexpected error during token refresh")
            raise OIDCRefreshTokenError(
                f"Unexpected error during token refresh: {str(e)}"
            ) from e

    def fetch_userinfo(self, access_token: str) -> dict[str, Any]:
        """Fetch user info from UserInfo endpoint.

        Optional operation to get additional user claims beyond ID token.

        :param access_token: Access token from token response
        :raises OIDCAuthenticationError: If fetch fails
        :return: UserInfo response dictionary
        """
        metadata = self.get_provider_metadata()
        userinfo_endpoint = metadata.get("userinfo_endpoint")

        if not userinfo_endpoint:
            raise OIDCAuthenticationError("Provider does not support UserInfo endpoint")

        self.log.info(f"Fetching user info from {userinfo_endpoint}")

        try:
            response = httpx.get(
                userinfo_endpoint,
                headers={"Authorization": f"Bearer {access_token}"},
                timeout=30.0,
            )
            response.raise_for_status()
            userinfo = cast(dict[str, Any], response.json())

            self.log.debug("Successfully fetched user info")
            return userinfo

        except httpx.HTTPError as e:
            self.log.exception("HTTP error fetching user info")
            raise OIDCAuthenticationError(f"Failed to fetch user info: {str(e)}") from e
        except Exception as e:
            self.log.exception("Unexpected error fetching user info")
            raise OIDCAuthenticationError(
                f"Unexpected error fetching user info: {str(e)}"
            ) from e

    def build_logout_url(
        self,
        id_token_hint: str,
        post_logout_redirect_uri: str,
        state: str | None = None,
    ) -> str:
        """Build logout URL for RP-Initiated Logout.

        :param id_token_hint: ID token from authentication
        :param post_logout_redirect_uri: Client callback URI after logout
        :param state: Optional state parameter for logout callback
        :raises OIDCAuthenticationError: If end_session_endpoint not available
        :return: Complete logout URL
        """
        metadata = self.get_provider_metadata()
        end_session_endpoint = metadata.get("end_session_endpoint")

        if not end_session_endpoint:
            if self._settings.end_session_endpoint:
                end_session_endpoint = str(self._settings.end_session_endpoint)
            else:
                raise OIDCAuthenticationError(
                    "Provider does not support RP-Initiated Logout (no end_session_endpoint)"
                )

        params = {
            "id_token_hint": id_token_hint,
            "post_logout_redirect_uri": post_logout_redirect_uri,
        }

        if state:
            params["state"] = state

        query_string = urlencode(params)
        logout_url = f"{end_session_endpoint}?{query_string}"

        self.log.info(f"Built logout URL for provider: {end_session_endpoint}")
        return logout_url

    def validate_id_token_hint(self, id_token: str) -> dict[str, Any]:
        """Validate ID token hint for logout.

        Similar to ID token validation but without nonce requirement.

        :param id_token: ID token to validate
        :raises OIDCAuthenticationError: If validation fails
        :return: Decoded ID token claims
        """
        return self.validate_id_token(id_token, nonce=None)

    def validate_logout_token(self, logout_token: str) -> dict[str, Any]:
        """Validate OIDC back-channel logout token.

        Logout tokens are similar to ID tokens but with specific requirements:
        - Must NOT contain 'nonce' claim
        - Must contain 'events' claim with back-channel logout event
        - Must contain either 'sub' or 'sid' claim
        - Must contain 'iat', 'jti' claims

        :param logout_token: Logout token JWT from provider
        :raises OIDCAuthenticationError: If validation fails
        :return: Decoded logout token claims
        """
        try:
            # Try to validate as ID token (requires 'sub')
            claims = self.validate_id_token(logout_token, nonce=None)
        except OIDCTokenClaimsError as e:
            # If validation failed due to missing 'sub', validate signature only and check for 'sid'
            if "Missing required claim: 'sub'" in str(e):
                # Get JWKS and validate signature
                metadata = self.get_provider_metadata()
                jwks_uri = metadata["jwks_uri"]
                jwks = self._utility.fetch_jwks(HttpUrl(jwks_uri))

                # Validate signature only (this decodes the token)
                claims = self._validator.validate_signature(logout_token, jwks)

                # Check if 'sid' is present
                if "sid" not in claims:
                    # Neither 'sub' nor 'sid' present
                    raise OIDCAuthenticationError(
                        "Logout token must contain either 'sub' or 'sid' claim"
                    ) from e

                # Manually validate the required claims (without requiring 'sub')
                expected_issuer = metadata.get(
                    "issuer", str(self._settings.issuer_url or "manual")
                )
                if claims.get("iss") != expected_issuer:
                    raise OIDCAuthenticationError(
                        f"Invalid issuer: expected {expected_issuer}, got {claims.get('iss')}"
                    ) from e
                if claims.get("aud") != self._settings.client_id:
                    raise OIDCAuthenticationError(
                        f"Invalid audience: expected {self._settings.client_id}, got {claims.get('aud')}"
                    ) from e
            else:
                # Other validation error, re-raise
                raise OIDCAuthenticationError(f"Invalid logout token: {e}") from e

        # Additional validation for logout tokens
        if "nonce" in claims:
            raise OIDCAuthenticationError("Logout token must not contain 'nonce' claim")

        events = claims.get("events")
        if not events:
            raise OIDCAuthenticationError("Logout token missing 'events' claim")

        # Check for back-channel logout event
        backchannel_logout_event = "http://schemas.openid.net/event/backchannel-logout"
        if backchannel_logout_event not in events:
            raise OIDCAuthenticationError(
                f"Logout token missing '{backchannel_logout_event}' event"
            )

        # Must contain either 'sub' or 'sid'
        if "sub" not in claims and "sid" not in claims:
            raise OIDCAuthenticationError(
                "Logout token must contain either 'sub' or 'sid' claim"
            )

        # Must contain 'iat' and 'jti'
        if "iat" not in claims:
            raise OIDCAuthenticationError("Logout token missing 'iat' claim")

        if "jti" not in claims:
            raise OIDCAuthenticationError("Logout token missing 'jti' claim")

        return claims
