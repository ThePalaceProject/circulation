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

import json
from typing import Any, cast
from urllib.parse import urlencode

from pydantic import HttpUrl
from requests import RequestException

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
from palace.manager.util.http.exception import RequestNetworkException
from palace.manager.util.http.http import HTTP
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

    def _extract_error_detail_from_response(
        self, exception: RequestNetworkException
    ) -> str:
        """Extract error detail from a RequestNetworkException response.

        Attempts to parse OAuth 2.0 error response format from the exception's
        response attribute if available.

        :param exception: The RequestNetworkException to extract details from
        :return: Formatted error detail string (may be empty)
        """
        if not hasattr(exception, "response") or exception.response is None:
            return ""

        try:
            error_data = exception.response.json()
            error = error_data.get("error", "")
            error_description = error_data.get("error_description", "")
            return f": {error} - {error_description}"
        except json.JSONDecodeError:
            # If JSON parsing fails, return truncated response text
            return f": {exception.response.text[:200]}"

    def _handle_request_error(
        self,
        exception: RequestNetworkException | RequestException,
        operation: str,
        error_class: type[OIDCAuthenticationError],
    ) -> None:
        """Handle HTTP request errors consistently.

        :param exception: The exception that was raised
        :param operation: Description of the operation (e.g., "exchange authorization code")
        :param error_class: The exception class to raise
        :raises error_class: Always raises the specified error class
        """
        if isinstance(exception, RequestNetworkException):
            self.log.exception(f"Network error during {operation}")
            error_detail = self._extract_error_detail_from_response(exception)
            raise error_class(f"Failed to {operation}{error_detail}") from exception
        else:
            # RequestException
            self.log.exception(f"Request error during {operation}")
            raise error_class(
                f"Request error during {operation}: {str(exception)}"
            ) from exception

    def _prepare_token_endpoint_auth(
        self, data: dict[str, str]
    ) -> tuple[str, str] | None:
        """Prepare authentication for token endpoint requests.

        Modifies data dict in-place if using client_secret_post method.

        :param data: Request data dictionary to potentially modify
        :return: Auth tuple for HTTP Basic Auth, or None if using client_secret_post
        """
        if self._settings.token_endpoint_auth_method == "client_secret_basic":
            # HTTP Basic Auth
            return (self._settings.client_id, self._settings.client_secret)
        else:
            # client_secret_post (default)
            data["client_id"] = self._settings.client_id
            data["client_secret"] = self._settings.client_secret
            return None

    def _request_json_endpoint(
        self,
        url: str,
        operation: str,
        error_class: type[OIDCAuthenticationError],
        method: str = "POST",
        data: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        auth: tuple[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make an HTTP request to an endpoint and parse JSON response.

        :param url: Endpoint URL
        :param operation: Description of operation for error messages
        :param error_class: Exception class to raise on errors
        :param method: HTTP method (GET or POST)
        :param data: Request body data (for POST)
        :param headers: Additional headers
        :param auth: HTTP Basic Auth tuple
        :return: Parsed JSON response
        :raises error_class: On any error
        """
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)

        # Make HTTP request
        try:
            if method == "POST":
                response = HTTP.post_with_timeout(
                    url,
                    data=data or {},
                    auth=auth,
                    headers=request_headers,
                    allowed_response_codes=["2xx"],
                )
            else:  # GET
                response = HTTP.get_with_timeout(
                    url,
                    headers=request_headers,
                    allowed_response_codes=["2xx"],
                )
        except (RequestNetworkException, RequestException) as e:
            self._handle_request_error(e, operation, error_class)

        # Parse JSON response
        try:
            result = cast(dict[str, Any], response.json())
        except json.JSONDecodeError as e:
            self.log.exception("Failed to decode JSON response")
            raise error_class(f"Invalid JSON in response: {str(e)}") from e

        return result

    def _request_token_endpoint(
        self,
        token_endpoint: str,
        data: dict[str, str],
        operation: str,
        error_class: type[OIDCAuthenticationError],
    ) -> dict[str, Any]:
        """Make a POST request to the token endpoint and parse response.

        :param token_endpoint: Token endpoint URL
        :param data: Request body data
        :param operation: Description of operation for error messages
        :param error_class: Exception class to raise on errors
        :return: Parsed token response
        :raises error_class: On any error
        """
        # Prepare authentication
        auth = self._prepare_token_endpoint_auth(data)

        return self._request_json_endpoint(
            token_endpoint,
            operation,
            error_class,
            method="POST",
            data=data,
            auth=auth,
        )

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
                HttpUrl(self._settings.issuer_url), use_cache=use_cache
            )
            return self._metadata

        # Manual configuration
        self.log.info("Using manually configured OIDC endpoints")
        self._metadata = {
            "issuer": self._settings.issuer,
            "authorization_endpoint": self._settings.authorization_endpoint,
            "token_endpoint": self._settings.token_endpoint,
            "jwks_uri": self._settings.jwks_uri,
        }

        # Add optional userinfo endpoint
        if self._settings.userinfo_endpoint:
            self._metadata["userinfo_endpoint"] = self._settings.userinfo_endpoint

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

        # Exchange code for tokens
        self.log.info(f"Exchanging authorization code for tokens at {token_endpoint}")

        tokens = self._request_token_endpoint(
            token_endpoint, data, "exchange authorization code", OIDCTokenExchangeError
        )

        # Validate response contains required fields
        if "access_token" not in tokens:
            raise OIDCTokenExchangeError("Token response missing access_token")
        if "id_token" not in tokens:
            raise OIDCTokenExchangeError("Token response missing id_token")

        self.log.info("Successfully exchanged authorization code for tokens")
        return tokens

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

        # Get expected issuer from metadata
        expected_issuer = metadata["issuer"]

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

        # Request new tokens
        self.log.info("Refreshing access token")

        tokens = self._request_token_endpoint(
            token_endpoint, data, "refresh access token", OIDCRefreshTokenError
        )

        # Validate response contains required fields
        if "access_token" not in tokens:
            raise OIDCRefreshTokenError("Token response missing access_token")

        # Refresh response may include a new ID token
        self.log.info("Successfully refreshed access token")
        return tokens

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

        userinfo = self._request_json_endpoint(
            userinfo_endpoint,
            "fetch user info",
            OIDCAuthenticationError,
            method="GET",
            headers={"Authorization": f"Bearer {access_token}"},
        )

        self.log.debug("Successfully fetched user info")
        return userinfo

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
                expected_issuer = metadata["issuer"]
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
