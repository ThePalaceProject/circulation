"""Unit tests for OIDC authentication manager."""

from __future__ import annotations

import time
from unittest.mock import Mock, patch

import httpx
import pytest

from palace.manager.integration.patron_auth.oidc.auth import (
    OIDCAuthenticationError,
    OIDCAuthenticationManager,
    OIDCAuthenticationManagerFactory,
    OIDCRefreshTokenError,
    OIDCTokenExchangeError,
)
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthSettings,
)

# Test constants
TEST_ISSUER_URL = "https://oidc.test.example.com"
TEST_CLIENT_ID = "test-client-id"
TEST_CLIENT_SECRET = "test-client-secret"
TEST_REDIRECT_URI = "https://cm.example.com/oidc_callback"
TEST_SECRET_KEY = "test-secret-key"


@pytest.fixture
def oidc_settings_with_discovery() -> OIDCAuthSettings:
    """OIDC settings configured for discovery."""
    return OIDCAuthSettings(
        issuer_url=TEST_ISSUER_URL,
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )


@pytest.fixture
def oidc_settings_manual() -> OIDCAuthSettings:
    """OIDC settings with manual endpoint configuration."""
    return OIDCAuthSettings(
        authorization_endpoint=f"{TEST_ISSUER_URL}/authorize",
        token_endpoint=f"{TEST_ISSUER_URL}/token",
        jwks_uri=f"{TEST_ISSUER_URL}/.well-known/jwks.json",
        userinfo_endpoint=f"{TEST_ISSUER_URL}/userinfo",
        client_id=TEST_CLIENT_ID,
        client_secret=TEST_CLIENT_SECRET,
    )


class TestOIDCAuthenticationManagerInit:
    """Tests for OIDCAuthenticationManager initialization."""

    def test_init_with_settings(self, oidc_settings_with_discovery, redis_fixture):
        """Test initialization with basic settings."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
            secret_key=TEST_SECRET_KEY,
        )

        assert manager._settings == oidc_settings_with_discovery
        assert manager._redis == redis_fixture.client
        assert manager._secret_key == TEST_SECRET_KEY
        assert manager._utility is not None
        assert manager._validator is not None
        assert manager._metadata is None

    def test_init_without_redis(self, oidc_settings_with_discovery):
        """Test initialization without Redis client."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            secret_key=TEST_SECRET_KEY,
        )

        assert manager._redis is None

    def test_init_without_secret_key(self, oidc_settings_with_discovery, redis_fixture):
        """Test initialization without secret key."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        assert manager._secret_key is None


class TestOIDCAuthenticationManagerMetadata:
    """Tests for provider metadata loading."""

    def test_get_provider_metadata_with_discovery(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test metadata loading via discovery."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            metadata = manager.get_provider_metadata()

            assert metadata == mock_discovery_document
            assert metadata["issuer"] == TEST_ISSUER_URL
            mock_get.assert_called_once()

    def test_get_provider_metadata_with_manual_config(
        self, oidc_settings_manual, redis_fixture
    ):
        """Test metadata loading from manual configuration."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_manual,
            redis_client=redis_fixture.client,
        )

        metadata = manager.get_provider_metadata()

        assert metadata["authorization_endpoint"] == f"{TEST_ISSUER_URL}/authorize"
        assert metadata["token_endpoint"] == f"{TEST_ISSUER_URL}/token"
        assert metadata["jwks_uri"] == f"{TEST_ISSUER_URL}/.well-known/jwks.json"
        assert metadata["userinfo_endpoint"] == f"{TEST_ISSUER_URL}/userinfo"
        assert metadata["issuer"] == "manual"

    def test_get_provider_metadata_caching(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test that metadata is cached after first fetch."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            # First call
            metadata1 = manager.get_provider_metadata()
            # Second call (should use cached)
            metadata2 = manager.get_provider_metadata()

            assert metadata1 == metadata2
            # httpx.get should only be called once due to caching
            mock_get.assert_called_once()

    def test_get_provider_metadata_bypass_cache(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test bypassing metadata cache."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            # First call with cache
            manager.get_provider_metadata(use_cache=True)
            # Second call bypassing cache
            manager.get_provider_metadata(use_cache=False)

            # httpx.get should be called twice
            assert mock_get.call_count == 2


class TestOIDCAuthenticationManagerAuthorizationURL:
    """Tests for authorization URL building."""

    def test_build_authorization_url_with_pkce(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test authorization URL building with PKCE enabled."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            state = "test-state"
            nonce = "test-nonce"
            code_challenge = "test-challenge"

            url = manager.build_authorization_url(
                redirect_uri=TEST_REDIRECT_URI,
                state=state,
                nonce=nonce,
                code_challenge=code_challenge,
            )

            from urllib.parse import quote

            assert "response_type=code" in url
            assert f"client_id={TEST_CLIENT_ID}" in url
            assert quote(TEST_REDIRECT_URI, safe="") in url
            assert "scope=openid" in url
            assert f"state={state}" in url
            assert f"nonce={nonce}" in url
            assert f"code_challenge={code_challenge}" in url
            assert "code_challenge_method=S256" in url
            assert "access_type=offline" in url

    def test_build_authorization_url_without_pkce(
        self, redis_fixture, mock_discovery_document
    ):
        """Test authorization URL building with PKCE disabled."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            use_pkce=False,
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            state = "test-state"
            nonce = "test-nonce"

            url = manager.build_authorization_url(
                redirect_uri=TEST_REDIRECT_URI,
                state=state,
                nonce=nonce,
            )

            assert "code_challenge" not in url
            assert "code_challenge_method" not in url

    def test_build_authorization_url_custom_scopes(
        self, redis_fixture, mock_discovery_document
    ):
        """Test authorization URL with custom scopes."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            scopes=["openid", "profile", "custom_scope"],
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            url = manager.build_authorization_url(
                redirect_uri=TEST_REDIRECT_URI,
                state="state",
                nonce="nonce",
            )

            assert "scope=openid+profile+custom_scope" in url

    def test_build_authorization_url_online_access(
        self, redis_fixture, mock_discovery_document
    ):
        """Test authorization URL with online access type."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            access_type="online",
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            url = manager.build_authorization_url(
                redirect_uri=TEST_REDIRECT_URI,
                state="state",
                nonce="nonce",
            )

            assert "access_type=online" in url


class TestOIDCAuthenticationManagerTokenExchange:
    """Tests for authorization code exchange."""

    def test_exchange_authorization_code_success(
        self,
        oidc_settings_with_discovery,
        redis_fixture,
        mock_discovery_document,
        mock_token_response,
    ):
        """Test successful token exchange."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token exchange
            mock_post_response = Mock()
            mock_post_response.json.return_value = mock_token_response
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            tokens = manager.exchange_authorization_code(
                code="test-auth-code",
                redirect_uri=TEST_REDIRECT_URI,
                code_verifier="test-verifier",
            )

            assert tokens == mock_token_response
            assert tokens["access_token"] == mock_token_response["access_token"]
            assert tokens["id_token"] == mock_token_response["id_token"]
            mock_post.assert_called_once()

            # Verify POST call parameters
            call_args = mock_post.call_args
            assert call_args.kwargs["data"]["grant_type"] == "authorization_code"
            assert call_args.kwargs["data"]["code"] == "test-auth-code"
            assert call_args.kwargs["data"]["code_verifier"] == "test-verifier"
            assert call_args.kwargs["data"]["client_id"] == TEST_CLIENT_ID
            assert call_args.kwargs["data"]["client_secret"] == TEST_CLIENT_SECRET

    def test_exchange_authorization_code_with_basic_auth(
        self, redis_fixture, mock_discovery_document, mock_token_response
    ):
        """Test token exchange with client_secret_basic authentication."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            token_endpoint_auth_method="client_secret_basic",
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token exchange
            mock_post_response = Mock()
            mock_post_response.json.return_value = mock_token_response
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            manager.exchange_authorization_code(
                code="test-auth-code",
                redirect_uri=TEST_REDIRECT_URI,
            )

            # Verify Basic Auth was used
            call_args = mock_post.call_args
            assert call_args.kwargs["auth"] == (TEST_CLIENT_ID, TEST_CLIENT_SECRET)
            assert "client_id" not in call_args.kwargs["data"]
            assert "client_secret" not in call_args.kwargs["data"]

    def test_exchange_authorization_code_without_pkce(
        self, redis_fixture, mock_discovery_document, mock_token_response
    ):
        """Test token exchange without PKCE."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            use_pkce=False,
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token exchange
            mock_post_response = Mock()
            mock_post_response.json.return_value = mock_token_response
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            manager.exchange_authorization_code(
                code="test-auth-code",
                redirect_uri=TEST_REDIRECT_URI,
            )

            # Verify no code_verifier in request
            call_args = mock_post.call_args
            assert "code_verifier" not in call_args.kwargs["data"]

    def test_exchange_authorization_code_missing_access_token(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test token exchange error when access_token is missing."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token exchange with missing access_token
            mock_post_response = Mock()
            mock_post_response.json.return_value = {"id_token": "test-id-token"}
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            with pytest.raises(OIDCTokenExchangeError, match="missing access_token"):
                manager.exchange_authorization_code(
                    code="test-auth-code",
                    redirect_uri=TEST_REDIRECT_URI,
                )

    def test_exchange_authorization_code_missing_id_token(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test token exchange error when id_token is missing."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token exchange with missing id_token
            mock_post_response = Mock()
            mock_post_response.json.return_value = {"access_token": "test-access-token"}
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            with pytest.raises(OIDCTokenExchangeError, match="missing id_token"):
                manager.exchange_authorization_code(
                    code="test-auth-code",
                    redirect_uri=TEST_REDIRECT_URI,
                )

    def test_exchange_authorization_code_http_error(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test token exchange error handling for HTTP errors."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock HTTP error
            mock_post.side_effect = httpx.HTTPStatusError(
                "400 Bad Request",
                request=Mock(),
                response=Mock(status_code=400, text="Invalid grant"),
            )

            with pytest.raises(OIDCTokenExchangeError):
                manager.exchange_authorization_code(
                    code="test-auth-code",
                    redirect_uri=TEST_REDIRECT_URI,
                )


class TestOIDCAuthenticationManagerTokenValidation:
    """Tests for ID token validation."""

    def test_validate_id_token_success(
        self,
        oidc_settings_with_discovery,
        redis_fixture,
        mock_discovery_document,
        mock_jwks,
        mock_id_token,
        mock_id_token_claims,
    ):
        """Test successful ID token validation."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            # Mock discovery and JWKS
            def mock_get_handler(url, **kwargs):
                mock_response = Mock()
                if ".well-known/openid-configuration" in str(url):
                    mock_response.json.return_value = mock_discovery_document
                elif "jwks" in str(url):
                    mock_response.json.return_value = mock_jwks
                mock_response.raise_for_status = Mock()
                return mock_response

            mock_get.side_effect = mock_get_handler

            claims = manager.validate_id_token(
                id_token=mock_id_token,
                nonce="test-nonce-abc123",
            )

            assert claims["sub"] == mock_id_token_claims["sub"]
            assert claims["iss"] == mock_id_token_claims["iss"]
            assert claims["aud"] == mock_id_token_claims["aud"]

    def test_validate_id_token_with_manual_config(
        self, redis_fixture, mock_jwks, oidc_test_keys
    ):
        """Test ID token validation with manual endpoint configuration."""
        # Create settings with manual config
        settings = OIDCAuthSettings(
            authorization_endpoint=f"{TEST_ISSUER_URL}/authorize",
            token_endpoint=f"{TEST_ISSUER_URL}/token",
            jwks_uri=f"{TEST_ISSUER_URL}/.well-known/jwks.json",
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        # Create ID token with issuer="manual" to match what the manager expects
        now = int(time.time())
        claims = {
            "iss": "manual",  # This matches what the manager sets for manual config
            "sub": "user123",
            "aud": TEST_CLIENT_ID,
            "exp": now + 3600,
            "iat": now,
            "nonce": "test-nonce-abc123",
        }
        id_token = oidc_test_keys.sign_jwt(claims)

        with patch("httpx.get") as mock_get:
            # Mock JWKS only (no discovery)
            mock_response = Mock()
            mock_response.json.return_value = mock_jwks
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            validated_claims = manager.validate_id_token(
                id_token=id_token,
                nonce="test-nonce-abc123",
            )

            assert validated_claims["sub"] == "user123"


class TestOIDCAuthenticationManagerTokenRefresh:
    """Tests for token refresh."""

    def test_refresh_access_token_success(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test successful token refresh."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        refresh_response = {
            "access_token": "new-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token refresh
            mock_post_response = Mock()
            mock_post_response.json.return_value = refresh_response
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            tokens = manager.refresh_access_token(refresh_token="test-refresh-token")

            assert tokens["access_token"] == "new-access-token"
            mock_post.assert_called_once()

            # Verify POST call parameters
            call_args = mock_post.call_args
            assert call_args.kwargs["data"]["grant_type"] == "refresh_token"
            assert call_args.kwargs["data"]["refresh_token"] == "test-refresh-token"

    def test_refresh_access_token_with_basic_auth(
        self, redis_fixture, mock_discovery_document
    ):
        """Test token refresh with client_secret_basic authentication."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            token_endpoint_auth_method="client_secret_basic",
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        refresh_response = {
            "access_token": "new-access-token",
            "token_type": "Bearer",
            "expires_in": 3600,
        }

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock token refresh
            mock_post_response = Mock()
            mock_post_response.json.return_value = refresh_response
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            manager.refresh_access_token(refresh_token="test-refresh-token")

            # Verify Basic Auth was used
            call_args = mock_post.call_args
            assert call_args.kwargs["auth"] == (TEST_CLIENT_ID, TEST_CLIENT_SECRET)

    def test_refresh_access_token_missing_access_token(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test refresh error when access_token is missing."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock refresh with missing access_token
            mock_post_response = Mock()
            mock_post_response.json.return_value = {"token_type": "Bearer"}
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            with pytest.raises(OIDCRefreshTokenError, match="missing access_token"):
                manager.refresh_access_token(refresh_token="test-refresh-token")

    def test_refresh_access_token_http_error(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test refresh error handling for HTTP errors."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get, patch("httpx.post") as mock_post:
            # Mock discovery
            mock_get_response = Mock()
            mock_get_response.json.return_value = mock_discovery_document
            mock_get_response.raise_for_status = Mock()
            mock_get.return_value = mock_get_response

            # Mock HTTP error
            mock_post.side_effect = httpx.HTTPStatusError(
                "400 Bad Request",
                request=Mock(),
                response=Mock(status_code=400, text="Invalid refresh token"),
            )

            with pytest.raises(OIDCRefreshTokenError):
                manager.refresh_access_token(refresh_token="test-refresh-token")


class TestOIDCAuthenticationManagerUserInfo:
    """Tests for UserInfo endpoint."""

    def test_fetch_userinfo_success(
        self,
        oidc_settings_with_discovery,
        redis_fixture,
        mock_discovery_document,
        mock_userinfo_response,
    ):
        """Test successful UserInfo fetch."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            # Track call count
            call_count = 0

            def mock_get_handler(url, **kwargs):
                nonlocal call_count
                call_count += 1
                mock_response = Mock()

                if ".well-known/openid-configuration" in str(url):
                    mock_response.json.return_value = mock_discovery_document
                else:
                    # UserInfo endpoint
                    mock_response.json.return_value = mock_userinfo_response

                mock_response.raise_for_status = Mock()
                return mock_response

            mock_get.side_effect = mock_get_handler

            userinfo = manager.fetch_userinfo(access_token="test-access-token")

            assert userinfo == mock_userinfo_response
            assert userinfo["sub"] == "user123"
            assert userinfo["email"] == "testuser@example.com"

    def test_fetch_userinfo_missing_endpoint(self, oidc_settings_manual, redis_fixture):
        """Test UserInfo fetch when endpoint is not configured."""
        # Create settings without userinfo_endpoint
        settings = OIDCAuthSettings(
            authorization_endpoint=f"{TEST_ISSUER_URL}/authorize",
            token_endpoint=f"{TEST_ISSUER_URL}/token",
            jwks_uri=f"{TEST_ISSUER_URL}/.well-known/jwks.json",
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
        )

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        with pytest.raises(
            OIDCAuthenticationError, match="does not support UserInfo endpoint"
        ):
            manager.fetch_userinfo(access_token="test-access-token")

    def test_fetch_userinfo_http_error(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        """Test UserInfo fetch error handling."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            call_count = 0

            def mock_get_handler(url, **kwargs):
                nonlocal call_count
                call_count += 1

                if call_count == 1:
                    # First call: discovery
                    mock_response = Mock()
                    mock_response.json.return_value = mock_discovery_document
                    mock_response.raise_for_status = Mock()
                    return mock_response
                else:
                    # Second call: UserInfo with error
                    raise httpx.HTTPStatusError(
                        "401 Unauthorized",
                        request=Mock(),
                        response=Mock(status_code=401),
                    )

            mock_get.side_effect = mock_get_handler

            with pytest.raises(OIDCAuthenticationError):
                manager.fetch_userinfo(access_token="invalid-token")


class TestOIDCAuthenticationManagerFactory:
    """Tests for OIDCAuthenticationManagerFactory."""

    def test_factory_create(self, oidc_settings_with_discovery, redis_fixture):
        """Test factory creates manager instance."""
        manager = OIDCAuthenticationManagerFactory.create(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
            secret_key=TEST_SECRET_KEY,
        )

        assert isinstance(manager, OIDCAuthenticationManager)
        assert manager._settings == oidc_settings_with_discovery
        assert manager._redis == redis_fixture.client
        assert manager._secret_key == TEST_SECRET_KEY

    def test_factory_create_minimal(self, oidc_settings_with_discovery):
        """Test factory with minimal parameters."""
        manager = OIDCAuthenticationManagerFactory.create(
            settings=oidc_settings_with_discovery
        )

        assert isinstance(manager, OIDCAuthenticationManager)
        assert manager._redis is None
        assert manager._secret_key is None
