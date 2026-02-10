"""Unit tests for OIDC authentication manager."""

from __future__ import annotations

import time
from unittest.mock import Mock, patch
from urllib.parse import quote

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

    @pytest.mark.parametrize(
        "use_pkce,scopes,access_type,code_challenge,expected_in_url,expected_not_in_url",
        [
            pytest.param(
                True,
                None,
                "offline",
                "test-challenge",
                [
                    "code_challenge=test-challenge",
                    "code_challenge_method=S256",
                    "access_type=offline",
                ],
                [],
                id="with-pkce",
            ),
            pytest.param(
                False,
                None,
                "offline",
                None,
                ["access_type=offline"],
                ["code_challenge", "code_challenge_method"],
                id="without-pkce",
            ),
            pytest.param(
                True,
                ["openid", "profile", "custom_scope"],
                "offline",
                None,
                ["scope=openid+profile+custom_scope"],
                [],
                id="custom-scopes",
            ),
            pytest.param(
                True,
                None,
                "online",
                None,
                ["access_type=online"],
                [],
                id="online-access",
            ),
        ],
    )
    def test_build_authorization_url(
        self,
        use_pkce,
        scopes,
        access_type,
        code_challenge,
        expected_in_url,
        expected_not_in_url,
        redis_fixture,
        mock_discovery_document,
    ):
        """Test authorization URL building with different configurations."""
        # Build settings with only non-None optional parameters
        settings_kwargs = {
            "issuer_url": TEST_ISSUER_URL,
            "client_id": TEST_CLIENT_ID,
            "client_secret": TEST_CLIENT_SECRET,
            "use_pkce": use_pkce,
            "access_type": access_type,
        }
        if scopes is not None:
            settings_kwargs["scopes"] = scopes

        settings = OIDCAuthSettings(**settings_kwargs)

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

            build_kwargs = {
                "redirect_uri": TEST_REDIRECT_URI,
                "state": state,
                "nonce": nonce,
            }
            if code_challenge:
                build_kwargs["code_challenge"] = code_challenge

            url = manager.build_authorization_url(**build_kwargs)

            # Common assertions
            assert "response_type=code" in url
            assert f"client_id={TEST_CLIENT_ID}" in url
            assert quote(TEST_REDIRECT_URI, safe="") in url
            assert f"state={state}" in url
            assert f"nonce={nonce}" in url

            # Check expected content
            for expected in expected_in_url:
                assert expected in url, f"Expected '{expected}' in URL"

            # Check expected not present
            for not_expected in expected_not_in_url:
                assert (
                    not_expected not in url
                ), f"Did not expect '{not_expected}' in URL"


class TestOIDCAuthenticationManagerTokenExchange:
    """Tests for authorization code exchange."""

    @pytest.mark.parametrize(
        "use_pkce,auth_method,pass_code_verifier,should_have_basic_auth",
        [
            pytest.param(True, "client_secret_post", True, False, id="with-pkce"),
            pytest.param(False, "client_secret_post", False, False, id="without-pkce"),
            pytest.param(
                True, "client_secret_basic", False, True, id="with-basic-auth"
            ),
        ],
    )
    def test_exchange_authorization_code(
        self,
        use_pkce,
        auth_method,
        pass_code_verifier,
        should_have_basic_auth,
        redis_fixture,
        mock_discovery_document,
        mock_token_response,
    ):
        """Test token exchange with different authentication and PKCE configurations."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            use_pkce=use_pkce,
            token_endpoint_auth_method=auth_method,
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

            exchange_kwargs = {
                "code": "test-auth-code",
                "redirect_uri": TEST_REDIRECT_URI,
            }
            if pass_code_verifier:
                exchange_kwargs["code_verifier"] = "test-verifier"

            tokens = manager.exchange_authorization_code(**exchange_kwargs)

            # Verify response
            assert tokens == mock_token_response
            assert tokens["access_token"] == mock_token_response["access_token"]
            assert tokens["id_token"] == mock_token_response["id_token"]
            mock_post.assert_called_once()

            # Verify POST call parameters
            call_args = mock_post.call_args

            if should_have_basic_auth:
                # Basic auth should be used
                assert call_args.kwargs["auth"] == (TEST_CLIENT_ID, TEST_CLIENT_SECRET)
                assert "client_id" not in call_args.kwargs["data"]
                assert "client_secret" not in call_args.kwargs["data"]
            else:
                # Credentials in POST data
                assert call_args.kwargs["data"]["client_id"] == TEST_CLIENT_ID
                assert call_args.kwargs["data"]["client_secret"] == TEST_CLIENT_SECRET

            # Verify PKCE
            if pass_code_verifier:
                assert call_args.kwargs["data"]["code_verifier"] == "test-verifier"
            else:
                assert "code_verifier" not in call_args.kwargs["data"]

    @pytest.mark.parametrize(
        "response_tokens,missing_field,error_message",
        [
            pytest.param(
                {"id_token": "test-id-token"},
                "access_token",
                "missing access_token",
                id="missing-access-token",
            ),
            pytest.param(
                {"access_token": "test-access-token"},
                "id_token",
                "missing id_token",
                id="missing-id-token",
            ),
        ],
    )
    def test_exchange_authorization_code_missing_token(
        self,
        response_tokens,
        missing_field,
        error_message,
        oidc_settings_with_discovery,
        redis_fixture,
        mock_discovery_document,
    ):
        """Test token exchange error when required token is missing."""
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

            # Mock token exchange with missing token
            mock_post_response = Mock()
            mock_post_response.json.return_value = response_tokens
            mock_post_response.raise_for_status = Mock()
            mock_post.return_value = mock_post_response

            with pytest.raises(OIDCTokenExchangeError, match=error_message):
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

    @pytest.mark.parametrize(
        "auth_method,should_have_basic_auth",
        [
            pytest.param("client_secret_post", False, id="post-auth"),
            pytest.param("client_secret_basic", True, id="basic-auth"),
        ],
    )
    def test_refresh_access_token_success(
        self,
        auth_method,
        should_have_basic_auth,
        redis_fixture,
        mock_discovery_document,
    ):
        """Test successful token refresh with different authentication methods."""
        settings = OIDCAuthSettings(
            issuer_url=TEST_ISSUER_URL,
            client_id=TEST_CLIENT_ID,
            client_secret=TEST_CLIENT_SECRET,
            token_endpoint_auth_method=auth_method,
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

            tokens = manager.refresh_access_token(refresh_token="test-refresh-token")

            assert tokens["access_token"] == "new-access-token"
            mock_post.assert_called_once()

            # Verify POST call parameters
            call_args = mock_post.call_args
            assert call_args.kwargs["data"]["grant_type"] == "refresh_token"
            assert call_args.kwargs["data"]["refresh_token"] == "test-refresh-token"

            if should_have_basic_auth:
                assert call_args.kwargs["auth"] == (TEST_CLIENT_ID, TEST_CLIENT_SECRET)
            else:
                # When not using basic auth, auth should be None or not present
                assert call_args.kwargs.get("auth") is None

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


class TestOIDCAuthenticationManagerLogout:
    """Tests for OIDC logout functionality."""

    @pytest.mark.parametrize(
        "state,use_custom_endpoint,expected_endpoint",
        [
            pytest.param(
                "test-state-token",
                False,
                "https://oidc.provider.test/logout",
                id="with-state",
            ),
            pytest.param(
                None,
                False,
                "https://oidc.provider.test/logout",
                id="without-state",
            ),
            pytest.param(
                None,
                True,
                "https://custom.logout.endpoint/logout",
                id="custom-endpoint",
            ),
        ],
    )
    def test_build_logout_url(
        self,
        state,
        use_custom_endpoint,
        expected_endpoint,
        redis_fixture,
        oidc_settings_with_discovery,
        mock_discovery_document,
    ):
        """Test logout URL building with different configurations."""
        if use_custom_endpoint:
            # Create settings with custom end_session_endpoint
            settings = OIDCAuthSettings(
                issuer_url=TEST_ISSUER_URL,
                client_id=TEST_CLIENT_ID,
                client_secret=TEST_CLIENT_SECRET,
                authorization_endpoint="https://custom.logout.endpoint/authorize",
                token_endpoint="https://custom.logout.endpoint/token",
                jwks_uri="https://custom.logout.endpoint/jwks",
                end_session_endpoint="https://custom.logout.endpoint/logout",
            )
        else:
            mock_discovery_document["end_session_endpoint"] = expected_endpoint
            settings = oidc_settings_with_discovery

        manager = OIDCAuthenticationManager(
            settings=settings,
            redis_client=redis_fixture.client,
        )

        id_token_hint = "test.id.token"
        post_logout_redirect_uri = "https://app.example.com/logout/callback"

        if use_custom_endpoint:
            # Mock get_provider_metadata for custom endpoint
            with patch.object(
                manager,
                "get_provider_metadata",
                return_value={
                    "issuer": TEST_ISSUER_URL,
                    "end_session_endpoint": expected_endpoint,
                },
            ):
                logout_url = manager.build_logout_url(
                    id_token_hint, post_logout_redirect_uri, state
                )
        else:
            with patch("httpx.get") as mock_get:
                mock_response = Mock()
                mock_response.json.return_value = mock_discovery_document
                mock_response.raise_for_status = Mock()
                mock_get.return_value = mock_response

                logout_url = manager.build_logout_url(
                    id_token_hint, post_logout_redirect_uri, state
                )

        # Common assertions
        assert expected_endpoint in logout_url
        assert f"id_token_hint={id_token_hint}" in logout_url
        assert f"post_logout_redirect_uri={post_logout_redirect_uri}" in logout_url

        # State assertions
        if state:
            assert f"state={state}" in logout_url
        else:
            assert "state=" not in logout_url

    def test_build_logout_url_not_supported(
        self, oidc_settings_with_discovery, redis_fixture, mock_discovery_document
    ):
        mock_discovery_document.pop("end_session_endpoint", None)

        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = mock_discovery_document
            mock_response.raise_for_status = Mock()
            mock_get.return_value = mock_response

            id_token_hint = "test.id.token"
            post_logout_redirect_uri = "https://app.example.com/logout/callback"

            with pytest.raises(
                OIDCAuthenticationError,
                match="does not support RP-Initiated Logout",
            ):
                manager.build_logout_url(id_token_hint, post_logout_redirect_uri)

    def test_validate_id_token_hint(
        self,
        oidc_settings_with_discovery,
        redis_fixture,
        mock_id_token,
        mock_discovery_document,
        mock_jwks,
    ):
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:
            # Mock both discovery and JWKS responses
            def mock_get_side_effect(url, **kwargs):
                response = Mock()
                response.raise_for_status = Mock()
                if "jwks" in str(url):
                    response.json.return_value = mock_jwks
                else:
                    response.json.return_value = mock_discovery_document
                return response

            mock_get.side_effect = mock_get_side_effect

            claims = manager.validate_id_token_hint(mock_id_token)

            assert claims["sub"] == "user123"
            assert claims["email"] == "testuser@example.com"


class TestOIDCAuthenticationManagerBackChannelLogout:
    """Tests for OIDC back-channel logout functionality."""

    def test_validate_logout_token_success(
        self,
        oidc_settings_with_discovery,
        redis_fixture,
        mock_logout_token,
        mock_discovery_document,
        mock_jwks,
    ):
        """Test successful validation of back-channel logout token."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        with patch("httpx.get") as mock_get:

            def mock_get_side_effect(url, **kwargs):
                response = Mock()
                response.raise_for_status = Mock()
                if "jwks" in str(url):
                    response.json.return_value = mock_jwks
                else:
                    response.json.return_value = mock_discovery_document
                return response

            mock_get.side_effect = mock_get_side_effect

            claims = manager.validate_logout_token(mock_logout_token)

            assert claims["sub"] == "user123"
            assert "events" in claims
            assert "jti" in claims
            assert "iat" in claims
            assert "nonce" not in claims

    @pytest.mark.parametrize(
        "claim_modification,error_match",
        [
            pytest.param(
                lambda claims: (claims.update({"nonce": "invalid-nonce"}), claims)[1],
                "must not contain 'nonce' claim",
                id="with-nonce",
            ),
            pytest.param(
                lambda claims: (claims.pop("events"), claims)[1],
                "missing 'events' claim",
                id="missing-events",
            ),
            pytest.param(
                lambda claims: (
                    claims.pop("sub"),
                    claims.pop("sid", None),
                    claims,
                )[2],
                "must contain either 'sub' or 'sid' claim",
                id="missing-sub-and-sid",
            ),
            pytest.param(
                lambda claims: (claims.pop("jti"), claims)[1],
                "missing 'jti' claim",
                id="missing-jti",
            ),
        ],
    )
    def test_validate_logout_token_validation_fails(
        self,
        claim_modification,
        error_match,
        oidc_settings_with_discovery,
        redis_fixture,
        oidc_test_keys,
        mock_logout_token_claims,
        mock_discovery_document,
        mock_jwks,
    ):
        """Test that invalid logout tokens are rejected."""
        manager = OIDCAuthenticationManager(
            settings=oidc_settings_with_discovery,
            redis_client=redis_fixture.client,
        )

        # Create invalid logout token with modified claims
        invalid_claims = mock_logout_token_claims.copy()
        claim_modification(invalid_claims)
        invalid_token = oidc_test_keys.sign_jwt(invalid_claims)

        with patch("httpx.get") as mock_get:

            def mock_get_side_effect(url, **kwargs):
                response = Mock()
                response.raise_for_status = Mock()
                if "jwks" in str(url):
                    response.json.return_value = mock_jwks
                else:
                    response.json.return_value = mock_discovery_document
                return response

            mock_get.side_effect = mock_get_side_effect

            with pytest.raises(OIDCAuthenticationError, match=error_match):
                manager.validate_logout_token(invalid_token)
