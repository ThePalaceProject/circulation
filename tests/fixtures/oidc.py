"""Test fixtures for OIDC authentication testing."""

from __future__ import annotations

import time
from base64 import urlsafe_b64encode
from typing import Any

import jwt
import pytest
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# RSA key parameters
RSA_KEY_SIZE = 2048
RSA_PUBLIC_EXPONENT = 65537

# Token expiry times (seconds)
ID_TOKEN_EXPIRY_SECONDS = 3600  # 1 hour
LOGOUT_TOKEN_EXPIRY_SECONDS = 120  # 2 minutes
ACCESS_TOKEN_EXPIRY_SECONDS = 3600  # 1 hour

# Test key identifier
TEST_KEY_ID = "test-key-id-1"


class OIDCTestKeys:
    """RSA key pair for signing and validating test ID tokens."""

    def __init__(self):
        """Generate a new RSA key pair for testing."""
        self.private_key = rsa.generate_private_key(
            public_exponent=RSA_PUBLIC_EXPONENT,
            key_size=RSA_KEY_SIZE,
            backend=default_backend(),
        )
        self.public_key = self.private_key.public_key()

        self.private_key_pem = self.private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode("utf-8")

        self.public_key_pem = self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode("utf-8")

        public_numbers = self.public_key.public_numbers()

        def _int_to_base64url(n: int) -> str:
            byte_length = (n.bit_length() + 7) // 8
            n_bytes = n.to_bytes(byte_length, byteorder="big")
            return urlsafe_b64encode(n_bytes).rstrip(b"=").decode("utf-8")

        self.n = _int_to_base64url(public_numbers.n)
        self.e = _int_to_base64url(public_numbers.e)
        self.kid = TEST_KEY_ID

    def sign_jwt(self, payload: dict[str, Any], algorithm: str = "RS256") -> str:
        """Sign a JWT payload with the private key.

        :param payload: JWT payload claims
        :param algorithm: Signing algorithm (default RS256)
        :return: Signed JWT token
        """
        return jwt.encode(
            payload,
            self.private_key_pem,
            algorithm=algorithm,
            headers={"kid": self.kid},
        )

    def get_jwk(self) -> dict[str, Any]:
        """Get the public key as a JWK (JSON Web Key).

        :return: JWK dictionary
        """
        return {
            "kty": "RSA",
            "use": "sig",
            "kid": self.kid,
            "n": self.n,
            "e": self.e,
            "alg": "RS256",
        }


@pytest.fixture
def oidc_test_keys() -> OIDCTestKeys:
    """RSA key pair for signing test ID tokens."""
    return OIDCTestKeys()


@pytest.fixture
def mock_discovery_document() -> dict[str, Any]:
    """OIDC discovery document (/.well-known/openid-configuration)."""
    return {
        "issuer": "https://oidc.test.example.com",
        "authorization_endpoint": "https://oidc.test.example.com/authorize",
        "token_endpoint": "https://oidc.test.example.com/token",
        "userinfo_endpoint": "https://oidc.test.example.com/userinfo",
        "jwks_uri": "https://oidc.test.example.com/.well-known/jwks.json",
        "end_session_endpoint": "https://oidc.test.example.com/logout",
        "registration_endpoint": "https://oidc.test.example.com/register",
        "scopes_supported": [
            "openid",
            "profile",
            "email",
            "address",
            "phone",
            "offline_access",
        ],
        "response_types_supported": [
            "code",
            "id_token",
            "token id_token",
            "code id_token",
            "code token",
            "code token id_token",
        ],
        "grant_types_supported": [
            "authorization_code",
            "implicit",
            "refresh_token",
        ],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": [
            "RS256",
            "RS384",
            "RS512",
            "ES256",
            "ES384",
            "ES512",
            "HS256",
            "HS384",
            "HS512",
        ],
        "token_endpoint_auth_methods_supported": [
            "client_secret_basic",
            "client_secret_post",
            "client_secret_jwt",
            "private_key_jwt",
        ],
        "code_challenge_methods_supported": ["plain", "S256"],
        "claims_supported": [
            "aud",
            "email",
            "email_verified",
            "exp",
            "family_name",
            "given_name",
            "iat",
            "iss",
            "locale",
            "name",
            "picture",
            "sub",
        ],
        "backchannel_logout_supported": True,
        "backchannel_logout_session_supported": True,
    }


@pytest.fixture
def mock_jwks(oidc_test_keys: OIDCTestKeys) -> dict[str, Any]:
    """JWKS (JSON Web Key Set) endpoint response."""
    return {"keys": [oidc_test_keys.get_jwk()]}


@pytest.fixture
def mock_id_token_claims() -> dict[str, Any]:
    """ID token claims (decoded payload)."""
    now = int(time.time())
    return {
        "iss": "https://oidc.test.example.com",
        "sub": "user123",
        "aud": "test-client-id",
        "exp": now + ID_TOKEN_EXPIRY_SECONDS,
        "iat": now,
        "nonce": "test-nonce-abc123",
        "email": "testuser@example.com",
        "email_verified": True,
        "name": "Test User",
        "given_name": "Test",
        "family_name": "User",
        "preferred_username": "testuser",
    }


@pytest.fixture
def mock_id_token(
    oidc_test_keys: OIDCTestKeys, mock_id_token_claims: dict[str, Any]
) -> str:
    """Signed ID token (JWT)."""
    return oidc_test_keys.sign_jwt(mock_id_token_claims)


@pytest.fixture
def mock_token_response(mock_id_token: str) -> dict[str, Any]:
    """Token endpoint response."""
    return {
        "access_token": "mock_access_token_abc123xyz",
        "token_type": "Bearer",
        "expires_in": ACCESS_TOKEN_EXPIRY_SECONDS,
        "refresh_token": "mock_refresh_token_xyz789abc",
        "id_token": mock_id_token,
        "scope": "openid profile email",
    }


@pytest.fixture
def mock_userinfo_response() -> dict[str, Any]:
    """UserInfo endpoint response."""
    return {
        "sub": "user123",
        "name": "Test User",
        "given_name": "Test",
        "family_name": "User",
        "preferred_username": "testuser",
        "email": "testuser@example.com",
        "email_verified": True,
        "picture": "https://example.com/avatar.jpg",
        "locale": "en-US",
    }


@pytest.fixture
def mock_logout_token_claims(mock_id_token_claims: dict[str, Any]) -> dict[str, Any]:
    """Back-channel logout token claims (nonce prohibited)."""
    now = int(time.time())
    claims = {k: v for k, v in mock_id_token_claims.items() if k != "nonce"}
    claims.update(
        {
            "iat": now,
            "exp": now + LOGOUT_TOKEN_EXPIRY_SECONDS,
            "jti": "logout-token-unique-id-123",
            "events": {"http://schemas.openid.net/event/backchannel-logout": {}},
            "sid": "session-id-abc123",
        }
    )
    return claims


@pytest.fixture
def mock_logout_token(
    oidc_test_keys: OIDCTestKeys, mock_logout_token_claims: dict[str, Any]
) -> str:
    """Signed back-channel logout token."""
    return oidc_test_keys.sign_jwt(mock_logout_token_claims)


@pytest.fixture
def mock_pkce() -> dict[str, str]:
    """PKCE (Proof Key for Code Exchange) values."""
    import hashlib

    code_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
    code_challenge = (
        urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .rstrip(b"=")
        .decode("utf-8")
    )

    return {
        "code_verifier": code_verifier,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }


@pytest.fixture
def mock_state_data() -> dict[str, Any]:
    """State parameter data (before HMAC signing)."""
    return {
        "library_short_name": "TESTLIB",
        "provider_name": "Test OIDC Provider",
        "redirect_uri": "app://auth/callback",
        "nonce": "test-nonce-abc123",
        "timestamp": int(time.time()),
    }


@pytest.fixture
def mock_authorization_code() -> str:
    """Authorization code from OIDC provider."""
    return "mock_authorization_code_4/0AY0e-g7xQ"


class MockOIDCProvider:
    """Mock OIDC provider for integration testing.

    Provides methods to simulate a complete OIDC provider including
    all endpoints and responses with stateful behavior.
    """

    def __init__(
        self,
        keys: OIDCTestKeys,
        discovery: dict[str, Any],
        jwks: dict[str, Any],
        id_token_claims: dict[str, Any],
    ):
        """Initialize mock provider with test fixtures."""
        self.keys = keys
        self.discovery_document = discovery
        self.jwks_document = jwks
        self.default_id_token_claims = id_token_claims
        self._issued_codes: set[str] = set()
        self._issued_refresh_tokens: set[str] = set()

    def authorize(
        self, client_id: str = "test-client-id", state: str | None = None
    ) -> str:
        """Simulate authorization endpoint (returns authorization code).

        :param client_id: OAuth client ID
        :param state: State parameter for CSRF protection
        :return: Authorization code
        """
        code = f"auth_code_{int(time.time())}"
        self._issued_codes.add(code)
        return code

    def token(
        self,
        code: str,
        grant_type: str = "authorization_code",
        code_verifier: str | None = None,
    ) -> dict[str, Any]:
        """Simulate token endpoint (exchanges code for tokens).

        :param code: Authorization code
        :param grant_type: OAuth grant type
        :param code_verifier: PKCE code verifier
        :return: Token response with access_token, id_token, refresh_token
        """
        if grant_type == "authorization_code":
            if code not in self._issued_codes:
                raise ValueError("Invalid authorization code")
            self._issued_codes.remove(code)

        elif grant_type == "refresh_token":
            if code not in self._issued_refresh_tokens:
                raise ValueError("Invalid refresh token")

        id_token = self.keys.sign_jwt(self.default_id_token_claims)
        refresh_token = f"refresh_token_{int(time.time())}"
        self._issued_refresh_tokens.add(refresh_token)

        return {
            "access_token": f"access_token_{int(time.time())}",
            "token_type": "Bearer",
            "expires_in": ACCESS_TOKEN_EXPIRY_SECONDS,
            "refresh_token": refresh_token,
            "id_token": id_token,
            "scope": "openid profile email",
        }

    def userinfo(self, access_token: str) -> dict[str, Any]:
        """Simulate UserInfo endpoint.

        :param access_token: Access token
        :return: User profile information
        """
        return {
            "sub": self.default_id_token_claims["sub"],
            "name": self.default_id_token_claims.get("name", "Test User"),
            "email": self.default_id_token_claims.get("email", "test@example.com"),
            "email_verified": self.default_id_token_claims.get("email_verified", True),
        }

    def decode_id_token(self, token: str) -> dict[str, Any]:
        """Decode and validate ID token (for testing purposes).

        :param token: JWT ID token
        :return: Decoded claims
        """
        return jwt.decode(
            token,
            self.keys.public_key_pem,
            algorithms=["RS256"],
            audience="test-client-id",
        )


@pytest.fixture
def mock_oidc_provider(
    oidc_test_keys: OIDCTestKeys,
    mock_discovery_document: dict[str, Any],
    mock_jwks: dict[str, Any],
    mock_id_token_claims: dict[str, Any],
) -> MockOIDCProvider:
    """Complete mock OIDC provider for integration testing."""
    return MockOIDCProvider(
        keys=oidc_test_keys,
        discovery=mock_discovery_document,
        jwks=mock_jwks,
        id_token_claims=mock_id_token_claims,
    )
