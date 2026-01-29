"""Unit tests for OIDC token validation."""

from __future__ import annotations

import re
import time

import pytest

from palace.manager.integration.patron_auth.oidc.validator import (
    OIDCPatronIDExtractionError,
    OIDCTokenClaimsError,
    OIDCTokenSignatureError,
    OIDCTokenValidator,
)

# Test constants
TEST_ISSUER = "https://oidc.test.example.com"
TEST_CLIENT_ID = "test-client-id"
TEST_NONCE = "test-nonce-abc123"
EXPIRED_TOKEN_AGE = 7200


class TestOIDCTokenValidatorSignature:
    """Tests for ID token signature validation."""

    def test_validate_signature_success(
        self, mock_id_token, mock_jwks, mock_id_token_claims
    ):
        validator = OIDCTokenValidator()

        claims = validator.validate_signature(mock_id_token, mock_jwks)

        assert claims["sub"] == mock_id_token_claims["sub"]
        assert claims["iss"] == mock_id_token_claims["iss"]
        assert claims["aud"] == mock_id_token_claims["aud"]

    def test_validate_signature_invalid_token_format(self, mock_jwks):
        validator = OIDCTokenValidator()
        invalid_tokens = [
            "not.a.jwt",
            "only-one-part",
            "",
            "too.many.parts.here.invalid",
        ]

        for invalid_token in invalid_tokens:
            with pytest.raises(OIDCTokenSignatureError):
                validator.validate_signature(invalid_token, mock_jwks)

    def test_validate_signature_wrong_key(self, mock_id_token):
        validator = OIDCTokenValidator()

        wrong_jwks = {
            "keys": [
                {
                    "kty": "RSA",
                    "kid": "wrong-key-id",
                    "use": "sig",
                    "n": "wrong-modulus",
                    "e": "AQAB",
                }
            ]
        }

        with pytest.raises(OIDCTokenSignatureError):
            validator.validate_signature(mock_id_token, wrong_jwks)

    def test_validate_signature_tampered_token(self, oidc_test_keys, mock_jwks):
        validator = OIDCTokenValidator()

        valid_payload = {"sub": "user123", "iss": TEST_ISSUER}
        valid_token = oidc_test_keys.sign_jwt(valid_payload)

        header, payload, signature = valid_token.split(".")
        tampered_token = f"{header}.{payload}.tampered_signature_xyz"

        with pytest.raises(OIDCTokenSignatureError):
            validator.validate_signature(tampered_token, mock_jwks)


class TestOIDCTokenValidatorClaims:
    """Tests for ID token claims validation."""

    def test_validate_claims_success(self, mock_id_token_claims):
        validator = OIDCTokenValidator()

        validator.validate_claims(
            claims=mock_id_token_claims,
            expected_issuer=TEST_ISSUER,
            expected_audience=TEST_CLIENT_ID,
            nonce=TEST_NONCE,
        )

    def test_validate_claims_missing_issuer(self):
        validator = OIDCTokenValidator()
        claims = {
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        with pytest.raises(OIDCTokenClaimsError, match="Missing required claim: 'iss'"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_issuer_mismatch(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": "https://wrong.issuer.com",
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        with pytest.raises(OIDCTokenClaimsError, match="Issuer mismatch"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_missing_audience(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        with pytest.raises(OIDCTokenClaimsError, match="Missing required claim: 'aud'"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_audience_mismatch(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": "wrong-client-id",
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        with pytest.raises(OIDCTokenClaimsError, match="Audience mismatch"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_audience_array(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": [TEST_CLIENT_ID, "other-client-id"],
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_missing_expiry(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "iat": int(time.time()),
        }

        with pytest.raises(OIDCTokenClaimsError, match="Missing required claim: 'exp'"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_expired_token(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": current_time - EXPIRED_TOKEN_AGE,
            "iat": current_time - EXPIRED_TOKEN_AGE - 100,
        }

        with pytest.raises(OIDCTokenClaimsError, match="Token expired"):
            validator.validate_claims(
                claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
            )

    def test_validate_claims_expiry_with_clock_skew(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": current_time - 100,
            "iat": current_time - 3700,
        }

        validator.validate_claims(
            claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
        )

    def test_validate_claims_missing_issued_at(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
        }

        with pytest.raises(OIDCTokenClaimsError, match="Missing required claim: 'iat'"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_future_issued_at(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())
        future_time = current_time + 1000

        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": future_time + 3600,
            "iat": future_time,
        }

        with pytest.raises(OIDCTokenClaimsError, match="issued in the future"):
            validator.validate_claims(
                claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
            )

    def test_validate_claims_missing_subject(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        with pytest.raises(OIDCTokenClaimsError, match="Missing required claim: 'sub'"):
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

    def test_validate_claims_nonce_mismatch(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "nonce": "wrong-nonce",
        }

        with pytest.raises(OIDCTokenClaimsError, match="Nonce mismatch"):
            validator.validate_claims(
                claims, TEST_ISSUER, TEST_CLIENT_ID, nonce=TEST_NONCE
            )

    def test_validate_claims_missing_required_nonce(self):
        validator = OIDCTokenValidator()
        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }

        with pytest.raises(
            OIDCTokenClaimsError, match="Missing required claim: 'nonce'"
        ):
            validator.validate_claims(
                claims, TEST_ISSUER, TEST_CLIENT_ID, nonce=TEST_NONCE
            )

    def test_validate_claims_multiple_errors(self):
        validator = OIDCTokenValidator()
        claims = {}

        with pytest.raises(OIDCTokenClaimsError) as exc_info:
            validator.validate_claims(claims, TEST_ISSUER, TEST_CLIENT_ID)

        error_message = str(exc_info.value)
        assert "Missing required claim: 'iss'" in error_message
        assert "Missing required claim: 'aud'" in error_message
        assert "Missing required claim: 'exp'" in error_message
        assert "Missing required claim: 'iat'" in error_message
        assert "Missing required claim: 'sub'" in error_message


class TestOIDCTokenValidatorPatronIDExtraction:
    """Tests for patron ID extraction from claims."""

    def test_extract_patron_id_from_sub(self, mock_id_token_claims):
        validator = OIDCTokenValidator()

        patron_id = validator.extract_patron_id(mock_id_token_claims, claim_name="sub")

        assert patron_id == "user123"

    def test_extract_patron_id_from_email(self, mock_id_token_claims):
        validator = OIDCTokenValidator()

        patron_id = validator.extract_patron_id(
            mock_id_token_claims, claim_name="email"
        )

        assert patron_id == "testuser@example.com"

    def test_extract_patron_id_from_preferred_username(self, mock_id_token_claims):
        validator = OIDCTokenValidator()

        patron_id = validator.extract_patron_id(
            mock_id_token_claims, claim_name="preferred_username"
        )

        assert patron_id == "testuser"

    def test_extract_patron_id_missing_claim(self):
        validator = OIDCTokenValidator()
        claims = {"sub": "user123", "email": "test@example.com"}

        with pytest.raises(OIDCPatronIDExtractionError, match="not found in ID token"):
            validator.extract_patron_id(claims, claim_name="nonexistent_claim")

    def test_extract_patron_id_empty_claim(self):
        validator = OIDCTokenValidator()
        claims = {"sub": "user123", "email": ""}

        with pytest.raises(
            OIDCPatronIDExtractionError, match="is empty or whitespace-only"
        ):
            validator.extract_patron_id(claims, claim_name="email")

    def test_extract_patron_id_with_regex(self, mock_id_token_claims):
        validator = OIDCTokenValidator()
        regex_pattern = re.compile(r"(?P<patron_id>[^@]+)@")

        patron_id = validator.extract_patron_id(
            mock_id_token_claims, claim_name="email", regex_pattern=regex_pattern
        )

        assert patron_id == "testuser"

    def test_extract_patron_id_regex_no_match(self, mock_id_token_claims):
        validator = OIDCTokenValidator()
        regex_pattern = re.compile(r"(?P<patron_id>NOMATCH)")

        with pytest.raises(
            OIDCPatronIDExtractionError, match="regex pattern did not match"
        ):
            validator.extract_patron_id(
                mock_id_token_claims, claim_name="email", regex_pattern=regex_pattern
            )

    def test_extract_patron_id_regex_missing_named_group(self, mock_id_token_claims):
        validator = OIDCTokenValidator()
        regex_pattern = re.compile(r"([^@]+)@")

        with pytest.raises(
            OIDCPatronIDExtractionError, match="must contain a named group 'patron_id'"
        ):
            validator.extract_patron_id(
                mock_id_token_claims, claim_name="email", regex_pattern=regex_pattern
            )

    def test_extract_patron_id_regex_empty_match(self):
        validator = OIDCTokenValidator()
        claims = {"email": "@example.com"}
        regex_pattern = re.compile(r"(?P<patron_id>[^@]*)@")

        with pytest.raises(
            OIDCPatronIDExtractionError, match="'patron_id' group is empty"
        ):
            validator.extract_patron_id(
                claims, claim_name="email", regex_pattern=regex_pattern
            )

    def test_extract_patron_id_non_string_claim(self):
        validator = OIDCTokenValidator()
        claims = {"sub": "user123", "age": 42}

        patron_id = validator.extract_patron_id(claims, claim_name="age")

        assert patron_id == "42"

    def test_extract_patron_id_complex_regex(self):
        validator = OIDCTokenValidator()
        claims = {"eduPersonPrincipalName": "jsmith123@university.edu"}
        regex_pattern = re.compile(r"(?P<patron_id>[a-z]+\d+)@")

        patron_id = validator.extract_patron_id(
            claims, claim_name="eduPersonPrincipalName", regex_pattern=regex_pattern
        )

        assert patron_id == "jsmith123"


class TestOIDCTokenValidatorCombined:
    """Tests for combined validation and extraction."""

    def test_validate_and_extract_success(
        self, mock_id_token, mock_jwks, mock_id_token_claims
    ):
        validator = OIDCTokenValidator()

        claims, patron_id = validator.validate_and_extract(
            id_token=mock_id_token,
            jwks=mock_jwks,
            expected_issuer=TEST_ISSUER,
            expected_audience=TEST_CLIENT_ID,
            patron_id_claim="sub",
            nonce=TEST_NONCE,
        )

        assert claims["sub"] == "user123"
        assert patron_id == "user123"

    def test_validate_and_extract_with_regex(
        self, mock_id_token, mock_jwks, mock_id_token_claims
    ):
        validator = OIDCTokenValidator()
        regex_pattern = re.compile(r"(?P<patron_id>[^@]+)@")

        claims, patron_id = validator.validate_and_extract(
            id_token=mock_id_token,
            jwks=mock_jwks,
            expected_issuer=TEST_ISSUER,
            expected_audience=TEST_CLIENT_ID,
            patron_id_claim="email",
            nonce=TEST_NONCE,
            patron_id_regex=regex_pattern,
        )

        assert claims["email"] == "testuser@example.com"
        assert patron_id == "testuser"

    def test_validate_and_extract_signature_failure(self, mock_jwks):
        validator = OIDCTokenValidator()
        invalid_token = "invalid.jwt.token"

        with pytest.raises(OIDCTokenSignatureError):
            validator.validate_and_extract(
                id_token=invalid_token,
                jwks=mock_jwks,
                expected_issuer=TEST_ISSUER,
                expected_audience=TEST_CLIENT_ID,
                patron_id_claim="sub",
            )

    def test_validate_and_extract_claims_failure(self, oidc_test_keys, mock_jwks):
        validator = OIDCTokenValidator()

        claims = {
            "iss": "https://wrong.issuer.com",
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        invalid_token = oidc_test_keys.sign_jwt(claims)

        with pytest.raises(OIDCTokenClaimsError, match="Issuer mismatch"):
            validator.validate_and_extract(
                id_token=invalid_token,
                jwks=mock_jwks,
                expected_issuer=TEST_ISSUER,
                expected_audience=TEST_CLIENT_ID,
                patron_id_claim="sub",
            )

    def test_validate_and_extract_patron_id_failure(self, oidc_test_keys, mock_jwks):
        validator = OIDCTokenValidator()

        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
        }
        token = oidc_test_keys.sign_jwt(claims)

        with pytest.raises(OIDCPatronIDExtractionError, match="not found in ID token"):
            validator.validate_and_extract(
                id_token=token,
                jwks=mock_jwks,
                expected_issuer=TEST_ISSUER,
                expected_audience=TEST_CLIENT_ID,
                patron_id_claim="nonexistent_claim",
            )


class TestOIDCTokenValidatorClockSkew:
    """Tests for clock skew tolerance in token validation."""

    def test_expiry_within_clock_skew_tolerance(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())

        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": current_time - 100,
            "iat": current_time - 3700,
        }

        validator.validate_claims(
            claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
        )

    def test_expiry_outside_clock_skew_tolerance(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())

        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": current_time - validator.CLOCK_SKEW_TOLERANCE - 100,
            "iat": current_time - 4000,
        }

        with pytest.raises(OIDCTokenClaimsError, match="Token expired"):
            validator.validate_claims(
                claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
            )

    def test_issued_at_within_clock_skew_tolerance(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())

        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": current_time + 3600,
            "iat": current_time + 100,
        }

        validator.validate_claims(
            claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
        )

    def test_issued_at_outside_clock_skew_tolerance(self):
        validator = OIDCTokenValidator()
        current_time = int(time.time())

        claims = {
            "iss": TEST_ISSUER,
            "aud": TEST_CLIENT_ID,
            "sub": "user123",
            "exp": current_time + 4000,
            "iat": current_time + validator.CLOCK_SKEW_TOLERANCE + 100,
        }

        with pytest.raises(OIDCTokenClaimsError, match="issued in the future"):
            validator.validate_claims(
                claims, TEST_ISSUER, TEST_CLIENT_ID, current_time=current_time
            )
