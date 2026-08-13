"""OIDC ID Token Validator.

This module provides functionality for validating OIDC ID tokens including:
- Signature verification using JWKS
- Claims validation (issuer, audience, expiry, etc.)
"""

from __future__ import annotations

import time
from typing import Any, cast

from joserfc import jwt
from joserfc.errors import JoseError
from joserfc.jwk import KeySet, KeySetSerialization

from palace.util.exceptions import BasePalaceException
from palace.util.log import LoggerMixin


class OIDCTokenValidationError(BasePalaceException):
    """Base exception for ID token validation errors."""


class OIDCTokenSignatureError(OIDCTokenValidationError):
    """Raised when ID token signature validation fails."""


class OIDCTokenClaimsError(OIDCTokenValidationError):
    """Raised when ID token claims validation fails."""


class OIDCTokenValidator(LoggerMixin):
    """Validator for OIDC ID tokens."""

    # Clock skew tolerance (seconds) - allows for time differences between servers
    CLOCK_SKEW_TOLERANCE = 300  # 5 minutes

    # Signing algorithms accepted when verifying ID token signatures
    ALLOWED_ALGORITHMS = frozenset(("RS256", "RS384", "RS512", "HS256"))

    def validate_signature(self, id_token: str, jwks: dict[str, Any]) -> dict[str, Any]:
        """Validate ID token signature using JWKS.

        :param id_token: Raw ID token (JWT)
        :param jwks: JSON Web Key Set from provider
        :raises OIDCTokenSignatureError: If signature validation fails
        :return: Decoded token claims
        """
        try:
            # Create key set from JWKS. The JWKS is arbitrary JSON fetched from
            # the provider, so cast it to the structured type joserfc expects.
            key_set = KeySet.import_key_set(cast(KeySetSerialization, jwks))

            # Decode and verify signature. joserfc's decode will automatically:
            # 1. Find the correct key from the set using the 'kid' header
            # 2. Verify the signature using one of the allowed algorithms
            # 3. Return a token whose 'claims' is the decoded payload
            # We validate the claims separately for better error messages.
            token = jwt.decode(
                id_token,
                key_set,
                algorithms=self.ALLOWED_ALGORITHMS,
            )

            self.log.debug("ID token signature validated successfully")
            return token.claims

        except JoseError as e:
            self.log.exception("ID token signature validation failed")
            raise OIDCTokenSignatureError(
                f"Failed to validate ID token signature: {str(e)}"
            ) from e
        except Exception as e:
            self.log.exception("Unexpected error during signature validation")
            raise OIDCTokenSignatureError(
                f"Unexpected error validating signature: {str(e)}"
            ) from e

    def validate_claims(
        self,
        claims: dict[str, Any],
        expected_issuer: str,
        expected_audience: str,
        nonce: str | None = None,
        current_time: int | None = None,
    ) -> None:
        """Validate ID token claims.

        Validates required OIDC claims:
        - iss (issuer) - must match expected_issuer
        - aud (audience) - must match expected_audience
        - exp (expiry) - must not be in the past (with clock skew tolerance)
        - iat (issued at) - must not be in the future (with clock skew tolerance)
        - nonce - if provided, must match expected nonce

        :param claims: Decoded token claims
        :param expected_issuer: Expected issuer URL
        :param expected_audience: Expected audience (client_id)
        :param nonce: Expected nonce value (if used)
        :param current_time: Current time in seconds (for testing)
        :raises OIDCTokenClaimsError: If claims validation fails
        """
        if current_time is None:
            current_time = int(time.time())

        errors = []

        # Validate issuer (iss)
        issuer = claims.get("iss")
        if not issuer:
            errors.append("Missing required claim: 'iss' (issuer)")
        elif issuer != expected_issuer:
            errors.append(
                f"Issuer mismatch: expected '{expected_issuer}', got '{issuer}'"
            )

        # Validate audience (aud)
        audience = claims.get("aud")
        if not audience:
            errors.append("Missing required claim: 'aud' (audience)")
        else:
            # Audience can be a string or array
            audiences = audience if isinstance(audience, list) else [audience]
            if expected_audience not in audiences:
                errors.append(
                    f"Audience mismatch: expected '{expected_audience}', got {audiences}"
                )

        # Validate expiry (exp)
        exp = claims.get("exp")
        if not exp:
            errors.append("Missing required claim: 'exp' (expiry)")
        else:
            try:
                exp_time = int(exp)
                if current_time > exp_time + self.CLOCK_SKEW_TOLERANCE:
                    errors.append(
                        f"Token expired: exp={exp_time}, current={current_time}, "
                        f"age={current_time - exp_time}s"
                    )
            except (ValueError, TypeError):
                errors.append(f"Invalid 'exp' claim format: {exp}")

        # Validate issued at (iat)
        iat = claims.get("iat")
        if not iat:
            errors.append("Missing required claim: 'iat' (issued at)")
        else:
            try:
                iat_time = int(iat)
                if current_time < iat_time - self.CLOCK_SKEW_TOLERANCE:
                    errors.append(
                        f"Token issued in the future: iat={iat_time}, current={current_time}"
                    )
            except (ValueError, TypeError):
                errors.append(f"Invalid 'iat' claim format: {iat}")

        # Validate subject (sub) - required by OIDC spec
        sub = claims.get("sub")
        if not sub:
            errors.append("Missing required claim: 'sub' (subject)")

        # Validate nonce if provided
        if nonce is not None:
            token_nonce = claims.get("nonce")
            if not token_nonce:
                errors.append("Missing required claim: 'nonce'")
            elif token_nonce != nonce:
                errors.append(
                    f"Nonce mismatch: expected '{nonce}', got '{token_nonce}'"
                )

        # Raise if any validation errors
        if errors:
            error_msg = "; ".join(errors)
            self.log.error(f"ID token claims validation failed: {error_msg}")
            raise OIDCTokenClaimsError(f"Invalid ID token claims: {error_msg}")

        self.log.debug("ID token claims validated successfully")
