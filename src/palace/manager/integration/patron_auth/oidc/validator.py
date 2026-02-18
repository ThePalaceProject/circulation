"""OIDC ID Token Validator.

This module provides functionality for validating OIDC ID tokens including:
- Signature verification using JWKS
- Claims validation (issuer, audience, expiry, etc.)
- Patron ID extraction from claims
"""

from __future__ import annotations

import time
from re import Pattern
from typing import Any, cast

from authlib.jose import JsonWebKey, JsonWebToken
from authlib.jose.errors import JoseError

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.util.log import LoggerMixin


class OIDCTokenValidationError(BasePalaceException):
    """Base exception for ID token validation errors."""


class OIDCTokenSignatureError(OIDCTokenValidationError):
    """Raised when ID token signature validation fails."""


class OIDCTokenClaimsError(OIDCTokenValidationError):
    """Raised when ID token claims validation fails."""


class OIDCPatronIDExtractionError(OIDCTokenValidationError):
    """Raised when patron ID cannot be extracted from claims."""


class OIDCTokenValidator(LoggerMixin):
    """Validator for OIDC ID tokens."""

    # Clock skew tolerance (seconds) - allows for time differences between servers
    CLOCK_SKEW_TOLERANCE = 300  # 5 minutes

    def __init__(self) -> None:
        """Initialize OIDC token validator."""
        self._jwt = JsonWebToken(algorithms=["RS256", "RS384", "RS512", "HS256"])

    def validate_signature(self, id_token: str, jwks: dict[str, Any]) -> dict[str, Any]:
        """Validate ID token signature using JWKS.

        :param id_token: Raw ID token (JWT)
        :param jwks: JSON Web Key Set from provider
        :raises OIDCTokenSignatureError: If signature validation fails
        :return: Decoded token claims
        """
        try:
            # Create JWK from JWKS
            jwk_set = JsonWebKey.import_key_set(jwks)

            # Decode and verify signature
            # authlib's decode will automatically:
            # 1. Find the correct key from the set using 'kid' header
            # 2. Verify the signature
            # 3. Return the payload claims
            claims = cast(
                dict[str, Any],
                self._jwt.decode(
                    id_token,
                    jwk_set,
                    # We'll validate claims separately for better error messages
                    claims_options={"iss": {"essential": False}},
                ),
            )

            self.log.debug("ID token signature validated successfully")
            return claims

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

    def extract_patron_id(
        self,
        claims: dict[str, Any],
        claim_name: str,
        regex_pattern: Pattern[str] | None = None,
    ) -> str:
        """Extract patron ID from ID token claims.

        :param claims: Decoded ID token claims
        :param claim_name: Name of claim containing patron ID
        :param regex_pattern: Optional regex pattern to extract ID from claim value.
                              Must contain a named group 'patron_id'.
        :raises OIDCPatronIDExtractionError: If patron ID cannot be extracted
        :return: Extracted patron ID
        """
        # Get claim value
        claim_value = claims.get(claim_name)
        if claim_value is None:
            self.log.error(f"Claim '{claim_name}' not found in ID token")
            available_claims = ", ".join(claims.keys())
            raise OIDCPatronIDExtractionError(
                f"Patron ID claim '{claim_name}' not found in ID token. "
                f"Available claims: {available_claims}"
            )

        # Convert to string if needed
        if not isinstance(claim_value, str):
            try:
                claim_value = str(claim_value)
            except Exception as e:
                raise OIDCPatronIDExtractionError(
                    f"Cannot convert claim '{claim_name}' value to string: {claim_value}"
                ) from e

        # Apply regex if provided
        if regex_pattern:
            match = regex_pattern.search(claim_value)
            if not match:
                self.log.error(
                    f"Regex pattern did not match claim value: '{claim_value}'"
                )
                raise OIDCPatronIDExtractionError(
                    f"Patron ID regex pattern did not match claim '{claim_name}' value: '{claim_value}'"
                )

            try:
                patron_id = match.group("patron_id")
            except IndexError:
                raise OIDCPatronIDExtractionError(
                    "Regex pattern must contain a named group 'patron_id'"
                )

            if not patron_id:
                raise OIDCPatronIDExtractionError(
                    f"Regex pattern matched but 'patron_id' group is empty"
                )

            self.log.debug(
                f"Extracted patron ID '{patron_id}' from claim '{claim_name}' "
                "using regex pattern"
            )
            return patron_id

        # No regex - use full claim value
        if not claim_value:
            raise OIDCPatronIDExtractionError(
                f"Claim '{claim_name}' is empty or whitespace-only"
            )

        self.log.debug(f"Extracted patron ID '{claim_value}' from claim '{claim_name}'")
        return claim_value

    def validate_and_extract(
        self,
        id_token: str,
        jwks: dict[str, Any],
        expected_issuer: str,
        expected_audience: str,
        patron_id_claim: str,
        nonce: str | None = None,
        patron_id_regex: Pattern[str] | None = None,
    ) -> tuple[dict[str, Any], str]:
        """Validate ID token and extract patron ID in one operation.

        Convenience method that combines signature validation, claims validation,
        and patron ID extraction.

        :param id_token: Raw ID token (JWT)
        :param jwks: JSON Web Key Set from provider
        :param expected_issuer: Expected issuer URL
        :param expected_audience: Expected audience (client_id)
        :param patron_id_claim: Name of claim containing patron ID
        :param nonce: Expected nonce value (if used)
        :param patron_id_regex: Optional regex pattern for patron ID extraction
        :raises OIDCTokenValidationError: If validation fails
        :return: Tuple of (claims dict, patron_id)
        """
        # Validate signature
        claims = self.validate_signature(id_token, jwks)

        # Validate claims
        self.validate_claims(claims, expected_issuer, expected_audience, nonce)

        # Extract patron ID
        patron_id = self.extract_patron_id(claims, patron_id_claim, patron_id_regex)

        return claims, patron_id
