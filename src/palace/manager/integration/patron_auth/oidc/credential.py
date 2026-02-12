"""OIDC Credential Manager.

This module manages OIDC credentials (tokens) stored in the database.
OIDC tokens include:
- ID token claims
- Access token
- Refresh token (optional)
- Expiry information
"""

from __future__ import annotations

import datetime
import json
from typing import Any, cast

from sqlalchemy import and_, exists
from sqlalchemy.orm import Session

from palace.manager.integration.patron_auth.oidc.auth import (
    OIDCAuthenticationManager,
    OIDCRefreshTokenError,
)
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin


class OIDCCredentialManager(LoggerMixin):
    """Manages OIDC tokens stored as Credential objects.

    OIDC tokens contain:
    - id_token_claims: Validated claims from the ID token
    - access_token: Access token for API calls
    - refresh_token: Optional refresh token for token renewal
    """

    TOKEN_TYPE = "OIDC token"
    TOKEN_DATA_SOURCE_NAME = "OIDC"

    def _get_token_data_source(self, db: Session) -> DataSource:
        """Get or create the data source for OIDC credentials.

        :param db: Database session
        :return: DataSource for OIDC credentials
        """
        datasource, _ = get_one_or_create(
            db, DataSource, name=self.TOKEN_DATA_SOURCE_NAME
        )
        return datasource

    @staticmethod
    def _create_token_value(
        id_token_claims: dict[str, Any],
        access_token: str,
        refresh_token: str | None = None,
    ) -> str:
        """Create OIDC token value by serializing token data.

        :param id_token_claims: Validated ID token claims
        :param access_token: Access token
        :param refresh_token: Optional refresh token
        :return: JSON-serialized token data
        """
        token_data = {
            "id_token_claims": id_token_claims,
            "access_token": access_token,
        }

        if refresh_token:
            token_data["refresh_token"] = refresh_token

        return json.dumps(token_data)

    def extract_token_data(self, credential: Credential) -> dict[str, Any]:
        """Extract token data from credential.

        :param credential: Credential object containing OIDC token
        :return: Dictionary with id_token_claims, access_token, refresh_token
        """
        self.log.debug(f"Extracting OIDC token data from credential {credential.id}")

        credential_value = credential.credential if credential.credential else "{}"

        try:
            token_data = cast(dict[str, Any], json.loads(credential_value))
        except json.JSONDecodeError as e:
            self.log.exception("Failed to decode OIDC token data")
            raise ValueError(f"Invalid OIDC token format: {str(e)}") from e

        # Validate structure
        if "id_token_claims" not in token_data:
            raise ValueError("OIDC token missing id_token_claims")
        if "access_token" not in token_data:
            raise ValueError("OIDC token missing access_token")

        return token_data

    def create_oidc_token(
        self,
        db: Session,
        patron: Patron,
        id_token_claims: dict[str, Any],
        access_token: str,
        refresh_token: str | None = None,
        expires_in: int | None = None,
        session_lifetime_days: int | None = None,
    ) -> Credential:
        """Create a Credential object for OIDC tokens.

        :param db: Database session
        :param patron: Patron object
        :param id_token_claims: Validated ID token claims
        :param access_token: Access token
        :param refresh_token: Optional refresh token
        :param expires_in: Token lifetime in seconds (from provider)
        :param session_lifetime_days: Override session lifetime in days
        :return: Created Credential object
        """
        # Calculate expiry
        if session_lifetime_days:
            # Use configured session lifetime override
            session_lifetime = datetime.timedelta(days=session_lifetime_days)
            self.log.debug(
                f"Using configured session lifetime: {session_lifetime_days} days"
            )
        elif expires_in:
            # Use provider's token expiry
            session_lifetime = datetime.timedelta(seconds=expires_in)
            self.log.debug(f"Using provider token expiry: {expires_in} seconds")
        else:
            # Default to 24 hours if no expiry information
            session_lifetime = datetime.timedelta(hours=24)
            self.log.warning("No expiry information provided, defaulting to 24 hours")

        # Create token value
        token_value = self._create_token_value(
            id_token_claims, access_token, refresh_token
        )

        # Get data source
        data_source = self._get_token_data_source(db)

        # Create credential
        self.log.info(
            f"Creating OIDC credential for patron {patron.id} "
            f"with lifetime {session_lifetime}"
        )

        oidc_credential, is_new = Credential.temporary_token_create(
            db, data_source, self.TOKEN_TYPE, patron, session_lifetime, token_value
        )

        return oidc_credential

    def lookup_oidc_token_by_patron(
        self, db: Session, patron: Patron
    ) -> Credential | None:
        """Look up OIDC credential by patron.

        :param db: Database session
        :param patron: Patron object
        :return: Credential object if found, None otherwise
        """
        self.log.debug(f"Looking up OIDC token for patron {patron.id}")

        credential = Credential.lookup_by_patron(
            db,
            self.TOKEN_DATA_SOURCE_NAME,
            self.TOKEN_TYPE,
            patron,
            allow_persistent_token=False,
            auto_create_datasource=True,
        )

        if credential:
            self.log.debug(
                f"Found OIDC credential {credential.id} for patron {patron.id}"
            )
        else:
            self.log.debug(f"No OIDC credential found for patron {patron.id}")

        return credential

    def lookup_oidc_token_by_value(
        self, db: Session, token_value: str, library_id: int
    ) -> Credential | None:
        """Look up OIDC credential by token value.

        Constrains lookup to patrons of the specified library for security.

        :param db: Database session
        :param token_value: OIDC token value to look up
        :param library_id: Library ID to constrain the search
        :return: Credential object if found, None otherwise
        """
        self.log.debug(f"Looking up OIDC token by value for library {library_id}")

        # Constrain to patrons of the specified library
        credential_constraint = exists().where(
            and_(Patron.id == Credential.patron_id, Patron.library_id == library_id)
        )

        credential = Credential.lookup_by_token(
            db,
            self._get_token_data_source(db),
            self.TOKEN_TYPE,
            token_value,
            constraint=credential_constraint,
        )

        if credential:
            self.log.debug(
                f"Found OIDC credential {credential.id} for library {library_id}"
            )
        else:
            self.log.debug(f"No OIDC credential found for library {library_id}")

        return credential

    def refresh_token_if_needed(
        self,
        db: Session,
        credential: Credential,
        auth_manager: OIDCAuthenticationManager,
        buffer_seconds: int = 300,
    ) -> Credential:
        """Refresh OIDC token if expired or expiring soon.

        :param db: Database session
        :param credential: Credential object to check/refresh
        :param auth_manager: OIDC authentication manager for token refresh
        :param buffer_seconds: Refresh if expiring within this many seconds (default: 5 minutes)
        :return: Updated credential (same object, may be modified)
        :raises OIDCRefreshTokenError: If refresh fails
        """
        # Check if token is expired or expiring soon
        now = utc_now()
        expiry = credential.expires

        if expiry is None:
            self.log.warning(
                f"Credential {credential.id} has no expiry, assuming valid"
            )
            return credential

        # Add buffer to expiry check to proactively refresh
        effective_expiry = expiry - datetime.timedelta(seconds=buffer_seconds)

        if now < effective_expiry:
            self.log.debug(
                f"Credential {credential.id} is still valid "
                f"(expires {expiry}, buffer {buffer_seconds}s)"
            )
            return credential

        # Token is expired or expiring soon - try to refresh
        self.log.info(
            f"Credential {credential.id} expired or expiring soon (expires {expiry}), "
            f"attempting refresh"
        )

        # Extract token data
        token_data = self.extract_token_data(credential)
        refresh_token = token_data.get("refresh_token")

        if not refresh_token:
            raise OIDCRefreshTokenError(
                "Credential has no refresh token, cannot refresh"
            )

        # Refresh tokens
        try:
            new_tokens = auth_manager.refresh_access_token(refresh_token)
        except OIDCRefreshTokenError:
            self.log.exception("Failed to refresh OIDC token")
            raise

        # Validate new ID token if present
        new_id_token_claims = token_data[
            "id_token_claims"
        ]  # Keep old claims as fallback
        if "id_token" in new_tokens:
            try:
                # Validate new ID token (no nonce check for refresh)
                new_id_token_claims = auth_manager.validate_id_token(
                    new_tokens["id_token"], nonce=None
                )
            except Exception as e:
                self.log.warning(f"Failed to validate refreshed ID token: {e}")
                # Keep using old claims if new token validation fails

        # Update credential with new tokens
        new_token_value = self._create_token_value(
            new_id_token_claims,
            new_tokens["access_token"],
            new_tokens.get("refresh_token", refresh_token),  # Use new or keep old
        )

        credential.credential = new_token_value

        # Update expiry
        if "expires_in" in new_tokens:
            credential.expires = now + datetime.timedelta(
                seconds=new_tokens["expires_in"]
            )
        else:
            # If no expiry provided, keep existing expiry logic
            self.log.warning("Refreshed token has no expiry information")

        db.commit()

        self.log.info(f"Successfully refreshed credential {credential.id}")
        return credential

    def lookup_patron_by_identifier(
        self, db: Session, patron_identifier: str, library_id: int
    ) -> Patron | None:
        """Look up patron by authorization identifier.

        :param db: Database session
        :param patron_identifier: Patron identifier from ID token
        :param library_id: Library ID to constrain the search
        :return: Patron object or None if not found
        """
        patron = (
            db.query(Patron)
            .filter(
                Patron.authorization_identifier == patron_identifier,
                Patron.library_id == library_id,
            )
            .first()
        )

        if patron:
            self.log.debug(
                f"Found patron {patron.id} for identifier {patron_identifier}"
            )
        else:
            self.log.debug(f"No patron found for identifier {patron_identifier}")

        return patron

    def invalidate_credential(self, db: Session, credential_id: int) -> None:
        """Invalidate a specific credential by marking it as expired.

        :param db: Database session
        :param credential_id: Credential ID to invalidate
        """
        credential = (
            db.query(Credential).filter(Credential.id == credential_id).one_or_none()
        )

        if not credential:
            self.log.warning(f"Credential {credential_id} not found")
            return

        credential.expires = utc_now()
        db.commit()

        self.log.info(f"Invalidated credential {credential_id}")

    def invalidate_patron_credentials(self, db: Session, patron_id: int) -> int:
        """Invalidate all OIDC credentials for a patron.

        :param db: Database session
        :param patron_id: Patron ID
        :return: Number of credentials invalidated
        """
        credentials = (
            db.query(Credential)
            .filter(
                Credential.patron_id == patron_id,
                Credential.type == self.TOKEN_TYPE,
            )
            .all()
        )

        count = 0
        for credential in credentials:
            credential.expires = utc_now()
            count += 1

        db.commit()

        self.log.info(f"Invalidated {count} credential(s) for patron {patron_id}")
        return count
