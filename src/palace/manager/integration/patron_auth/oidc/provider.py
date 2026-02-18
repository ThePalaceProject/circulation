"""OIDC Authentication Provider.

This module provides the OIDC authentication provider implementation for patron authentication.
"""

from __future__ import annotations

from collections.abc import Generator
from typing import TYPE_CHECKING, Any

from flask import url_for
from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.manager.api.authentication.base import PatronData, PatronLookupNotSupported
from palace.manager.api.authenticator import BaseOIDCAuthenticationProvider
from palace.manager.integration.patron_auth.oidc.auth import OIDCAuthenticationManager
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.credential import OIDCCredentialManager
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.problem_detail import (
    ProblemDetail as pd,
    ProblemDetailException,
)

if TYPE_CHECKING:
    from palace.manager.core.selftest import SelfTestResult

OIDC_CANNOT_DETERMINE_PATRON = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/cannot-identify-patron",
    status_code=401,
    title=_("Unable to identify patron."),
    detail=_(
        "Unable to determine patron from ID token claims. "
        "This may indicate a service configuration issue."
    ),
)

OIDC_TOKEN_EXPIRED = pd(
    "http://palaceproject.io/terms/problem/auth/recoverable/oidc/session-expired",
    status_code=401,
    title=_("OIDC session expired."),
    detail=_(
        "Your OIDC session has expired. Please reauthenticate via your identity provider."
    ),
)


class OIDCAuthenticationProvider(
    BaseOIDCAuthenticationProvider[OIDCAuthSettings, OIDCAuthLibrarySettings]
):
    """OIDC authentication provider implementing OpenID Connect authentication flow."""

    def __init__(
        self,
        library_id: int,
        integration_id: int,
        settings: OIDCAuthSettings,
        library_settings: OIDCAuthLibrarySettings,
        analytics: Analytics | None = None,
    ):
        """Initialize OIDC authentication provider.

        :param library_id: Library identifier
        :param integration_id: Integration identifier
        :param settings: OIDC authentication settings
        :param library_settings: Library-specific settings
        :param analytics: Analytics service
        """
        super().__init__(
            library_id, integration_id, settings, library_settings, analytics
        )

        self._credential_manager = OIDCCredentialManager()
        self._settings = settings

    @classmethod
    def label(cls) -> str:
        """Return human-readable label for this authentication provider."""
        return "OpenID Connect"

    @classmethod
    def description(cls) -> str:
        """Return human-readable description for this authentication provider."""
        return (
            "OpenID Connect authentication provider supporting standard OIDC flows "
            "with PKCE for enhanced security."
        )

    @property
    def identifies_individuals(self) -> bool:
        """Indicate whether this provider identifies individual patrons."""
        return True

    @classmethod
    def settings_class(cls) -> type[OIDCAuthSettings]:
        """Return the settings class for this provider."""
        return OIDCAuthSettings

    @classmethod
    def library_settings_class(cls) -> type[OIDCAuthLibrarySettings]:
        """Return the library settings class for this provider."""
        return OIDCAuthLibrarySettings

    def get_credential_from_header(self, auth: Authorization) -> str | None:
        """Extract credential from Authorization header.

        For OIDC, the credential is the bearer token stored in our database.

        :param auth: Authorization header data
        :return: Credential token if present, None otherwise
        """
        if auth and auth.type and auth.type.lower() == "bearer" and auth.token:
            return auth.token
        return None

    def _create_authentication_link(self, authenticate_url: str) -> dict[str, Any]:
        """Build an authentication link for an authentication entry."""
        display_name = self._settings.auth_link_display_name or self.label()
        description = self._settings.auth_link_description or display_name

        # Build link with metadata
        link: dict[str, Any] = {
            "rel": "authenticate",
            "href": authenticate_url,
            "display_names": [{"value": display_name, "language": "en"}],
            "descriptions": [{"value": description, "language": "en"}],
            "information_urls": [],
            "privacy_statement_urls": [],
            "logo_urls": [],
        }

        # Add optional fields where provided
        if self._settings.auth_link_information_url:
            link["information_urls"] = [
                {
                    "value": str(self._settings.auth_link_information_url),
                    "language": "en",
                }
            ]
        if self._settings.auth_link_privacy_statement_url:
            link["privacy_statement_urls"] = [
                {
                    "value": str(self._settings.auth_link_privacy_statement_url),
                    "language": "en",
                }
            ]
        if self._settings.auth_link_logo_url:
            link["logo_urls"] = [
                {"value": str(self._settings.auth_link_logo_url), "language": "en"}
            ]

        return link

    def _authentication_flow_document(self, db: Session) -> dict[str, Any]:
        """Build an `authentication` entry suitable for an authentication document.

        :param db: Database session
        :return: Authentication entry
        """
        library = self.library(db)
        if not library:
            raise ValueError("Library not found")

        authenticate_url = url_for(
            "oidc_authenticate",
            _external=True,
            library_short_name=library.short_name,
            provider=self.label(),
        )
        link = self._create_authentication_link(authenticate_url)

        return {
            "type": self.flow_type,
            "description": self.label(),
            "links": [link],
        }

    def _run_self_tests(self, db: Session) -> Generator[SelfTestResult]:
        """Run self-tests for this authentication provider."""
        yield from ()

    def authenticated_patron(
        self, db: Session, token: dict[str, str] | str
    ) -> Patron | pd | None:
        """Authenticate patron using OIDC token.

        :param db: Database session
        :param token: The OIDC bearer token
        :return: Authenticated Patron, None if not found, or ProblemDetail on error
        """
        if not isinstance(token, str):
            return None

        credential = self._credential_manager.lookup_oidc_token_by_value(
            db, token, self.library_id
        )

        if not credential:
            return OIDC_TOKEN_EXPIRED

        auth_manager = self.get_authentication_manager()

        try:
            refreshed_credential = self._credential_manager.refresh_token_if_needed(
                db, credential, auth_manager
            )
            return refreshed_credential.patron
        except Exception as e:
            self.log.warning(f"Failed to refresh OIDC token: {e}")
            return OIDC_TOKEN_EXPIRED

    def get_authentication_manager(self) -> OIDCAuthenticationManager:
        """Return OIDC authentication manager for this provider.

        :return: OIDC authentication manager
        """
        return OIDCAuthenticationManager(self._settings)

    def remote_patron_lookup_from_oidc_claims(
        self, id_token_claims: dict[str, str]
    ) -> PatronData:
        """Create PatronData from ID token claims.

        :param id_token_claims: Validated ID token claims
        :return: PatronData object
        :raises: ProblemDetailException if patron cannot be determined
        """
        patron_id_claim = self._settings.patron_id_claim
        raw_patron_id = id_token_claims.get(patron_id_claim)

        if not raw_patron_id:
            raise ProblemDetailException(problem_detail=OIDC_CANNOT_DETERMINE_PATRON)

        if self._settings.patron_id_regular_expression:
            match = self._settings.patron_id_regular_expression.match(
                str(raw_patron_id)
            )
            if not match or "patron_id" not in match.groupdict():
                raise ProblemDetailException(
                    problem_detail=OIDC_CANNOT_DETERMINE_PATRON
                )
            patron_id = match.group("patron_id")
        else:
            patron_id = str(raw_patron_id)

        return PatronData(
            permanent_id=patron_id,
            authorization_identifier=patron_id,
            external_type="A",
            complete=True,
        )

    def remote_patron_lookup(
        self, patron_or_patrondata: PatronData | Patron
    ) -> PatronData | None:
        """Look up patron information.

        OIDC authentication requires the full OAuth flow, so we cannot perform
        a fresh lookup using only an authorization identifier.

        :param patron_or_patrondata: PatronData or Patron object
        :return: None
        :raises: PatronLookupNotSupported
        """
        raise PatronLookupNotSupported()

    def oidc_callback(
        self,
        db: Session,
        id_token_claims: dict[str, str],
        access_token: str,
        refresh_token: str | None = None,
        expires_in: int | None = None,
    ) -> tuple[Credential, Patron, PatronData]:
        """Handle OIDC callback after successful authentication.

        :param db: Database session
        :param id_token_claims: Validated ID token claims
        :param access_token: Access token from token exchange
        :param refresh_token: Optional refresh token
        :param expires_in: Token expiry in seconds
        :return: 3-tuple (Credential, Patron, PatronData)
        """
        patron_data = self.remote_patron_lookup_from_oidc_claims(id_token_claims)

        patron, is_new = patron_data.get_or_create_patron(
            db, self.library_id, self.analytics
        )

        credential = self._credential_manager.create_oidc_token(
            db,
            patron,
            id_token_claims,
            access_token,
            refresh_token,
            expires_in,
            self._settings.session_lifetime,
        )

        return credential, patron, patron_data
