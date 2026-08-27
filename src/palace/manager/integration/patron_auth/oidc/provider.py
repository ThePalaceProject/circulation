"""OIDC Authentication Provider.

This module provides the OIDC authentication provider implementation for patron authentication.
"""

from __future__ import annotations

from collections.abc import Generator, Sequence
from typing import TYPE_CHECKING, Any, Protocol

from flask import url_for
from flask_babel import lazy_gettext as _
from pydantic import HttpUrl
from sqlalchemy.orm import Session
from werkzeug.datastructures import Authorization

from palace.opds.authentication.document import AuthenticateLink, PalaceAuthentication
from palace.opds.authentication.palace import LocalizedValue
from palace.opds.rwpm import Link
from palace.util.exceptions import PalaceValueError
from palace.util.log import LoggerType

from palace.manager.api.authentication.base import PatronData, PatronLookupNotSupported
from palace.manager.api.authenticator import BaseOIDCAuthenticationProvider
from palace.manager.integration.patron_auth.constants import (
    LOGOUT_REDIRECT_QUERY_PARAM,
)
from palace.manager.integration.patron_auth.oidc.auth import (
    OIDCAuthenticationManager,
    OIDCRefreshTokenError,
)
from palace.manager.integration.patron_auth.oidc.configuration.model import (
    OIDCAuthLibrarySettings,
    OIDCAuthSettings,
)
from palace.manager.integration.patron_auth.oidc.credential import OIDCCredentialManager
from palace.manager.integration.patron_auth.oidc.util import (
    OIDCDiscoveryError,
)
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.filter import (
    FilterExpression,
    FilterExpressionError,
    today_utc,
)
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

OIDC_NO_ACCESS_ERROR = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/no-access",
    status_code=401,
    title=_("No access."),
    detail=_("Patron does not have access based on their ID token claims."),
)

OIDC_LIBRARY_NOT_FOUND = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/library-not-found",
    status_code=500,
    title=_("Library not found."),
    detail=_("The library associated with this OIDC integration could not be found."),
)

OIDC_FILTER_EVALUATION_ERROR = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/filter-evaluation-error",
    status_code=500,
    title=_("Filter expression error."),
    detail=_("OIDC patron filter expression could not be evaluated."),
)


def evaluate_patron_filters(
    expressions: Sequence[tuple[str, str]],
    context: dict[str, Any],
    *,
    library: Library,
    claim_names: Sequence[str],
    log: LoggerType,
) -> None:
    """Evaluate labeled filter expressions as an authorization check.

    Every expression must evaluate to True for access to be granted. All
    expressions are always evaluated so that a single log entry can report
    every failing level at once. Callers assemble the evaluation context,
    which lets providers expose whatever data their filters operate on.

    :param expressions: (label, expression) pairs to evaluate
    :param context: Evaluation context passed to each expression
    :param library: Library the patron is authenticating against (for logging)
    :param claim_names: Claim names included in denial log entries
    :param log: Logger to record evaluation results with
    :raises ProblemDetailException: if the patron is denied access or an expression errors
    """
    failing: list[str] = []
    eval_errors: list[FilterExpressionError] = []
    for label, expression in expressions:
        try:
            result = FilterExpression(expression).evaluate(context)
        except FilterExpressionError as exc:
            log.error(
                "Filter [%s] evaluation error for library %s (%s): %s",
                label,
                library.name,
                library.short_name,
                exc,
            )
            failing.append(label)
            eval_errors.append(exc)
            continue
        log.info(
            "Filter [%s] %s for library %s (%s)",
            label,
            "passed" if result else "failed",
            library.name,
            library.short_name,
        )
        if not result:
            failing.append(label)

    if failing:
        log.warning(
            "Access denied for library %s (%s): filter(s) failed: %s; claim names=%s",
            library.name,
            library.short_name,
            ", ".join(failing),
            claim_names,
        )
        if eval_errors:
            raise ProblemDetailException(
                problem_detail=OIDC_FILTER_EVALUATION_ERROR.detailed(
                    "; ".join(str(e) for e in eval_errors)
                )
            ) from eval_errors[0]
        raise ProblemDetailException(problem_detail=OIDC_NO_ACCESS_ERROR)


class OIDCAuthLinkSettings(Protocol):
    """Display attributes for the authenticate link in an authentication document."""

    @property
    def auth_link_display_name(self) -> str | None: ...

    @property
    def auth_link_description(self) -> str | None: ...

    @property
    def auth_link_logo_url(self) -> HttpUrl | None: ...

    @property
    def auth_link_information_url(self) -> HttpUrl | None: ...

    @property
    def auth_link_privacy_statement_url(self) -> HttpUrl | None: ...


def build_authenticate_link(
    settings: OIDCAuthLinkSettings, label: str, authenticate_url: str
) -> AuthenticateLink:
    """Build an authentication link for an authentication entry.

    :param settings: Display attributes for the link
    :param label: Provider label, used when no display name is configured
    :param authenticate_url: URL the client follows to start authentication
    """
    display_name = settings.auth_link_display_name or label
    description = settings.auth_link_description or display_name

    information_urls: list[LocalizedValue] = []
    if settings.auth_link_information_url:
        information_urls = [
            LocalizedValue(value=str(settings.auth_link_information_url), language="en")
        ]
    privacy_statement_urls: list[LocalizedValue] = []
    if settings.auth_link_privacy_statement_url:
        privacy_statement_urls = [
            LocalizedValue(
                value=str(settings.auth_link_privacy_statement_url),
                language="en",
            )
        ]
    logo_urls: list[LocalizedValue] = []
    if settings.auth_link_logo_url:
        logo_urls = [
            LocalizedValue(value=str(settings.auth_link_logo_url), language="en")
        ]

    return AuthenticateLink(
        rel="authenticate",
        href=authenticate_url,
        display_names=[LocalizedValue(value=display_name, language="en")],
        descriptions=[LocalizedValue(value=description, language="en")],
        information_urls=information_urls,
        privacy_statement_urls=privacy_statement_urls,
        logo_urls=logo_urls,
    )


def build_oidc_authentication_document(
    settings: OIDCAuthLinkSettings,
    *,
    flow_type: str,
    label: str,
    library_short_name: str,
    supports_logout: bool,
) -> PalaceAuthentication:
    """Build an `authentication` entry suitable for an authentication document.

    The authenticate and logout links point at the shared OIDC routes, which
    dispatch to the right provider by its label. Any provider registered on
    the OIDC path can therefore use this builder, passing its own flow type
    and label and omitting the logout link when logout is unsupported.

    :param settings: Display attributes for the authenticate link
    :param flow_type: Authentication flow type URI advertised to clients
    :param label: Provider label, used for route dispatch and as the description
    :param library_short_name: Library the entry is generated for
    :param supports_logout: Whether to include a templated logout link
    :return: Authentication entry
    """
    authenticate_url = url_for(
        "oidc_authenticate",
        _external=True,
        library_short_name=library_short_name,
        provider=label,
    )
    links: list[Link] = [build_authenticate_link(settings, label, authenticate_url)]

    if supports_logout:
        logout_url = url_for(
            "oidc_logout",
            _external=True,
            library_short_name=library_short_name,
            provider=label,
        )
        links.append(
            Link(
                rel="logout",
                href=f"{logout_url}{{&{LOGOUT_REDIRECT_QUERY_PARAM}}}",
                templated=True,
            )
        )

    return PalaceAuthentication(
        type=flow_type,
        description=label,
        links=links,
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
        self._library_settings = library_settings
        self._auth_manager: OIDCAuthenticationManager | None = None

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

    @property
    def patron_id_claim(self) -> str:
        """Name of the ID token claim used to identify the patron."""
        return self._settings.patron_id_claim

    @property
    def credential_manager(self) -> OIDCCredentialManager:
        """Credential manager storing this provider's tokens."""
        return self._credential_manager

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

    def _authentication_flow_document(self, db: Session) -> PalaceAuthentication:
        """Build an `authentication` entry suitable for an authentication document.

        :param db: Database session
        :return: Authentication entry
        """
        library = self.library(db)
        if not library:
            raise PalaceValueError("Library not found")

        return build_oidc_authentication_document(
            self._settings,
            flow_type=self.flow_type,
            label=self.label(),
            library_short_name=library.short_name,
            supports_logout=self.get_authentication_manager().supports_logout(),
        )

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
        except OIDCRefreshTokenError as e:
            library = self.library(db)
            lib_label = (
                f"{library.name} ({library.short_name})"
                if library
                else f"library_id={self.library_id}"
            )
            self.log.warning("Failed to refresh OIDC token for %s: %s", lib_label, e)
            return OIDC_TOKEN_EXPIRED

    def get_authentication_manager(self) -> OIDCAuthenticationManager:
        """Return OIDC authentication manager for this provider.

        The manager is cached once provider metadata loads successfully. If
        discovery fails — for example because the IdP is temporarily unreachable
        — the manager is returned uncached so the next call retries from scratch.

        :return: OIDC authentication manager
        """
        if self._auth_manager is not None:
            return self._auth_manager

        manager = OIDCAuthenticationManager(self._settings)
        try:
            manager.get_provider_metadata()
        except OIDCDiscoveryError as e:
            self.log.warning(
                f"Failed to configure OIDC authentication manager: {e}. "
                "Will retry on next request."
            )
            return manager

        self._auth_manager = manager
        return self._auth_manager

    def extract_patron_identifier(self, id_token_claims: dict[str, Any]) -> str | None:
        """Extract the patron identifier from validated token claims.

        Applies the configured patron ID claim and optional extraction
        regular expression. Logout paths use this so their patron lookups
        match the identifier stored at login.

        :param id_token_claims: Validated token claims
        :return: Patron identifier, or None if it cannot be determined
        """
        patron_id_claim = self.patron_id_claim
        id_token_claim_names = list(id_token_claims.keys())
        raw_patron_id = id_token_claims.get(patron_id_claim)

        if not raw_patron_id:
            self.log.error(
                "Failed to extract patron ID: claim '%s' not found; token claims: %s",
                patron_id_claim,
                id_token_claim_names,
            )
            return None

        if self._settings.patron_id_regular_expression:
            match = self._settings.patron_id_regular_expression.match(
                str(raw_patron_id)
            )
            if not match or "patron_id" not in match.groupdict():
                # raw_patron_id is intentionally included to aid regex debugging.
                self.log.warning(
                    "Failed to extract patron ID: value %r for claim '%s' did not match pattern",
                    raw_patron_id,
                    patron_id_claim,
                )
                return None
            patron_id = match.group("patron_id")
        else:
            patron_id = str(raw_patron_id)

        self.log.info(
            "Extracted patron ID '%s' from claim '%s'; token claims: %s",
            patron_id,
            patron_id_claim,
            id_token_claim_names,
        )
        return patron_id

    def remote_patron_lookup_from_oidc_claims(
        self, id_token_claims: dict[str, Any]
    ) -> PatronData:
        """Create PatronData from ID token claims.

        :param id_token_claims: Validated ID token claims
        :return: PatronData object
        :raises: ProblemDetailException if patron cannot be determined
        """
        patron_id = self.extract_patron_identifier(id_token_claims)
        if patron_id is None:
            raise ProblemDetailException(problem_detail=OIDC_CANNOT_DETERMINE_PATRON)

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

    def _filter_claims(self, db: Session, id_token_claims: dict[str, Any]) -> None:
        """Apply the configured filter expression as an authorization check.

        The integration-level expression is evaluated first, then the library-level
        expression. Both must evaluate to True for access to be granted. The
        evaluation context exposes ``claims`` (ID token claims dict),
        ``integration`` (fields from the integration settings marked with
        ``patron_auth_filter_context=True``, including ``extra_data``),
        ``library`` (``id``, ``name``, ``short_name``), and ``today_utc``
        (current UTC date: ``year``, ``month``, ``day``). The context is built
        once per authentication and shared by both expressions, so they always
        evaluate against identical data, including the date.

        All configured expressions are always evaluated so that a single log entry
        can report every failing level at once.

        :param db: Database session
        :param id_token_claims: Validated ID token claims from the OIDC provider
        :raises ProblemDetailException: if the patron is denied access or an expression errors
        """
        labeled_expressions = [
            (label, e)
            for label, e in (
                ("integration", self._settings.filter_expression),
                ("library", self._library_settings.filter_expression),
            )
            if e is not None
        ]
        if not labeled_expressions:
            return

        id_token_claim_names = list(id_token_claims.keys())
        library = self.library(db)
        if library is None:
            raise ProblemDetailException(problem_detail=OIDC_LIBRARY_NOT_FOUND)
        self.log.debug(
            "Evaluating filter expression for library %s (%s) with claims: %s",
            library.name,
            library.short_name,
            id_token_claim_names,
        )
        # One shared context for both integration and library expressions.
        context = {
            "claims": id_token_claims,
            "integration": self._settings.filter_context_dump(),
            "library": {
                "short_name": library.short_name,
                "name": library.name,
                "id": library.id,
            },
            "today_utc": today_utc(),
        }

        evaluate_patron_filters(
            labeled_expressions,
            context,
            library=library,
            claim_names=id_token_claim_names,
            log=self.log,
        )

    def oidc_callback(
        self,
        db: Session,
        id_token_claims: dict[str, Any],
        access_token: str,
        refresh_token: str | None = None,
        expires_in: int | None = None,
        id_token: str | None = None,
    ) -> tuple[Credential, Patron, PatronData]:
        """Handle OIDC callback after successful authentication.

        :param db: Database session
        :param id_token_claims: Validated ID token claims
        :param access_token: Access token from token exchange
        :param refresh_token: Optional refresh token
        :param expires_in: Token expiry in seconds
        :param id_token: Raw ID token JWT (stored for use as id_token_hint on logout)
        :return: 3-tuple (Credential, Patron, PatronData)
        """
        self._filter_claims(db, id_token_claims)

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
            id_token,
        )

        library = self.library(db)
        lib_name = library.name if library else str(self.library_id)
        lib_short = library.short_name if library else "unknown"
        self.log.info(
            "OIDC patron access granted for library %s (%s): patron=%s",
            lib_name,
            lib_short,
            patron_data.authorization_identifier,
        )

        return credential, patron, patron_data
