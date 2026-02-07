"""OIDC Authentication Configuration Models."""

from __future__ import annotations

from re import Pattern
from typing import Annotated

from flask_babel import lazy_gettext as _
from pydantic import (
    HttpUrl,
    PositiveInt,
    field_validator,
    model_validator,
)

from palace.manager.api.admin.problem_details import INCOMPLETE_CONFIGURATION
from palace.manager.api.authentication.base import (
    AuthProviderLibrarySettings,
    AuthProviderSettings,
)
from palace.manager.integration.settings import (
    FormFieldType,
    FormMetadata,
    SettingsValidationError,
)
from palace.manager.util.log import LoggerMixin


class OIDCAuthSettings(AuthProviderSettings, LoggerMixin):
    """OIDC Authentication Provider Settings.

    Configures OpenID Connect (OIDC) authentication for patron authentication.
    Supports Google OAuth 2.0, Keycloak, OpenAthens Keystone, and other
    standard OIDC providers.
    """

    # Discovery & Endpoints
    issuer_url: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("Issuer URL"),
            description=_(
                "OIDC provider's issuer URL (e.g., https://accounts.google.com). "
                "The system will automatically discover endpoints via "
                "/.well-known/openid-configuration. "
                "If provided, this will be used for automatic discovery. "
                "If not provided, you must manually specify all endpoint URLs below."
            ),
        ),
    ] = None

    authorization_endpoint: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("Authorization Endpoint"),
            description=_(
                "OIDC provider's authorization endpoint URL. "
                "Only required if Issuer URL is not provided. "
                "Example: https://accounts.google.com/o/oauth2/v2/auth"
            ),
        ),
    ] = None

    token_endpoint: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("Token Endpoint"),
            description=_(
                "OIDC provider's token endpoint URL. "
                "Only required if Issuer URL is not provided. "
                "Example: https://oauth2.googleapis.com/token"
            ),
        ),
    ] = None

    jwks_uri: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("JWKS URI"),
            description=_(
                "OIDC provider's JSON Web Key Set (JWKS) endpoint URL. "
                "Used for validating ID token signatures. "
                "Only required if Issuer URL is not provided. "
                "Example: https://www.googleapis.com/oauth2/v3/certs"
            ),
        ),
    ] = None

    userinfo_endpoint: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("UserInfo Endpoint (Optional)"),
            description=_(
                "OIDC provider's UserInfo endpoint URL. "
                "Optional - used to fetch additional user claims. "
                "Example: https://openidconnect.googleapis.com/v1/userinfo"
            ),
        ),
    ] = None

    end_session_endpoint: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("End Session Endpoint (Optional)"),
            description=_(
                "OIDC provider's end session endpoint URL for RP-Initiated Logout. "
                "Optional - enables logout functionality if supported by provider. "
                "Automatically discovered if Issuer URL is provided. "
                "Example: https://accounts.google.com/o/oauth2/revoke"
            ),
        ),
    ] = None

    # Client Configuration
    client_id: Annotated[
        str,
        FormMetadata(
            label=_("Client ID"),
            description=_(
                "OAuth 2.0 Client ID assigned by the OIDC provider during registration. "
                "This is a public identifier for your application."
            ),
        ),
    ]

    client_secret: Annotated[
        str,
        FormMetadata(
            label=_("Client Secret"),
            description=_(
                "OAuth 2.0 Client Secret assigned by the OIDC provider. "
                "This is a confidential credential - keep it secure. "
                "Used for authenticating token exchange requests."
            ),
            type=FormFieldType.TEXT,
        ),
    ]

    # Scopes & Claims
    scopes: Annotated[
        list[str],
        FormMetadata(
            label=_("OAuth Scopes"),
            description=_(
                "List of OAuth 2.0 scopes to request. "
                "Must include 'openid' for OIDC compliance. "
                "Common scopes: 'profile', 'email'. "
                "Comma-separated list."
            ),
            type=FormFieldType.LIST,
        ),
    ] = ["openid", "profile", "email"]

    patron_id_claim: Annotated[
        str,
        FormMetadata(
            label=_("Patron ID Claim"),
            description=_(
                "Name of the ID token claim containing the unique patron identifier. "
                "Common values: 'sub' (subject - recommended), 'email', "
                "'preferred_username', 'eduPersonPrincipalName'. "
                "Default: 'sub'"
            ),
        ),
    ] = "sub"

    patron_id_regular_expression: Annotated[
        Pattern[str] | None,
        FormMetadata(
            label=_("Patron ID Regular Expression (Optional)"),
            description=_(
                "Regular expression to extract patron ID from the claim value. "
                "MUST contain a named group 'patron_id'. "
                "Example to extract username from email: "
                "<pre>(?P&lt;patron_id&gt;[^@]+)@example\\.edu</pre>"
                "Leave empty to use the full claim value."
            ),
        ),
    ] = None

    # Session Configuration
    session_lifetime: Annotated[
        PositiveInt | None,
        FormMetadata(
            label=_("Session Lifetime (Days)"),
            description=_(
                "Override the OIDC provider's token lifetime with a custom session duration in days. "
                "Leave empty to use the provider's token expiry. "
                "Note: This only affects the Circulation Manager's session. "
                "Protected content access is still governed by the OIDC provider's tokens."
            ),
        ),
    ] = None

    # Advanced Options
    use_pkce: Annotated[
        bool,
        FormMetadata(
            label=_("Use PKCE (Proof Key for Code Exchange)"),
            description=_(
                "Enable PKCE for additional security during authorization code exchange. "
                "Recommended for all deployments. Required by some providers (Google, Microsoft). "
                "Default: True"
            ),
            type=FormFieldType.SELECT,
            options={
                True: "Enable PKCE (Recommended)",
                False: "Disable PKCE",
            },
        ),
    ] = True

    token_endpoint_auth_method: Annotated[
        str,
        FormMetadata(
            label=_("Token Endpoint Authentication Method"),
            description=_(
                "Method for authenticating to the token endpoint. "
                "Options: 'client_secret_post' (send credentials in request body - recommended), "
                "'client_secret_basic' (send credentials in Authorization header). "
                "Default: 'client_secret_post'"
            ),
            type=FormFieldType.SELECT,
            options={
                "client_secret_post": "Client Secret POST (Recommended)",
                "client_secret_basic": "Client Secret Basic",
            },
        ),
    ] = "client_secret_post"

    access_type: Annotated[
        str,
        FormMetadata(
            label=_("Access Type"),
            description=_(
                "Type of access to request. "
                "'offline' requests a refresh token for long-lived sessions. "
                "'online' for session-only access. "
                "Default: 'offline'"
            ),
            type=FormFieldType.SELECT,
            options={
                "offline": "Offline (with refresh token)",
                "online": "Online (session only)",
            },
        ),
    ] = "offline"

    filter_expression: Annotated[
        str | None,
        FormMetadata(
            label=_("Filter Expression (Optional)"),
            description=_(
                "Python expression to filter patrons based on ID token claims. "
                "Access claims via the 'claims' dictionary. "
                "Example to restrict by email domain: "
                "<pre>claims.get('email', '').endswith('@example.edu')</pre>"
                "Example to check membership: "
                "<pre>'library-patron' in claims.get('groups', [])</pre>"
                "Leave empty to allow all authenticated users."
            ),
            type=FormFieldType.TEXTAREA,
        ),
    ] = None

    # Authentication Link Settings
    auth_link_display_name: Annotated[
        str | None,
        FormMetadata(
            label=_("Authorization Link: Display Name (Optional)"),
            description=_(
                "Human-readable name for this authentication provider shown to patrons. "
                "If not provided, the integration name will be used. "
                "Example: 'University Single Sign-On' or 'Library Login'"
            ),
            weight=1000,
        ),
    ] = None

    auth_link_description: Annotated[
        str | None,
        FormMetadata(
            label=_("Authorization Link: Description (Optional)"),
            description=_(
                "Brief description of this authentication method shown to patrons. "
                "If not provided, the display name will be used. "
                "Example: 'Log in with your university credentials'"
            ),
            type=FormFieldType.TEXTAREA,
            weight=1001,
        ),
    ] = None

    auth_link_logo_url: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("Authorization Link: Logo URL (Optional)"),
            description=_(
                "URL to a logo image representing this authentication provider. "
                "Displayed in authentication selection screens. "
                "Should be a publicly accessible HTTPS URL. "
                "Recommended size: 64x64 pixels or larger."
            ),
            weight=1002,
        ),
    ] = None

    auth_link_information_url: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("Authorization Link: Information URL (Optional)"),
            description=_(
                "URL to a page with more information about this authentication method. "
                "Example: Help page, registration instructions, or provider information."
            ),
            weight=1003,
        ),
    ] = None

    auth_link_privacy_statement_url: Annotated[
        HttpUrl | None,
        FormMetadata(
            label=_("Authorization Link: Privacy Statement URL (Optional)"),
            description=_(
                "URL to the authentication provider's privacy policy or statement. "
                "Helps patrons understand how their data is handled."
            ),
            weight=1004,
        ),
    ] = None

    @model_validator(mode="after")
    def validate_endpoints(self) -> OIDCAuthSettings:
        """Validate that either issuer_url or manual endpoints are provided."""
        if not self.issuer_url:
            # If no issuer_url, require manual endpoint configuration
            if not self.authorization_endpoint:
                raise SettingsValidationError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        "Either 'Issuer URL' for automatic discovery or "
                        "'Authorization Endpoint' must be provided."
                    )
                )
            if not self.token_endpoint:
                raise SettingsValidationError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        "Either 'Issuer URL' for automatic discovery or "
                        "'Token Endpoint' must be provided."
                    )
                )
            if not self.jwks_uri:
                raise SettingsValidationError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        "Either 'Issuer URL' for automatic discovery or "
                        "'JWKS URI' must be provided."
                    )
                )
        return self

    @field_validator("scopes")
    @classmethod
    def validate_scopes(cls, v: list[str]) -> list[str]:
        """Ensure 'openid' scope is present."""
        if "openid" not in v:
            raise SettingsValidationError(
                problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                    "The 'openid' scope is required for OIDC authentication. "
                    "Current scopes: " + ", ".join(v)
                )
            )
        return v

    @field_validator("patron_id_regular_expression")
    @classmethod
    def validate_patron_id_regex(cls, v: Pattern[str] | None) -> Pattern[str] | None:
        """Validate that the regex contains a 'patron_id' named group."""
        if v is not None:
            if "patron_id" not in v.groupindex:
                raise SettingsValidationError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        "Patron ID regular expression must contain a named group 'patron_id'. "
                        "Example: (?P<patron_id>[^@]+)@example\\.edu"
                    )
                )
        return v

    @field_validator("filter_expression")
    @classmethod
    def validate_filter_expression(cls, v: str | None) -> str | None:
        """Validate the filter expression syntax."""
        if v is not None:
            # Try to compile the expression to check for syntax errors
            try:
                compile(v, "<filter_expression>", "eval")
            except SyntaxError as e:
                cls.logger().exception("Invalid filter expression syntax")
                raise SettingsValidationError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        f"Filter expression has invalid syntax: {e.msg}"
                    )
                ) from e
            except Exception as e:
                cls.logger().exception("Unexpected error validating filter expression")
                raise SettingsValidationError(
                    problem_detail=INCOMPLETE_CONFIGURATION.detailed(
                        f"Filter expression validation failed: {str(e)}"
                    )
                ) from e
        return v


class OIDCAuthLibrarySettings(AuthProviderLibrarySettings):
    """OIDC Authentication Library-Level Settings.

    Currently empty (like SAML). Future enhancements may include:
    - Library-specific scope overrides
    - Custom claim mappings
    - Library-specific filter expressions
    """

    ...
