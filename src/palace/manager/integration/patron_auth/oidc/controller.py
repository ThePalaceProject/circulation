"""OIDC Controller for handling OIDC authentication requests."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING
from urllib.parse import SplitResult, parse_qs, urlencode, urlsplit

from flask import redirect, url_for
from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session
from werkzeug.wrappers import Response as BaseResponse

from palace.manager.api.authenticator import BaseOIDCAuthenticationProvider
from palace.manager.integration.patron_auth.oidc.util import OIDCUtility
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import (
    ProblemDetail,
    ProblemDetail as pd,
    ProblemDetailException,
    json as pd_json,
)

if TYPE_CHECKING:
    from palace.manager.api.authenticator import Authenticator
    from palace.manager.api.circulation_manager import CirculationManager

OIDC_INVALID_REQUEST = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/invalid-request",
    status_code=400,
    title=_("OIDC invalid request."),
    detail=_("The OIDC authentication request is missing required parameters."),
)

OIDC_INVALID_RESPONSE = pd(
    "http://palaceproject.io/terms/problem/auth/recoverable/oidc/invalid-response",
    status_code=401,
    title=_("OIDC invalid response."),
    detail=_("The OIDC provider response is invalid or incomplete."),
)

OIDC_INVALID_STATE = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/invalid-state",
    status_code=401,
    title=_("OIDC invalid state."),
    detail=_("The state parameter is invalid or has expired. Please try again."),
)

OIDC_LOGOUT_NOT_SUPPORTED = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/logout-not-supported",
    status_code=400,
    title=_("OIDC logout not supported."),
    detail=_("The OIDC provider does not support RP-Initiated Logout."),
)

OIDC_INVALID_ID_TOKEN_HINT = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/invalid-id-token-hint",
    status_code=400,
    title=_("Invalid ID token hint."),
    detail=_("The ID token hint is missing or invalid."),
)

OIDC_INVALID_LOGOUT_TOKEN = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/invalid-logout-token",
    status_code=400,
    title=_("Invalid logout token."),
    detail=_("The logout token is missing or invalid."),
)

OIDC_BACKCHANNEL_LOGOUT_NOT_SUPPORTED = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/oidc/backchannel-logout-not-supported",
    status_code=501,
    title=_("Back-channel logout not supported."),
    detail=_("The OIDC provider does not support back-channel logout."),
)


class OIDCController(LoggerMixin):
    """Controller for handling OIDC authentication requests."""

    ERROR = "error"
    REDIRECT_URI = "redirect_uri"
    PROVIDER_NAME = "provider"
    STATE = "state"
    CODE = "code"
    ACCESS_TOKEN = "access_token"
    PATRON_INFO = "patron_info"
    ID_TOKEN_HINT = "id_token_hint"
    POST_LOGOUT_REDIRECT_URI = "post_logout_redirect_uri"
    LOGOUT_STATUS = "logout_status"
    LOGOUT_TOKEN = "logout_token"

    def __init__(
        self, circulation_manager: CirculationManager, authenticator: Authenticator
    ) -> None:
        """Initialize OIDC controller.

        :param circulation_manager: Circulation Manager
        :param authenticator: Authenticator for routing requests to LibraryAuthenticator
        """
        self._circulation_manager = circulation_manager
        self._authenticator = authenticator

    def _add_params_to_url(self, url: str, params: dict[str, str]) -> str:
        """Add parameters to URL query string.

        New params will override any existing `url` query params with the same key.
        A warning will be logged if there are collisions.

        :param url: Base URL
        :param params: Parameters to add
        :return: URL with parameters
        """
        url_parts = urlsplit(url)
        existing_params_raw = parse_qs(url_parts.query)
        existing_params = {k: v[0] for k, v in existing_params_raw.items()}
        collisions = set(params.keys()) & set(existing_params.keys())
        if collisions:
            self.log.warning(f"Parameter collision in redirect_uri: {collisions}")
        existing_params.update(params)
        new_query = urlencode(existing_params, True)
        url_parts = SplitResult(
            url_parts.scheme,
            url_parts.netloc,
            url_parts.path,
            new_query,
            url_parts.fragment,
        )
        return url_parts.geturl()

    def _error_uri(self, redirect_uri: str, problem_detail: ProblemDetail) -> str:
        """Encode ProblemDetail into redirect URI.

        :param redirect_uri: Redirect URL
        :param problem_detail: Error to encode
        :return: Redirect URL with error
        """
        problem_detail_json = pd_json(
            problem_detail.uri,
            problem_detail.status_code,
            problem_detail.title,
            problem_detail.detail,
            problem_detail.debug_message,
        )
        params = {self.ERROR: problem_detail_json}
        return self._add_params_to_url(redirect_uri, params)

    @staticmethod
    def _get_request_parameter(
        params: dict[str, str], name: str, default_value: str | None = None
    ) -> str | ProblemDetail:
        """Get parameter from request.

        :param params: Request parameters
        :param name: Parameter name
        :param default_value: Optional default value
        :return: Parameter value or ProblemDetail if missing
        """
        parameter = params.get(name, default_value)

        if not parameter:
            return OIDC_INVALID_REQUEST.detailed(
                _(f"Required parameter {name} is missing")
            )

        return parameter

    def _redirect_with_error(
        self, redirect_uri: str, problem_detail: ProblemDetail
    ) -> BaseResponse:
        """Redirect patron to URL with error encoded.

        :param redirect_uri: Redirect URL
        :param problem_detail: Error to include
        :return: Redirect response
        """
        return redirect(self._error_uri(redirect_uri, problem_detail))

    def oidc_authentication_redirect(
        self, params: dict[str, str], db: Session
    ) -> BaseResponse | ProblemDetail:
        """Redirect patron to OIDC provider for authentication.

        :param params: Query parameters including provider and redirect_uri
        :param db: Database session
        :return: Redirect to OIDC provider or ProblemDetail on error
        """
        provider_name = self._get_request_parameter(params, self.PROVIDER_NAME)
        if isinstance(provider_name, ProblemDetail):
            return provider_name

        redirect_uri = self._get_request_parameter(params, self.REDIRECT_URI)
        if isinstance(redirect_uri, ProblemDetail):
            return redirect_uri

        # TODO: Validate redirect_uri against configured patron web client URLs
        #  to prevent open redirect attacks. This should be done consistently
        #  for both OIDC and SAML.

        provider = self._authenticator.oidc_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        library = provider.library(db)
        authentication_manager = provider.get_authentication_manager()

        utility = OIDCUtility(redis_client=None)
        nonce = utility.generate_nonce()
        state_data: dict[str, str] = {
            "library_short_name": library.short_name,
            "provider": provider_name,
            "redirect_uri": redirect_uri,
            "nonce": nonce,
        }

        code_challenge = None
        if provider._settings.use_pkce:
            code_verifier, code_challenge = utility.generate_pkce()
            state_data["code_verifier"] = code_verifier

        library_authenticator = self._authenticator.library_authenticators[
            library.short_name
        ]
        secret = library_authenticator.bearer_token_signing_secret
        state = utility.generate_state(state_data, secret)

        callback_uri = url_for("oidc_callback", _external=True)

        authorization_url = authentication_manager.build_authorization_url(
            redirect_uri=callback_uri,
            state=state,
            nonce=nonce,
            code_challenge=code_challenge,
        )

        return redirect(authorization_url)

    def oidc_authentication_callback(
        self, params: dict[str, str], db: Session
    ) -> BaseResponse | ProblemDetail:
        """Handle OIDC callback after authentication.

        :param params: Query parameters including code and state
        :param db: Database session
        :return: Redirect to client app with token or ProblemDetail on error
        """
        code = self._get_request_parameter(params, self.CODE)
        if isinstance(code, ProblemDetail):
            return OIDC_INVALID_RESPONSE.detailed(_("Missing authorization code"))

        state = self._get_request_parameter(params, self.STATE)
        if isinstance(state, ProblemDetail):
            return OIDC_INVALID_RESPONSE.detailed(_("Missing state parameter"))

        utility = OIDCUtility(redis_client=None)

        try:
            # Get the bearer token signing secret from any library authenticator
            # (they all share the same global secret)
            library_authenticator = next(
                iter(self._authenticator.library_authenticators.values())
            )
            secret = library_authenticator.bearer_token_signing_secret
            state_data = utility.validate_state(state, secret)
        except Exception as e:
            self.log.error(f"Invalid state parameter: {e}")
            return OIDC_INVALID_STATE

        library_short_name = state_data.get("library_short_name")
        provider_name = state_data.get("provider")
        redirect_uri = state_data.get("redirect_uri")
        nonce = state_data.get("nonce")
        code_verifier = state_data.get("code_verifier")

        if not library_short_name or not provider_name or not redirect_uri:
            return OIDC_INVALID_STATE

        library = self._circulation_manager.index_controller.library_for_request(
            library_short_name
        )
        if isinstance(library, ProblemDetail):
            return self._redirect_with_error(redirect_uri, library)

        provider = self._authenticator.oidc_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        authentication_manager = provider.get_authentication_manager()

        callback_uri = url_for("oidc_callback", _external=True)

        try:
            tokens = authentication_manager.exchange_authorization_code(
                code=code,
                redirect_uri=callback_uri,
                code_verifier=code_verifier,
            )
        except Exception as e:
            self.log.exception("Token exchange failed")
            error = OIDC_INVALID_RESPONSE.detailed(
                _("Failed to exchange authorization code for tokens")
            )
            return self._redirect_with_error(redirect_uri, error)

        id_token = tokens.get("id_token")
        if not id_token:
            error = OIDC_INVALID_RESPONSE.detailed(_("Missing ID token in response"))
            return self._redirect_with_error(redirect_uri, error)

        try:
            id_token_claims = authentication_manager.validate_id_token(
                id_token, nonce=nonce
            )
        except Exception as e:
            self.log.exception("ID token validation failed")
            error = OIDC_INVALID_RESPONSE.detailed(_("ID token validation failed"))
            return self._redirect_with_error(redirect_uri, error)

        try:
            credential, patron, patron_data = provider.oidc_callback(
                db,
                id_token_claims,
                tokens.get("access_token"),
                tokens.get("refresh_token"),
                tokens.get("expires_in"),
            )
        except ProblemDetailException as e:
            return self._redirect_with_error(redirect_uri, e.problem_detail)

        simplified_token = self._authenticator.create_bearer_token(
            provider.label(), credential.credential
        )

        patron_info = json.dumps(patron_data.to_response_parameters)
        result_params = {
            self.ACCESS_TOKEN: simplified_token,
            self.PATRON_INFO: patron_info,
        }

        final_redirect_uri = self._add_params_to_url(redirect_uri, result_params)

        return redirect(final_redirect_uri)

    def oidc_logout_initiate(
        self, request_args: dict[str, str], db: Session
    ) -> BaseResponse | ProblemDetail:
        """Initiate OIDC RP-Initiated Logout flow.

        :param request_args: Request arguments from Flask
        :param db: Database session
        :return: Redirect to provider logout endpoint or error
        """
        provider_name = request_args.get(self.PROVIDER_NAME)
        id_token_hint = request_args.get(self.ID_TOKEN_HINT)
        post_logout_redirect_uri = request_args.get(self.POST_LOGOUT_REDIRECT_URI)

        if not provider_name:
            return OIDC_INVALID_REQUEST.detailed(
                _("Missing 'provider' parameter in logout request")
            )

        if not id_token_hint:
            return OIDC_INVALID_ID_TOKEN_HINT.detailed(
                _("Missing 'id_token_hint' parameter in logout request")
            )

        if not post_logout_redirect_uri:
            return OIDC_INVALID_REQUEST.detailed(
                _("Missing 'post_logout_redirect_uri' parameter in logout request")
            )

        library = self._circulation_manager.index_controller.library_for_request(None)
        if isinstance(library, ProblemDetail):
            return library

        library_authenticator = self._authenticator.library_authenticators.get(
            library.short_name
        )
        if not library_authenticator:
            return OIDC_INVALID_REQUEST.detailed(
                _("No authenticator found for library")
            )

        provider = library_authenticator.oidc_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return provider

        try:
            auth_manager = provider._authentication_manager_factory.create(  # type: ignore[attr-defined]
                provider._settings  # type: ignore[attr-defined]
            )

            claims = auth_manager.validate_id_token_hint(id_token_hint)
        except Exception as e:
            self.log.exception("ID token hint validation failed")
            return OIDC_INVALID_ID_TOKEN_HINT.detailed(
                _(f"ID token hint validation failed: {str(e)}")
            )

        patron_identifier = claims.get(provider._settings.patron_id_claim)  # type: ignore[attr-defined]
        if not patron_identifier:
            return OIDC_INVALID_ID_TOKEN_HINT.detailed(
                _("ID token hint missing patron identifier claim")
            )

        try:
            credential_manager = provider._credential_manager  # type: ignore[attr-defined]
            patron = credential_manager.lookup_patron_by_identifier(
                db, patron_identifier
            )

            if patron:
                credential_manager.invalidate_patron_credentials(db, patron.id)
                self.log.info(f"Invalidated credentials for patron {patron_identifier}")
            else:
                self.log.warning(f"Patron not found for identifier {patron_identifier}")

        except Exception as e:
            self.log.exception("Failed to invalidate credentials")

        callback_url = url_for("oidc_logout_callback", _external=True)

        logout_state_data = {
            "provider_name": provider_name,
            "redirect_uri": post_logout_redirect_uri,
            "library_short_name": library.short_name,
        }
        logout_state = OIDCUtility.generate_state(
            logout_state_data, library_authenticator.bearer_token_signing_secret
        )

        utility = OIDCUtility(self._circulation_manager.services.redis.client())
        utility.store_logout_state(logout_state, post_logout_redirect_uri)

        try:
            logout_url = auth_manager.build_logout_url(
                id_token_hint, callback_url, logout_state
            )
        except Exception as e:
            self.log.exception("Failed to build logout URL")
            return OIDC_LOGOUT_NOT_SUPPORTED.detailed(_(str(e)))

        return redirect(logout_url)

    def oidc_logout_callback(
        self, request_args: dict[str, str], db: Session
    ) -> BaseResponse | ProblemDetail:
        """Handle OIDC logout callback.

        :param request_args: Request arguments from Flask
        :param db: Database session
        :return: Redirect to client redirect_uri or error
        """
        state = request_args.get(self.STATE)

        if not state:
            return OIDC_INVALID_REQUEST.detailed(
                _("Missing 'state' parameter in logout callback")
            )

        utility = OIDCUtility(self._circulation_manager.services.redis.client())
        logout_data = utility.retrieve_logout_state(state, delete=False)

        if not logout_data:
            return OIDC_INVALID_STATE.detailed(
                _("Logout state not found or expired. Please try again.")
            )

        redirect_uri = logout_data.get("redirect_uri")
        if not redirect_uri:
            return OIDC_INVALID_STATE.detailed(
                _("Missing redirect_uri in logout state")
            )

        library_short_name = logout_data.get("library_short_name")
        if not library_short_name:
            return OIDC_INVALID_STATE.detailed(_("Missing library in logout state"))

        library_authenticator = self._authenticator.library_authenticators.get(
            library_short_name
        )
        if not library_authenticator:
            return OIDC_INVALID_REQUEST.detailed(
                _("No authenticator found for library")
            )

        try:
            state_data = OIDCUtility.validate_state(
                state, library_authenticator.bearer_token_signing_secret
            )
        except Exception as e:
            self.log.exception("Logout state validation failed")
            return OIDC_INVALID_STATE.detailed(_(f"State validation failed: {str(e)}"))

        utility.delete_logout_state(state)

        result_params = {self.LOGOUT_STATUS: "success"}
        final_redirect_uri = self._add_params_to_url(redirect_uri, result_params)

        return redirect(final_redirect_uri)

    def oidc_backchannel_logout(
        self, request_form: dict[str, str], db: Session
    ) -> tuple[str, int]:
        """Handle OIDC back-channel logout request from provider.

        :param request_form: POST form data from OIDC provider
        :param db: Database session
        :return: Tuple of (response body, status code)
        """
        logout_token = request_form.get(self.LOGOUT_TOKEN)

        if not logout_token:
            self.log.warning("Back-channel logout request missing logout_token")
            return "", 400

        # We need to determine which provider sent this logout token
        # Try all configured OIDC providers until we find one that can validate the token
        for (
            library_authenticator
        ) in self._authenticator.library_authenticators.values():
            # Get all OIDC providers for this library
            for provider in library_authenticator.providers:
                # Skip non-OIDC providers
                if not isinstance(provider, BaseOIDCAuthenticationProvider):
                    continue

                try:
                    auth_manager = provider._authentication_manager_factory.create(  # type: ignore[attr-defined]
                        provider._settings  # type: ignore[attr-defined]
                    )

                    # Try to validate the logout token with this provider
                    claims = auth_manager.validate_logout_token(logout_token)

                    # Successfully validated - get patron identifier
                    patron_identifier = claims.get(
                        provider._settings.patron_id_claim  # type: ignore[attr-defined]
                    )
                    if not patron_identifier:
                        self.log.warning("Logout token missing patron identifier claim")
                        return "", 400

                    # Invalidate patron credentials
                    credential_manager = provider._credential_manager  # type: ignore[attr-defined]
                    patron = credential_manager.lookup_patron_by_identifier(
                        db, patron_identifier
                    )

                    if patron:
                        credential_manager.invalidate_patron_credentials(db, patron.id)
                        self.log.info(
                            f"Back-channel logout: invalidated credentials for patron {patron_identifier}"
                        )
                    else:
                        self.log.warning(
                            f"Back-channel logout: patron not found for identifier {patron_identifier}"
                        )

                    return "", 200

                except Exception as e:
                    # This provider couldn't validate the token, try the next one
                    self.log.debug(
                        f"Provider {provider.label()} could not validate logout token: {e}"
                    )
                    continue

        # No provider could validate the logout token
        self.log.error("No OIDC provider could validate the logout token")
        return "", 400
