"""OIDC Controller for handling OIDC authentication requests."""

from __future__ import annotations

import json
import logging
from urllib.parse import SplitResult, parse_qs, urlencode, urlsplit

from flask import redirect
from flask_babel import lazy_gettext as _

from palace.manager.integration.patron_auth.oidc.util import OIDCUtility
from palace.manager.util.problem_detail import (
    ProblemDetail,
    ProblemDetail as pd,
    ProblemDetailException,
    json as pd_json,
)

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


class OIDCController:
    """Controller for handling OIDC authentication requests."""

    ERROR = "error"
    REDIRECT_URI = "redirect_uri"
    PROVIDER_NAME = "provider"
    STATE = "state"
    CODE = "code"
    ACCESS_TOKEN = "access_token"
    PATRON_INFO = "patron_info"

    def __init__(self, circulation_manager, authenticator):
        """Initialize OIDC controller.

        :param circulation_manager: Circulation Manager
        :param authenticator: Authenticator for routing requests to LibraryAuthenticator
        """
        self._circulation_manager = circulation_manager
        self._authenticator = authenticator
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _add_params_to_url(url: str, params: dict) -> str:
        """Add parameters to URL query string.

        :param url: Base URL
        :param params: Parameters to add
        :return: URL with parameters
        """
        url_parts = urlsplit(url)
        existing_params = parse_qs(url_parts.query)
        params.update(existing_params)
        new_query = urlencode(params, True)
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
        params: dict, name: str, default_value: str | None = None
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
    ) -> redirect:
        """Redirect patron to URL with error encoded.

        :param redirect_uri: Redirect URL
        :param problem_detail: Error to include
        :return: Redirect response
        """
        return redirect(self._error_uri(redirect_uri, problem_detail))

    def oidc_authentication_redirect(
        self, params: dict, db
    ) -> redirect | ProblemDetail:
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

        provider = self._authenticator.oidc_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        library = provider.library(db)
        authentication_manager = provider.get_authentication_manager()

        utility = OIDCUtility(redis_client=None)
        nonce = utility.generate_nonce()
        state_data = {
            "library_short_name": library.short_name,
            "provider": provider_name,
            "redirect_uri": redirect_uri,
            "nonce": nonce,
        }

        pkce_data = None
        if provider._settings.use_pkce:
            pkce_data = utility.generate_pkce()
            state_data["code_verifier"] = pkce_data["code_verifier"]

        state = utility.generate_state(state_data)

        from flask import url_for

        callback_uri = url_for("oidc_callback", _external=True)

        code_challenge = pkce_data["code_challenge"] if pkce_data else None

        authorization_url = authentication_manager.build_authorization_url(
            redirect_uri=callback_uri,
            state=state,
            nonce=nonce,
            code_challenge=code_challenge,
        )

        return redirect(authorization_url)

    def oidc_authentication_callback(
        self, params: dict, db
    ) -> redirect | ProblemDetail:
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
            state_data = utility.validate_state(state)
        except Exception as e:
            self._logger.error(f"Invalid state parameter: {e}")
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

        from flask import url_for

        callback_uri = url_for("oidc_callback", _external=True)

        try:
            tokens = authentication_manager.exchange_authorization_code(
                code=code,
                redirect_uri=callback_uri,
                code_verifier=code_verifier,
            )
        except Exception as e:
            self._logger.exception("Token exchange failed")
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
            self._logger.exception("ID token validation failed")
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
