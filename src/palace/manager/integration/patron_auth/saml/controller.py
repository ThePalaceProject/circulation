import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import (
    SplitResult,
    parse_qs,
    urlencode,
    urlparse,
    urlsplit,
    urlunparse,
)

import jwt
import sqlalchemy.orm.session
from flask import Request, Response, redirect, request as flask_request, url_for
from flask_babel import lazy_gettext as _
from lxml import etree
from werkzeug import Response as wkResponse
from werkzeug.datastructures import MultiDict

from palace.manager.api.util.flask import get_request_library
from palace.manager.integration.patron_auth.constants import LOGOUT_REDIRECT_QUERY_PARAM
from palace.manager.integration.patron_auth.saml.configuration.problem_details import (
    SAML_INCORRECT_METADATA,
    SAML_METADATA_NOT_CONFIGURED,
)
from palace.manager.integration.patron_auth.saml.configuration.service_provider import (
    SamlServiceProviderConfiguration,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLMetadataParser,
    SAMLMetadataParsingError,
)
from palace.manager.integration.patron_auth.saml.provider import (
    SAMLWebSSOAuthenticationProvider,
)
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.problem_detail import (
    ProblemDetail,
    ProblemDetail as pd,
    ProblemDetailException,
    json as pd_json,
)

if TYPE_CHECKING:
    from palace.manager.api.authenticator import Authenticator
    from palace.manager.api.circulation_manager import CirculationManager

SAML_INVALID_REQUEST = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/saml/invalid-request",
    status_code=401,
    title=_("SAML invalid request."),
    detail=_("SAML invalid request."),
)

SAML_INVALID_RESPONSE = pd(
    "http://palaceproject.io/terms/problem/auth/recoverable/saml/invalid-response",
    status_code=401,
    title=_("SAML invalid response."),
    detail=_("SAML invalid response."),
)

SAML_UNSOLICITED_RESPONSE = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/saml/unsolicited-response",
    status_code=400,
    title=_("Unsolicited SAML response."),
    detail=_(
        "This sign-in did not start from the Palace app or web catalog. "
        "Please sign in from one of those instead."
    ),
)


@dataclass(frozen=True)
class SAMLRelayStateParameters:
    """The parameters the circulation manager adds to the relay state before
    sending it to the IdP, and reads back when the IdP returns it, along with
    the client's redirect URI that remains once they are removed.
    """

    library_short_name: str
    provider_name: str
    idp_entity_id: str
    redirect_uri: str


class SAMLController:
    """Controller used for handing SAML 2.0 authentication requests"""

    ERROR = "error"
    REDIRECT_URI = "redirect_uri"
    PROVIDER_NAME = "provider"
    IDP_ENTITY_ID = "idp_entity_id"
    LIBRARY_SHORT_NAME = "library_short_name"
    SAML_RESPONSE = "SAMLResponse"
    RELAY_STATE = "RelayState"
    ACCESS_TOKEN = "access_token"
    PATRON_INFO = "patron_info"
    LOGOUT_STATUS = "logout_status"
    FORCE_AUTHN = "force_authn"

    VALID_FORCE_AUTHN_VALUES: ClassVar[frozenset[str]] = frozenset({"true", "false"})

    # The parameters we add to the relay state before sending it to the IdP.
    INTERNAL_RELAY_STATE_PARAMETERS: ClassVar[frozenset[str]] = frozenset(
        {LIBRARY_SHORT_NAME, PROVIDER_NAME, IDP_ENTITY_ID}
    )

    _SITE_WIDE_METADATA_CACHE_KEY: ClassVar[str | None] = None
    _sp_metadata_cache: ClassVar[dict[str | None, str | None]] = {}

    @classmethod
    def clear_metadata_cache(cls) -> None:
        """Clear the SP metadata XML processing cache.

        Should be called whenever SAML settings may have changed so that the next
        request re-validates and re-serializes the metadata from scratch.
        """
        cls._sp_metadata_cache.clear()

    @classmethod
    def _validated_sp_metadata(cls, key: str | None, xml: str | None) -> str | None:
        """Validate and normalize SP metadata XML, then store the result in the cache.

        Uses `SAMLMetadataParser` for structural validation, then
        re-serializes via lxml so that the output always includes an XML
        declaration and is free of any leading BOM or extraneous whitespace.

        Results are stored in the cache under *key* (the library short name,
        or ``None`` for site-wide metadata). The raw *xml* is never stored as
        a cache key, keeping the cache memory footprint small.

        :param key: Cache key — the library short name for a specific library,
            or ``None`` for site-wide metadata.
        :param xml: Raw SP metadata XML string to validate and normalize, or
            ``None`` if no metadata is configured (cached as-is).
        :return: Validated and normalized XML string with an XML declaration,
            or ``None`` if *xml* is ``None``.
        :raises SAMLMetadataParsingError: If the XML is not valid SAML metadata.
        """
        if isinstance(xml, str):
            SAMLMetadataParser().parse(xml)
            root = etree.fromstring(xml.encode())
            cls._sp_metadata_cache[key] = etree.tostring(
                root, xml_declaration=True, encoding="UTF-8"
            ).decode()
        else:
            cls._sp_metadata_cache[key] = None
        return cls._sp_metadata_cache[key]

    def __init__(
        self,
        circulation_manager: "CirculationManager",
        authenticator: "Authenticator",
    ) -> None:
        """Initializes a new instance of SAMLController class

        :param circulation_manager: Circulation Manager
        :param authenticator: Authenticator object used to route requests to the appropriate LibraryAuthenticator
        """
        self._circulation_manager = circulation_manager
        self._authenticator = authenticator

        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _add_params_to_url(url, params):
        """Adds parameters as a query part of the URL

        :param url: URL
        :type url: string

        :param params: Dictionary containing parameters
        :type params: Dict

        :return: URL with parameters formatted as a query string
        :rtype: string
        """
        url_parts = urlsplit(url)

        # Extract the existing parameters specified in the redirection URI
        existing_params = parse_qs(url_parts.query)

        # Enrich our custom parameters with the existing ones
        params.update(existing_params)

        new_query = urlencode(params, True)
        url_parts = SplitResult(
            url_parts.scheme,
            url_parts.netloc,
            url_parts.path,
            new_query,
            url_parts.fragment,
        )
        url = url_parts.geturl()

        return url

    def _error_uri(self, redirect_uri, problem_detail):
        """Encodes the given ProblemDetail into the fragment identifier of the given URI

        :param redirect_uri: Redirection URL
        :type redirect_uri: string

        :param problem_detail: ProblemDetail object describing the error
        :type problem_detail: ProblemDetail

        :return: Redirection URL
        :rtype: string
        """
        problem_detail_json = pd_json(
            problem_detail.uri,
            problem_detail.status_code,
            problem_detail.title,
            problem_detail.detail,
            problem_detail.debug_message,
        )
        params = {self.ERROR: problem_detail_json}
        redirect_uri = self._add_params_to_url(redirect_uri, params)

        return redirect_uri

    @staticmethod
    def _get_request_parameter(
        params: Mapping[str, str], name: str, default_value: str | None = None
    ) -> str | ProblemDetail:
        """Returns a parameter contained in the incoming request

        :param params: Request's parameters
        :param name: Name of the parameter
        :param default_value: Optional default value

        :return: Parameter's value, or a ProblemDetail if the parameter is missing or empty
        """
        parameter = params.get(name, default_value)

        if not parameter:
            return SAML_INVALID_REQUEST.detailed(
                _(f"Required parameter {name} is missing")
            )

        return parameter

    @staticmethod
    def _get_relay_state_parameter(
        relay_parameters: Mapping[str, list[str]], name: str
    ) -> str | ProblemDetail:
        """Returns a parameter from the parsed query string of the relay state
        returned by the IdP

        :param relay_parameters: Parsed query string of the relay state
        :param name: Name of the parameter

        :return: Parameter's value, or a ProblemDetail if the parameter is missing
        """
        if name not in relay_parameters:
            return SAMLController._missing_relay_state_parameter(name)

        return relay_parameters[name][0]

    @staticmethod
    def _missing_relay_state_parameter(name: str) -> ProblemDetail:
        """Returns the ProblemDetail for a relay state that lacks one of our
        parameters

        :param name: Name of the missing parameter
        """
        return SAML_INVALID_RESPONSE.detailed(
            _(f"Required parameter {name} is missing from RelayState")
        )

    def _parse_relay_state(
        self, relay_state: str
    ) -> SAMLRelayStateParameters | ProblemDetail | None:
        """Extracts the parameters that saml_authentication_redirect and
        saml_logout_redirect add to the relay state before sending it to the IdP,
        and the client's redirect URI that remains once they are removed.

        :param relay_state: Relay state returned by the IdP

        :return: The parameters; a ProblemDetail if some but not all of them are
            present; or None if none of them are, meaning the relay state is not
            one of ours (an unsolicited response, or one we cannot parse at all)
        """
        try:
            relay_state_parse_result = urlparse(relay_state)
        except ValueError:
            return None
        relay_state_parameters = parse_qs(relay_state_parse_result.query)

        if self.INTERNAL_RELAY_STATE_PARAMETERS.isdisjoint(relay_state_parameters):
            return None

        library_short_name = self._get_relay_state_parameter(
            relay_state_parameters, self.LIBRARY_SHORT_NAME
        )
        if isinstance(library_short_name, ProblemDetail):
            return library_short_name

        provider_name = self._get_relay_state_parameter(
            relay_state_parameters, self.PROVIDER_NAME
        )
        if isinstance(provider_name, ProblemDetail):
            return provider_name

        idp_entity_id = self._get_relay_state_parameter(
            relay_state_parameters, self.IDP_ENTITY_ID
        )
        if isinstance(idp_entity_id, ProblemDetail):
            return idp_entity_id

        # The client's redirect URI is the relay state without our own parameters.
        client_parameters = {
            name: values
            for name, values in relay_state_parameters.items()
            if name not in self.INTERNAL_RELAY_STATE_PARAMETERS
        }
        redirect_uri = urlunparse(
            (
                relay_state_parse_result.scheme,
                relay_state_parse_result.netloc,
                relay_state_parse_result.path,
                relay_state_parse_result.params,
                urlencode(client_parameters, True),
                relay_state_parse_result.fragment,
            )
        )

        return SAMLRelayStateParameters(
            library_short_name=library_short_name,
            provider_name=provider_name,
            idp_entity_id=idp_entity_id,
            redirect_uri=redirect_uri,
        )

    def _patron_web_url(self, url: str) -> str | None:
        """Returns the URL, re-serialized from its parsed parts, if its scheme,
        host, and port match a known patron web client, so it is safe to send a
        browser there. Otherwise returns None.

        :param url: URL to check
        """
        try:
            parts = urlsplit(url.strip())
        except ValueError:
            return None
        if parts.scheme not in ("http", "https") or not parts.netloc:
            return None

        # Match on the whole netloc, so a URL with userinfo in front of a known
        # host does not pass. A "*" entry (development only) never matches, since
        # honoring it would make this an open redirect. A known entry we cannot
        # parse is skipped rather than failing the request.
        origin = (parts.scheme, parts.netloc.lower())
        for known_url in self._circulation_manager.patron_web_domains:
            try:
                known = urlsplit(known_url)
            except ValueError:
                continue
            if origin == (known.scheme, known.netloc.lower()):
                # Serialize from the parsed parts. urlsplit drops tab, CR, and LF,
                # and strip() removed surrounding whitespace, so the characters a
                # Location header cannot carry are gone. Werkzeug percent-encodes
                # any other control character.
                return parts.geturl()

        return None

    def _redirect_unsolicited_response(
        self, relay_state: str | None
    ) -> wkResponse | ProblemDetail:
        """Sends the browser that delivered an unsolicited SAML response to a web
        catalog, where the patron can sign in.

        The IdP may supply its own relay state. It is honored only when it points
        at a known patron web client; its path and query pass through as-is, so
        it can land the patron on a specific library. Otherwise the sitewide
        default web catalog is used. The response itself is not validated: no
        token is issued, and the only destinations are hosts we already trust.

        :param relay_state: Relay state returned by the IdP, if any

        :return: Redirection response, or a ProblemDetail if there is nowhere to send
            the patron
        """
        destination: str | None = (
            self._circulation_manager.services.config.sitewide.patron_web_default_url()
        )

        if relay_state:
            patron_web_url = self._patron_web_url(relay_state)
            if patron_web_url is not None:
                destination = patron_web_url
            else:
                self._logger.warning(
                    "Ignoring the RelayState of an unsolicited SAML response because "
                    f"it is not a known patron web client: {relay_state!r}"
                )

        if destination is None:
            self._logger.warning(
                "Rejecting an unsolicited SAML response: no patron web default URL "
                "is configured"
            )
            return SAML_UNSOLICITED_RESPONSE

        self._logger.info(
            f"Redirecting an unsolicited SAML response to {destination!r}"
        )
        # The response arrives as a POST; 303 makes the browser follow with a GET.
        return redirect(destination, code=303)

    def _redirect_with_error(self, redirect_uri, problem_detail):
        """Redirects the patron to the given URL, with the given ProblemDetail encoded into the fragment identifier

        :param redirect_uri: Redirection URL
        :type redirect_uri: string

        :param problem_detail: ProblemDetail object describing the error
        :type problem_detail: ProblemDetail

        :return: Redirection response
        :rtype: Response
        """
        return redirect(self._error_uri(redirect_uri, problem_detail))

    def saml_authentication_redirect(
        self,
        params: MultiDict[str, str],
        db: sqlalchemy.orm.session.Session,
    ) -> wkResponse | ProblemDetail:
        """Redirects an unauthenticated patron to the authentication URL of the
        appropriate SAML IdP.
        Over on that other site, the patron will authenticate and be
        redirected back to the circulation manager, ending up in
        saml_authentication_callback.

        :param params: Query parameters. In addition to the required `provider`,
            `idp_entity_id`, and `redirect_uri` parameters, an optional
            `force_authn` parameter ("true" or "false") may be supplied to
            request that the IdP re-authenticate the patron even if the patron
            has an existing IdP session.

        :param db: Database session

        :return: Redirection response, or a ProblemDetail on error
        """
        provider_name = self._get_request_parameter(params, self.PROVIDER_NAME)
        if isinstance(provider_name, ProblemDetail):
            return provider_name

        idp_entity_id = self._get_request_parameter(params, self.IDP_ENTITY_ID)
        if isinstance(idp_entity_id, ProblemDetail):
            return idp_entity_id

        redirect_uri = self._get_request_parameter(params, self.REDIRECT_URI)
        if isinstance(redirect_uri, ProblemDetail):
            return redirect_uri

        # Optional parameter. Clients pass force_authn=true to request that the
        # IdP re-authenticate the patron even if the patron has an existing IdP
        # session, e.g. a session for an account from a different tenant.
        force_authn = params.get(self.FORCE_AUTHN)
        if force_authn is not None and force_authn not in self.VALID_FORCE_AUTHN_VALUES:
            return self._redirect_with_error(
                redirect_uri,
                SAML_INVALID_REQUEST.detailed(
                    _(
                        'Invalid %(name)s value; expected "true" or "false"',
                        name=self.FORCE_AUTHN,
                    )
                ),
            )
        force_authn_requested = force_authn == "true"

        provider = self._authenticator.saml_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        authentication_manager = provider.get_authentication_manager()

        # In general relay state should contain only a redirect URL.
        # However, we need to pass additional parameters which will be required in saml_authentication_callback.
        # There is no other way to pass them back to the Circulation Manager from the IdP.
        # We have to add them to the query part of the relay state and then remove them
        # before redirecting to the URL containing in the relay state.
        # The required parameters are:
        # - library's name
        # - SAML provider's name
        # - IdP's entity ID
        relay_state = self._add_params_to_url(
            redirect_uri,
            {
                # NOTE: we cannot use @has_library decorator and append a library's name
                # to SAMLController.saml_callback route (e.g. https://cm.org/LIBRARY_NAME/saml_callback).
                # The URL of the SP's assertion consumer service (SAMLController.saml_callback) should be constant:
                # SP's metadata is registered in the IdP and cannot change.
                # If we try to append a library's name to the ACS's URL sent as a part of the SAML request,
                # the IdP will fail this request because the URL mentioned in the request and
                # the URL saved in the SP's metadata configured in this IdP will differ.
                # Library's name is passed as a part of the relay state and
                # processed in SAMLController.saml_authentication_callback
                self.LIBRARY_SHORT_NAME: provider.library(db).short_name,
                self.PROVIDER_NAME: provider_name,
                self.IDP_ENTITY_ID: idp_entity_id,
            },
        )
        redirect_uri = authentication_manager.start_authentication(
            db, idp_entity_id, relay_state, force_authn=force_authn_requested
        )
        if isinstance(redirect_uri, ProblemDetail):
            return redirect_uri

        return redirect(redirect_uri)

    def saml_authentication_callback(
        self, request: Request, db: sqlalchemy.orm.session.Session
    ) -> wkResponse | ProblemDetail:
        """Creates a Patron object and a bearer token for a patron who has just
        authenticated with one of our SAML IdPs

        :param request: Flask request
        :param db: Database session

        :return: Redirection response or a ProblemDetail if the response is not correct
        """
        # SAMLResponse is what makes this request a SAML response. Check for it before
        # RelayState, which is optional in the SAML POST binding and holds only our own
        # state. That way a request that is not a SAML response at all (a scanner or a
        # bare POST) is reported as such, and nothing in RelayState is acted on
        # before there is a response to validate.
        saml_response = self._get_request_parameter(request.form, self.SAML_RESPONSE)
        if isinstance(saml_response, ProblemDetail):
            return saml_response

        # A response whose RelayState carries none of the parameters we add in
        # saml_authentication_redirect did not start with us, so it is unsolicited.
        # We cannot sign the patron in from it, since no client is waiting for a
        # token, so we send them to a web catalog to sign in from there.
        relay_state = request.form.get(self.RELAY_STATE)
        relay_params = self._parse_relay_state(relay_state) if relay_state else None
        if relay_params is None:
            return self._redirect_unsolicited_response(relay_state)
        if isinstance(relay_params, ProblemDetail):
            return relay_params

        redirect_uri = relay_params.redirect_uri

        library = self._circulation_manager.index_controller.library_for_request(
            relay_params.library_short_name
        )
        if isinstance(library, ProblemDetail):
            return self._redirect_with_error(redirect_uri, library)

        provider = self._authenticator.saml_provider_lookup(relay_params.provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        authentication_manager = provider.get_authentication_manager()

        subject = authentication_manager.finish_authentication(
            db, relay_params.idp_entity_id
        )
        if isinstance(subject, ProblemDetail):
            return self._redirect_with_error(redirect_uri, subject)

        try:
            response = provider.saml_callback(db, subject)
        except ProblemDetailException as e:
            return self._redirect_with_error(redirect_uri, e.problem_detail)

        provider_token, patron, patron_data = response

        # Turn the provider token into a bearer token we can give to
        # the patron
        simplified_token = self._authenticator.create_bearer_token(
            provider.label(), provider_token.credential
        )

        patron_info = json.dumps(patron_data.to_response_parameters)
        params = {"access_token": simplified_token, "patron_info": patron_info}

        redirect_uri = self._add_params_to_url(redirect_uri, params)

        return redirect(redirect_uri)

    def saml_logout_redirect(
        self,
        params: MultiDict[str, str],
        db: sqlalchemy.orm.session.Session,
    ) -> wkResponse | ProblemDetail:
        """Initiate SP-Initiated SAML SLO.

        Validates the patron's bearer token, immediately invalidates their local SAML
        credential, then redirects to the IdP's SLO endpoint (if one is configured).
        Falls back to a local-only logout redirect when the IdP does not support SLO.

        :param params: Query parameters from the request
        :param db: Database session
        :return: Redirect response or ProblemDetail
        """
        provider_name = self._get_request_parameter(params, self.PROVIDER_NAME)
        if isinstance(provider_name, ProblemDetail):
            return provider_name

        post_logout_redirect_uri = self._get_request_parameter(
            params, LOGOUT_REDIRECT_QUERY_PARAM
        )
        if isinstance(post_logout_redirect_uri, ProblemDetail):
            return post_logout_redirect_uri

        auth_header = flask_request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return SAML_INVALID_REQUEST.detailed(
                _("Missing or invalid Authorization header")
            )
        cm_jwt = auth_header.removeprefix("Bearer ")

        library = get_request_library()
        library_authenticator = self._authenticator.library_authenticators.get(
            library.short_name
        )
        if not library_authenticator:
            return SAML_INVALID_REQUEST.detailed(
                _("No authenticator found for library")
            )

        try:
            decoded_provider_name, token_value = (
                library_authenticator.decode_bearer_token(cm_jwt)
            )
        except jwt.exceptions.InvalidTokenError:
            self._logger.warning("Invalid bearer token in SAML logout request")
            return SAML_INVALID_REQUEST.detailed(_("Invalid bearer token"))

        if decoded_provider_name != provider_name:
            return SAML_INVALID_REQUEST.detailed(_("Provider mismatch in bearer token"))

        provider = self._authenticator.saml_provider_lookup(provider_name)
        if isinstance(provider, ProblemDetail):
            return provider

        credential = provider._credential_manager.lookup_saml_token_by_value(
            db, token_value, library.id
        )

        name_id = None
        idp_entity_id = None
        if credential:
            subject = provider._credential_manager.extract_saml_token(credential)
            name_id = subject.name_id
            idp_entity_id = subject.idp
            provider._credential_manager.invalidate_saml_token(db, credential)

        # If we can't identify the IdP or the patron's name ID, do a local-only logout.
        if not name_id or not idp_entity_id:
            final_uri = self._add_params_to_url(
                post_logout_redirect_uri, {self.LOGOUT_STATUS: "success"}
            )
            return redirect(final_uri)

        # Check if the IdP supports SLO.
        auth_manager = provider.get_authentication_manager()
        identity_providers = [
            idp
            for idp in auth_manager.configuration.get_identity_providers(db)
            if idp.entity_id == idp_entity_id
        ]
        if not identity_providers or not identity_providers[0].slo_service:
            final_uri = self._add_params_to_url(
                post_logout_redirect_uri, {self.LOGOUT_STATUS: "success"}
            )
            return redirect(final_uri)

        # Encode library/provider/idp into the relay state so the callback can
        # reconstruct the context without server-side state storage.
        relay_state = self._add_params_to_url(
            post_logout_redirect_uri,
            {
                self.LIBRARY_SHORT_NAME: library.short_name,
                self.PROVIDER_NAME: provider_name,
                self.IDP_ENTITY_ID: idp_entity_id,
            },
        )

        callback_url = url_for("saml_logout_callback", _external=True)
        redirect_url = auth_manager.start_logout(
            db, idp_entity_id, name_id, callback_url, relay_state
        )
        if isinstance(redirect_url, ProblemDetail):
            self._logger.warning(
                "Failed to initiate SAML SLO; falling back to partial logout"
            )
            final_uri = self._add_params_to_url(
                post_logout_redirect_uri, {self.LOGOUT_STATUS: "partial"}
            )
            return redirect(final_uri)

        return redirect(redirect_url)

    def saml_logout_callback(
        self,
        request: Request,
        db: sqlalchemy.orm.session.Session,
    ) -> wkResponse | ProblemDetail:
        """Handle the IdP's LogoutResponse for SP-Initiated SAML SLO.

        Validates the SAMLResponse and redirects the patron back to the
        client app's redirect URI with a `logout_status` query parameter.

        Supports both HTTP-Redirect (GET) and HTTP-POST binding.

        :param request: Flask request object
        :param db: Database session
        :return: Redirect response or ProblemDetail
        """
        # SAMLResponse and RelayState may arrive via GET (HTTP-Redirect) or POST
        # (HTTP-POST). request.values reads the query string for GET, and both the
        # query string and the form body for POST. As in saml_authentication_callback,
        # SAMLResponse is checked first, since RelayState is optional and only carries
        # our own state.
        saml_response = self._get_request_parameter(request.values, self.SAML_RESPONSE)
        if isinstance(saml_response, ProblemDetail):
            return saml_response

        relay_state = request.values.get(self.RELAY_STATE)
        if not relay_state:
            return SAML_INVALID_RESPONSE.detailed(
                _(
                    "Required parameter {} is missing from the logout callback".format(
                        self.RELAY_STATE
                    )
                )
            )

        # Unlike login, an unsolicited logout response has nowhere useful to go,
        # so a relay state that is not ours is reported the same way as one that
        # is missing its first parameter.
        relay_params = self._parse_relay_state(relay_state)
        if relay_params is None:
            return self._missing_relay_state_parameter(self.LIBRARY_SHORT_NAME)
        if isinstance(relay_params, ProblemDetail):
            return relay_params

        redirect_uri = relay_params.redirect_uri

        # Set request.library so invoke_authenticator_method can route correctly.
        library = self._circulation_manager.index_controller.library_for_request(
            relay_params.library_short_name
        )
        if isinstance(library, ProblemDetail):
            return self._redirect_with_error(redirect_uri, library)

        provider = self._authenticator.saml_provider_lookup(relay_params.provider_name)
        if isinstance(provider, ProblemDetail):
            return self._redirect_with_error(redirect_uri, provider)

        auth_manager = provider.get_authentication_manager()
        callback_url = url_for("saml_logout_callback", _external=True)
        result = auth_manager.finish_logout(
            db, relay_params.idp_entity_id, callback_url
        )
        if isinstance(result, ProblemDetail):
            return self._redirect_with_error(redirect_uri, result)

        final_uri = self._add_params_to_url(
            redirect_uri, {self.LOGOUT_STATUS: "success"}
        )
        return redirect(final_uri)

    def saml_sp_metadata(self) -> Response:
        """Returns the SAML SP metadata XML for the active library or system-wide.

        When a library is set in the request context (via the ``allows_library``
        decorator), returns the SP metadata for that library's SAML integration.
        Otherwise returns the system-wide SP metadata from environment configuration.

        :return: Flask response with SP metadata XML.
        :raises ProblemDetailException: If the metadata is not configured or has an
            incorrect format.
        """
        library = get_request_library(default=None)
        key = (
            self._SITE_WIDE_METADATA_CACHE_KEY
            if library is None
            else library.short_name
        )

        # On a cache miss, fetch the raw XML and validate/normalize it into the cache.
        # None (no XML configured) is also cached to avoid redundant lookups on
        # repeated requests. The cache is cleared on settings reload.
        if key not in self._sp_metadata_cache:
            raw_xml = self._load_sp_metadata_xml(library)
            try:
                self._validated_sp_metadata(key, raw_xml)
            except SAMLMetadataParsingError as e:
                message = "SAML metadata has an incorrect format."
                self._logger.error(message)
                raise ProblemDetailException(SAML_INCORRECT_METADATA) from e

        if not (xml := self._sp_metadata_cache[key]):
            message = "SAML metadata is not configured."
            self._logger.error(message)
            raise ProblemDetailException(
                SAML_METADATA_NOT_CONFIGURED.detailed(_(message))
            )

        return Response(xml, 200, {"Content-Type": "application/samlmetadata+xml"})

    def _load_sp_metadata_xml(self, library: Library | None) -> str | None:
        if library is not None:
            provider = self._authenticator.saml_provider_lookup(
                SAMLWebSSOAuthenticationProvider.label()
            )
            if isinstance(provider, ProblemDetail):
                message = (
                    f"Library '{library.name}' ('{library.short_name}')"
                    " is not configured for SAML authentication."
                )
                self._logger.error(message)
                raise ProblemDetailException(
                    SAML_METADATA_NOT_CONFIGURED.detailed(_(message))
                )
            xml = provider.get_sp_metadata_xml()
        else:
            xml = SamlServiceProviderConfiguration().get_metadata()
        return xml
