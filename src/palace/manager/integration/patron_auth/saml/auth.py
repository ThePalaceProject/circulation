from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from flask import request
from flask_babel import lazy_gettext as _
from lxml.etree import tostring
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.authn_request import OneLogin_Saml2_Authn_Request
from onelogin.saml2.errors import OneLogin_Saml2_Error
from onelogin.saml2.utils import OneLogin_Saml2_Utils
from onelogin.saml2.xmlparser import fromstring

from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLConfigurationError,
    SAMLOneLoginConfiguration,
)
from palace.manager.integration.patron_auth.saml.configuration.service_provider import (
    SamlServiceProviderConfiguration,
)
from palace.manager.integration.patron_auth.saml.metadata.model import (
    SAMLACSSelectionPolicy,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLSubjectParser,
)
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetail as pd

if TYPE_CHECKING:
    import sqlalchemy.orm.session

    from palace.manager.integration.patron_auth.saml.configuration.model import (
        SAMLWebSSOAuthSettings,
    )
    from palace.manager.integration.patron_auth.saml.metadata.model import SAMLNameID

SAML_GENERIC_ERROR = pd(
    "http://librarysimplified.org/terms/problem/saml/generic-error",
    status_code=500,
    title=_("SAML error."),
    detail=_("SAML error."),
)

SAML_INCORRECT_RESPONSE = pd(
    "http://librarysimplified.org/terms/problem/saml/incorrect-response",
    status_code=400,
    title=_("SAML incorrect response."),
    detail=_("SAML incorrect response."),
)

SAML_AUTHENTICATION_ERROR = pd(
    "http://palaceproject.io/terms/problem/auth/recoverable/saml/authentication-failed",
    status_code=401,
    title=_("SAML identity provider authentication failed."),
    detail=_(
        "Authentication with your identity provider failed. Please try again or contact your library for assistance."
    ),
)

SAML_NO_ACCESS_ERROR = pd(
    "http://palaceproject.io/terms/problem/auth/unrecoverable/saml/no-access",
    status_code=401,
    title=_("No access."),
    detail=_("Patron does not have access based on their attributes."),
)


# The SAML toolkit is untyped, so its classes are Any and cannot normally be subclassed.
class AcsOmittingAuthnRequest(OneLogin_Saml2_Authn_Request):  # type: ignore[misc]
    """An AuthnRequest that names no AssertionConsumerService endpoint.

    SAML 2.0 Core section 3.4.1 permits a request to omit both
    AssertionConsumerServiceURL and ProtocolBinding, in which case the IdP responds to
    the endpoint it has registered for the SP. The toolkit always writes both, so they
    are removed after the request is built.
    """

    ACS_ATTRIBUTES = ("AssertionConsumerServiceURL", "ProtocolBinding")

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # get_request() and the redirect signature are both derived from this value, so
        # stripping it here keeps the signature over what is actually sent.
        root = fromstring(self.get_xml().encode())

        for attribute in self.ACS_ATTRIBUTES:
            root.attrib.pop(attribute, None)

        self._authn_request = tostring(root, encoding="unicode")


class SAMLAuthenticationManager:
    """Implements SAML authentication process."""

    def __init__(
        self,
        configuration: SAMLOneLoginConfiguration,
        subject_parser: SAMLSubjectParser,
    ) -> None:
        """Initialize a new instance of SAMLAuthenticationManager.

        :param configuration: OneLoginConfiguration object
        :type configuration: api.saml.configuration.model.SAMLOneLoginConfiguration

        :param subject_parser: Subject parser
        :type subject_parser: api.saml.metadata.parser.SAMLSubjectParser
        """
        if not isinstance(configuration, SAMLOneLoginConfiguration):
            raise ValueError(
                "Argument 'configuration' must be an instance of {} class".format(
                    SAMLOneLoginConfiguration
                )
            )
        if not isinstance(subject_parser, SAMLSubjectParser):
            raise ValueError(
                "Argument 'subject_parser' must be an instance of {} class".format(
                    SAMLSubjectParser
                )
            )

        self._configuration = configuration
        self._subject_parser = subject_parser

        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _get_request_data() -> dict[str, Any]:
        """Map Flask request to what the SAML toolkit expects.

        :return: Dictionary containing information about the request in the format SAML toolkit expects
        :rtype: Dict
        """
        # If server is behind proxys or balancers use the HTTP_X_FORWARDED fields
        url_data = urlparse(request.url)

        request_data = {
            "https": "on" if request.scheme == "https" else "off",
            "http_host": request.host,
            "server_port": url_data.port,
            "script_name": request.path,
            "get_data": request.args.copy(),
            # Uncomment if using ADFS as IdP, https://github.com/onelogin/python-saml/pull/144
            # 'lowercase_urlencoding': True,
            "post_data": request.form.copy(),
        }

        # For HTTP-Redirect binding, pass the raw query string to OneLogin for exact
        # signature validation to avoid potential encoding mismatches between what
        # was signed and what we would otherwise reconstruct from decoded parameters.
        if request.query_string:
            request_data["query_string"] = request.query_string.decode("utf-8")
            request_data["validate_signature_from_qs"] = True

        return request_data

    @staticmethod
    def _get_asserted_acs_url(auth: OneLogin_Saml2_Auth) -> str | None:
        """Return the ACS URL that we send in our authentication requests.

        This is the endpoint selected from the SP metadata by the configuration
        layer. It is the value an IdP compares against its own registered ACS URL.

        :param auth: OneLogin_Saml2_Auth object
        :return: Asserted ACS URL, or None if none is configured
        """
        acs = auth.get_settings().get_sp_data().get("assertionConsumerService", {})
        url = acs.get("url")

        return url if isinstance(url, str) and url else None

    def _warn_on_acs_endpoint_mismatch(
        self, idp_entity_id: str, asserted_url: str | None, received_url: str
    ) -> None:
        """Warn when a response arrives at an ACS endpoint that we did not expect.

        An SP may publish several AssertionConsumerService endpoints, and a request
        names at most one of them. Requests naming none leave nothing to compare, so
        they are ignored. Otherwise an IdP registered against a different endpoint
        fails authentication with an IdP-authored message that names both URLs but
        not which side chose which, so recording both makes the mismatch diagnosable
        from our own logs.
        """
        if asserted_url is None:
            return

        # Normalize like OneLogin, so that we get only legitimate warnings.
        if OneLogin_Saml2_Utils.normalize_url(
            asserted_url
        ) == OneLogin_Saml2_Utils.normalize_url(received_url):
            return

        self._logger.warning(
            f"SAML response for IdP '{idp_entity_id}' arrived at '{received_url}', but the "
            f"authentication request asserted '{asserted_url}'. The identity provider is "
            f"likely registered against a different Assertion Consumer Service endpoint "
            f"than the one selected from our Service Provider metadata."
        )

    def _create_auth_object(
        self,
        db: sqlalchemy.orm.session.Session,
        idp_entity_id: str,
        settings: dict[str, Any] | None = None,
    ) -> OneLogin_Saml2_Auth:
        """Create and initialize an OneLogin_Saml2_Auth object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :param settings: Optional pre-built settings dict. When provided, skips
            `get_settings` so callers can supply custom settings (e.g. SLO settings).
        :type settings: dict | None

        :return: OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """
        request_data = self._get_request_data()
        if settings is None:
            settings = self._configuration.get_settings(db, idp_entity_id)

        auth = OneLogin_Saml2_Auth(request_data, old_settings=settings)

        if self._defers_acs_to_idp():
            # Set on the instance rather than globally, so integrations using other
            # policies are unaffected.
            auth.authn_request_class = AcsOmittingAuthnRequest

        return auth

    def _defers_acs_to_idp(self) -> bool:
        """Whether requests should name no ACS endpoint at all.

        Resolved through the configuration rather than read from the integration
        setting, so that a site-wide default from the environment is honored.
        """
        policy = self._configuration.get_acs_selection_policy()

        return policy is SAMLACSSelectionPolicy.DEFER_TO_IDP

    def _get_auth_object(self, db, idp_entity_id):
        """Return a cached OneLogin_Saml2_Auth object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """
        auth_object = self._create_auth_object(db, idp_entity_id)

        return auth_object

    @property
    def configuration(self):
        """Return configuration object.

        :return: Configuration object
        :rtype: SAMLOneLoginConfiguration
        """
        return self._configuration

    def start_authentication(
        self,
        db: sqlalchemy.orm.session.Session,
        idp_entity_id: str,
        return_to_url: str,
        force_authn: bool = False,
    ) -> str | ProblemDetail:
        """Start the SAML authentication workflow by sending a AuthnRequest to the IdP.

        :param db: Database session

        :param idp_entity_id: IdP's entityID

        :param return_to_url: URL which will the user agent will be redirected to after authentication

        :param force_authn: Whether to set ForceAuthn="true" on the AuthnRequest,
            telling the IdP to re-authenticate the user even if they have an
            existing session (SAML 2.0 Core 3.4.1)

        :return: Redirection URL, or a ProblemDetail on error
        """
        self._logger.info(
            "Started authentication workflow for IdP '{}' (redirection URL = '{}')".format(
                idp_entity_id, return_to_url
            )
        )

        try:
            auth = self._get_auth_object(db, idp_entity_id)
            redirect_url = auth.login(return_to_url, force_authn=force_authn)

            if self._logger.isEnabledFor(logging.DEBUG):
                if self._defers_acs_to_idp():
                    self._logger.debug(
                        f"AuthnRequest for IdP '{idp_entity_id}' names no ACS endpoint; "
                        f"the identity provider will use the one it has registered"
                    )
                else:
                    self._logger.debug(
                        f"AuthnRequest for IdP '{idp_entity_id}' asserts ACS endpoint "
                        f"'{self._get_asserted_acs_url(auth)}'"
                    )
                self._logger.debug(f"SAML request: {auth.get_last_request_xml()}")

            self._logger.info(
                "Finished authentication workflow for IdP '{}' (redirection URL = '{}'): {}".format(
                    idp_entity_id, return_to_url, redirect_url
                )
            )

            return redirect_url
        except OneLogin_Saml2_Error as exception:
            self._logger.exception(
                "Unexpected exception occurred while initiating authentication workflow"
            )

            return SAML_GENERIC_ERROR.detailed(str(exception))

    def finish_authentication(self, db, idp_entity_id):
        """Finish the SAML authentication workflow by validating AuthnResponse and extracting a SAML assertion from it.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: Subject object containing name ID and attributes in the case of a successful authentication
            or ProblemDetail object otherwise
        :rtype: Union[api.saml.metadata.model.SAMLSubject, core.util.problem_detail.ProblemDetail]
        """
        self._logger.info(
            "Started finishing authentication workflow for IdP '{}'".format(
                idp_entity_id
            )
        )

        request_data = self._get_request_data()

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(f"Request data: {request_data}")

        if (
            "post_data" not in request_data
            or "SAMLResponse" not in request_data["post_data"]
        ):
            return SAML_INCORRECT_RESPONSE.detailed(
                "There is no SAMLResponse in the body of the response"
            )

        auth = self._get_auth_object(db, idp_entity_id)

        received_acs_url = OneLogin_Saml2_Utils.get_self_url_no_query(request_data)

        if self._defers_acs_to_idp():
            # The request named no endpoint, so there is nothing to compare. Record the
            # identity provider's choice, which is what pinning a policy would select.
            asserted_acs_url = None
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(
                    f"SAML response for IdP '{idp_entity_id}' arrived at "
                    f"'{received_acs_url}', chosen by the identity provider"
                )
        else:
            asserted_acs_url = self._get_asserted_acs_url(auth)
            self._warn_on_acs_endpoint_mismatch(
                idp_entity_id, asserted_acs_url, received_acs_url
            )

        auth.process_response()

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(f"SAML response: {auth.get_last_response_xml()}")

        authenticated = auth.is_authenticated()

        if authenticated:
            subject = self._subject_parser.parse(auth)

            self._logger.info(
                "Finished finishing authentication workflow for IdP '{}': {}".format(
                    idp_entity_id, subject
                )
            )

            return subject
        else:
            error_reason = auth.get_last_error_reason()

            asserted = f"'{asserted_acs_url}'" if asserted_acs_url else "none"
            self._logger.error(
                f"SAML authentication failed for IdP '{idp_entity_id}': {error_reason} "
                f"(request asserted ACS endpoint {asserted}; response arrived "
                f"at '{received_acs_url}')"
            )

            return SAML_AUTHENTICATION_ERROR.detailed(error_reason)

    def start_logout(
        self,
        db: sqlalchemy.orm.session.Session,
        idp_entity_id: str,
        name_id: SAMLNameID,
        sp_slo_callback_url: str,
        relay_state: str,
    ) -> str | ProblemDetail:
        """Initiate SP-Initiated SAML SLO by sending a LogoutRequest to the IdP.

        :param db: Database session
        :param idp_entity_id: IdP's entityID
        :param name_id: Patron's SAML NameID from their existing session
        :param sp_slo_callback_url: Absolute URL of our SLO callback endpoint
        :param relay_state: State string to round-trip through the IdP (encodes redirect URI)
        :return: Redirect URL to the IdP's SLO endpoint, or a ProblemDetail on error
        """
        self._logger.info(
            f"Starting SLO for IdP '{idp_entity_id}' (callback={sp_slo_callback_url})"
        )
        try:
            logout_settings = self._configuration.get_logout_settings(
                db, idp_entity_id, sp_slo_callback_url
            )
            auth = self._create_auth_object(db, idp_entity_id, settings=logout_settings)
            redirect_url = auth.logout(
                return_to=relay_state,
                name_id=name_id.name_id,
                nq=name_id.name_qualifier,
                name_id_format=name_id.name_format,
                spnq=name_id.sp_name_qualifier,
            )
        except (OneLogin_Saml2_Error, SAMLConfigurationError) as exception:
            self._logger.exception("Unexpected error while initiating SAML SLO")
            return SAML_GENERIC_ERROR.detailed(str(exception))

        self._logger.info(
            f"SLO initiated for IdP '{idp_entity_id}': redirecting to {redirect_url}"
        )
        return redirect_url

    def finish_logout(
        self,
        db: sqlalchemy.orm.session.Session,
        idp_entity_id: str,
        sp_slo_callback_url: str,
    ) -> bool | pd:
        """Validate the IdP's LogoutResponse for SP-Initiated SLO.

        Only handles SAMLResponse (LogoutResponse). IdP-Initiated SLO
        (inbound SAMLRequest / LogoutRequest) is not supported.

        :param db: Database session
        :param idp_entity_id: IdP's entityID
        :param sp_slo_callback_url: Absolute URL of our SLO callback endpoint
        :return: True on success, or a ProblemDetail on error
        """
        request_data = self._get_request_data()

        # Reject IdP-Initiated SLO (inbound LogoutRequest).
        has_response = bool(
            request_data.get("get_data", {}).get("SAMLResponse")
            or request_data.get("post_data", {}).get("SAMLResponse")
        )
        if not has_response:
            return SAML_GENERIC_ERROR.detailed(
                "SAMLResponse not found; IdP-Initiated SLO is not supported"
            )

        # HTTP-POST binding: the OneLogin SAML toolkit's process_slo only
        # handles HTTP-Redirect and would raise immediately for POST. Since we
        # already invalidated the patron's local credential before initiating
        # SLO, treat any POST response as a best-effort confirmation and return
        # success without further validation.
        # NOTE: In the future we could add partial validation here (e.g. status
        # code and issuer checks), but full validation would require XML digital
        # signature verification that the toolkit does not provide for this binding.
        if not request_data.get("get_data", {}).get("SAMLResponse"):
            self._logger.info(
                f"HTTP-POST SLO response received for IdP '{idp_entity_id}'; "
                "skipping validation (not supported by the OneLogin toolkit)"
            )
            return True

        self._logger.info(f"Processing SLO response for IdP '{idp_entity_id}'")
        try:
            logout_settings = self._configuration.get_logout_settings(
                db, idp_entity_id, sp_slo_callback_url
            )
            auth = self._create_auth_object(db, idp_entity_id, settings=logout_settings)
            auth.process_slo(keep_local_session=True)
            errors = auth.get_errors()
            if errors:
                reason = auth.get_last_error_reason()
                self._logger.error(f"SLO validation errors: {errors} — {reason}")
                return SAML_GENERIC_ERROR.detailed(f"SLO validation failed: {reason}")
        except (OneLogin_Saml2_Error, SAMLConfigurationError) as exception:
            self._logger.exception(
                "Unexpected error while processing SAML SLO response"
            )
            return SAML_GENERIC_ERROR.detailed(str(exception))

        self._logger.info(
            f"SLO response validated successfully for IdP '{idp_entity_id}'"
        )
        return True


class SAMLAuthenticationManagerFactory:
    """Responsible for creating SAMLAuthenticationManager instances"""

    def create(
        self, configuration: SAMLWebSSOAuthSettings
    ) -> SAMLAuthenticationManager:
        """
        Creates a new instance of SAMLAuthenticationManager class

        :param configuration: SAML authentication provider's configuration

        :return: SAML authentication manager
        """
        # Load SP configuration from environment variables
        sp_config = SamlServiceProviderConfiguration()

        # Pass both configs to SAMLOneLoginConfiguration
        onelogin_configuration = SAMLOneLoginConfiguration(configuration, sp_config)
        subject_parser = SAMLSubjectParser()
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser
        )

        return authentication_manager
