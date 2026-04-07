from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from flask import request
from flask_babel import lazy_gettext as _
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.errors import OneLogin_Saml2_Error

from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLConfigurationError,
    SAMLOneLoginConfiguration,
)
from palace.manager.integration.patron_auth.saml.configuration.service_provider import (
    SamlServiceProviderConfiguration,
)
from palace.manager.integration.patron_auth.saml.metadata.filter import (
    SAMLSubjectFilter,
    SAMLSubjectFilterError,
)
from palace.manager.integration.patron_auth.saml.metadata.parser import (
    SAMLSubjectParser,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.evaluator import (
    DSLEvaluationVisitor,
    DSLEvaluator,
)
from palace.manager.integration.patron_auth.saml.python_expression_dsl.parser import (
    DSLParser,
)
from palace.manager.util.problem_detail import ProblemDetail as pd

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


class SAMLAuthenticationManager:
    """Implements SAML authentication process."""

    def __init__(self, configuration, subject_parser, subject_filter):
        """Initialize a new instance of SAMLAuthenticationManager.

        :param configuration: OneLoginConfiguration object
        :type configuration: api.saml.configuration.model.SAMLOneLoginConfiguration

        :param subject_parser: Subject parser
        :type subject_parser: api.saml.metadata.parser.SAMLSubjectParser

        :param subject_filter: Subject filter
        :type subject_filter: api.saml.metadata.filter.SAMLSubjectFilter
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
        if not isinstance(subject_filter, SAMLSubjectFilter):
            raise ValueError(
                "Argument 'subject_filter' must be an instance of {} class".format(
                    SAMLSubjectFilter
                )
            )

        self._configuration = configuration
        self._subject_parser = subject_parser
        self._subject_filter = subject_filter

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

        return auth

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

    def _filter_subject(self, subject):
        """Filter the subject object using the filtration expression (if there is any).

        :param subject: SAML subject object
        :type subject: api.saml.metadata.model.SAMLSubject

        :return: SAML subject object if it has not been filtered out, a ProblemDetail object instead
        :rtype: Union[api.saml.metadata.model.SAMLSubject, core.util.problem_detail.ProblemDetail]
        """
        self._logger.info(f"Started filtering {subject}")

        if not self._configuration.configuration.filter_expression:
            self._logger.info(
                "There is no filtration expression. Finished filtering {}".format(
                    subject
                )
            )

            return subject

        try:
            filtration_result = self._subject_filter.execute(
                self._configuration.configuration.filter_expression, subject
            )

            self._logger.info(f"Finished filtering {subject}: {filtration_result}")

            if not filtration_result:
                return SAML_NO_ACCESS_ERROR

            return subject
        except SAMLSubjectFilterError as exception:
            self._logger.info(
                f"An unexpected error occurred during filtering {subject}"
            )

            return SAML_GENERIC_ERROR.detailed(str(exception))

    @property
    def configuration(self):
        """Return configuration object.

        :return: Configuration object
        :rtype: SAMLOneLoginConfiguration
        """
        return self._configuration

    def start_authentication(self, db, idp_entity_id, return_to_url):
        """Start the SAML authentication workflow by sending a AuthnRequest to the IdP.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :param return_to_url: URL which will the user agent will be redirected to after authentication
        :type return_to_url: string

        :return: Redirection URL
        :rtype: string
        """
        self._logger.info(
            "Started authentication workflow for IdP '{}' (redirection URL = '{}')".format(
                idp_entity_id, return_to_url
            )
        )

        try:
            auth = self._get_auth_object(db, idp_entity_id)
            redirect_url = auth.login(return_to_url)

            if self._logger.isEnabledFor(logging.DEBUG):
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
        auth.process_response()

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(f"SAML response: {auth.get_last_response_xml()}")

        authenticated = auth.is_authenticated()

        if authenticated:
            subject = self._subject_parser.parse(auth)
            subject = self._filter_subject(subject)

            self._logger.info(
                "Finished finishing authentication workflow for IdP '{}': {}".format(
                    idp_entity_id, subject
                )
            )

            return subject
        else:
            self._logger.error(auth.get_last_error_reason())

            return SAML_AUTHENTICATION_ERROR.detailed(auth.get_last_error_reason())

    def start_logout(
        self,
        db: sqlalchemy.orm.session.Session,
        idp_entity_id: str,
        name_id: SAMLNameID,
        sp_slo_callback_url: str,
        relay_state: str,
    ) -> str | pd:
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
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        return authentication_manager
