from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from flask import request
from flask_babel import lazy_gettext as _
from onelogin.saml2.auth import OneLogin_Saml2_Auth
from onelogin.saml2.errors import OneLogin_Saml2_Error

from palace.manager.integration.patron_auth.saml.configuration.model import (
    SAMLOneLoginConfiguration,
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
    from palace.manager.integration.patron_auth.saml.provider import (
        SAMLWebSSOAuthSettings,
    )

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
    "http://librarysimplified.org/terms/problem/saml/authentication-error",
    status_code=401,
    title=_("SAML authentication error."),
    detail=_("SAML authentication error."),
)

SAML_NO_ACCESS_ERROR = pd(
    "http://librarysimplified.org/terms/problem/saml/no-access-error",
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
    def _get_request_data():
        """Map Flask request to what the SAML toolkit expects.

        :return: Dictionary containing information about the request in the format SAML toolkit expects
        :rtype: Dict
        """
        # If server is behind proxys or balancers use the HTTP_X_FORWARDED fields
        url_data = urlparse(request.url)

        return {
            "https": "on" if request.scheme == "https" else "off",
            "http_host": request.host,
            "server_port": url_data.port,
            "script_name": request.path,
            "get_data": request.args.copy(),
            # Uncomment if using ADFS as IdP, https://github.com/onelogin/python-saml/pull/144
            # 'lowercase_urlencoding': True,
            "post_data": request.form.copy(),
        }

    def _create_auth_object(self, db, idp_entity_id):
        """Create and initialize an OneLogin_Saml2_Auth object.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param idp_entity_id: IdP's entityID
        :type idp_entity_id: string

        :return: OneLogin_Saml2_Auth object
        :rtype: OneLogin_Saml2_Auth
        """
        request_data = self._get_request_data()
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
        onelogin_configuration = SAMLOneLoginConfiguration(configuration)
        subject_parser = SAMLSubjectParser()
        parser = DSLParser()
        visitor = DSLEvaluationVisitor()
        evaluator = DSLEvaluator(parser, visitor)
        subject_filter = SAMLSubjectFilter(evaluator)
        authentication_manager = SAMLAuthenticationManager(
            onelogin_configuration, subject_parser, subject_filter
        )

        return authentication_manager
