from __future__ import annotations

import sys
from collections.abc import MutableSet

from celery.canvas import Signature
from sqlalchemy.orm import Session
from typing_extensions import Unpack
from uritemplate import URITemplate

from palace.manager.api.circulation.base import BaseCirculationAPI
from palace.manager.api.circulation.exceptions import CannotFulfill
from palace.manager.api.circulation.fulfillment import RedirectFulfillment
from palace.manager.integration.license.opds.base.api import BaseOPDSAPI
from palace.manager.integration.license.opds.opds2.settings import (
    OPDS2ImporterLibrarySettings,
    OPDS2ImporterSettings,
)
from palace.manager.integration.patron_auth.saml.metadata.model import SAMLAttributeType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.licensing import (
    LicensePool,
    LicensePoolDeliveryMechanism,
)
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.http.exception import BadResponseException
from palace.manager.util.http.http import HTTP

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class TemplateVariable(StrEnum):
    """Supported template variables for OPDS2 token authentication URLs."""

    PATRON_ID = "patron_id"
    SAML_ENTITY_ID = "saml_entity_id"
    SAML_PERSON_SCOPED_AFFILIATION = "saml_person_scoped_affiliation"


SAML_TEMPLATE_VARIABLES: frozenset[str] = frozenset(
    var for var in TemplateVariable if var.startswith("saml_")
)
SUPPORTED_TEMPLATE_VARIABLES: frozenset[str] = frozenset(
    var for var in TemplateVariable
)


class OPDS2API(BaseOPDSAPI):
    TOKEN_AUTH_CONFIG_KEY = "token_auth_endpoint"

    @classmethod
    def settings_class(cls) -> type[OPDS2ImporterSettings]:
        return OPDS2ImporterSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDS2ImporterLibrarySettings]:
        return OPDS2ImporterLibrarySettings

    @classmethod
    def label(cls) -> str:
        return "OPDS 2.0 Import"

    @classmethod
    def description(cls) -> str:
        return "Import books from a publicly-accessible OPDS 2.0 feed."

    def __init__(self, _db: Session, collection: Collection):
        super().__init__(_db, collection)
        self.token_auth_configuration: str | None = (
            collection.integration_configuration.context.get(self.TOKEN_AUTH_CONFIG_KEY)
        )

    def _get_saml_token_template_parameters(
        self, patron: Patron
    ) -> tuple[str | None, list[str] | None]:
        """Get the SAML token arguments for a patron

        :param patron: The patron to get parameters for

        :return: A tuple containing the SAML entity ID and a list of person scoped affiliations,
            or (None, None) if the patron has no SAML credentials, or they cannot be
            extracted.
        """
        saml_credential = self.saml_credential_manager.lookup_saml_token_by_patron(
            self._db, patron
        )

        if not saml_credential:
            return None, None

        saml_subject = self.saml_credential_manager.extract_saml_token(saml_credential)

        if not saml_subject:
            return None, None

        entity_id = saml_subject.idp
        person_scoped_affiliation = None

        if (
            saml_subject.attribute_statement
            and saml_subject.attribute_statement.attributes
        ):
            scoped_affiliation_attr = saml_subject.attribute_statement.attributes.get(
                SAMLAttributeType.eduPersonScopedAffiliation.name
            )

            if scoped_affiliation_attr:
                person_scoped_affiliation = scoped_affiliation_attr.values

        return entity_id, person_scoped_affiliation

    def _build_template_parameters(
        self, patron: Patron, datasource: DataSource, variable_names: MutableSet[str]
    ) -> dict[str, str | list[str]]:
        """Build template parameters based on what the template requires.

        :param patron: The patron to get parameters for
        :param datasource: The datasource for patron_id lookup
        :param variable_names: Set of variable names from the URI template

        :return: Dictionary of template parameters

        :raises CannotFulfill: If template requires SAML parameters but patron lacks them
        """
        parameters: dict[str, str | list[str]] = {}

        requested_variables = variable_names & SUPPORTED_TEMPLATE_VARIABLES

        if TemplateVariable.PATRON_ID in requested_variables:
            patron_id = patron.identifier_to_remote_service(datasource)
            parameters[TemplateVariable.PATRON_ID] = patron_id

        # Handle SAML template variables if any are requested.
        # The logic here ensures that if the template requires SAML parameters, the patron
        # must have ALL required SAML credentials. Partial data is not accepted - if the
        # template asks for both entity_id and affiliation, the patron must have both.
        if requested_saml_variables := (SAML_TEMPLATE_VARIABLES & requested_variables):
            # Fetch patron's SAML credentials
            entity_id, person_scoped_affiliation = (
                self._get_saml_token_template_parameters(patron)
            )

            # Try to add each requested SAML variable to parameters
            if TemplateVariable.SAML_ENTITY_ID in requested_variables and entity_id:
                parameters[TemplateVariable.SAML_ENTITY_ID] = entity_id

            if (
                TemplateVariable.SAML_PERSON_SCOPED_AFFILIATION in requested_variables
                and person_scoped_affiliation
            ):
                parameters[TemplateVariable.SAML_PERSON_SCOPED_AFFILIATION] = (
                    person_scoped_affiliation
                )

            # Check if any requested SAML variables are still missing from parameters.
            # This will be true if the patron lacks the required SAML credentials.
            missing_saml_variables = requested_saml_variables - parameters.keys()
            if missing_saml_variables:
                self.log.error(
                    f"Template requires SAML parameters {', '.join(requested_saml_variables)}, "
                    f"but patron (authorization identifier: '{patron.authorization_identifier}') "
                    f"is missing: {', '.join(missing_saml_variables)}."
                )
                raise CannotFulfill()

        return parameters

    def get_authentication_token(
        self, patron: Patron, datasource: DataSource, token_auth_url: str
    ) -> str:
        """Get the authentication token for a patron from a token authentication endpoint.

        The token authentication URL is a URI template that may include template variables
        defined in TemplateVariable. Supported variables include patron_id, saml_entity_id,
        and saml_person_scoped_affiliation. This method will expand the template with the
        appropriate values based on the patron's credentials and make an HTTP GET request
        to retrieve the authentication token.

        :param patron: The patron to authenticate
        :param datasource: The datasource for patron identifier lookup
        :param token_auth_url: URI template for the token endpoint. See TemplateVariable
            for supported template variables.

        :return: The authentication token as a string

        :raises CannotFulfill: If the token endpoint returns a non-2xx status code, an empty
            response, or if the template requires SAML parameters but the patron lacks the
            necessary SAML credentials
        """
        # Parse template and build parameters
        template = URITemplate(token_auth_url)
        parameters = self._build_template_parameters(
            patron, datasource, template.variable_names
        )

        # Expand template and make request
        # We need a type ignore here because of an upstream type issue.
        # See: https://github.com/python-hyper/uritemplate/pull/130
        url = template.expand(parameters)  # type: ignore[arg-type]
        try:
            response = HTTP.get_with_timeout(url, allowed_response_codes=["2xx"])
        except BadResponseException as e:
            response = e.response
            self.log.error(
                f"Could not authenticate the patron (authorization identifier: '{patron.authorization_identifier}'). "
                f"Bad status code {response.status_code} from {url} expected 2xx.",
                extra={
                    "palace_response_content": response.content,
                    "palace_response_status_code": response.status_code,
                    "palace_template_parameters": parameters,
                },
            )
            raise CannotFulfill() from e

        # The response should be a token in plain text, that we are able to pass as-is
        # to the fulfillment URL.
        token = response.text.strip() if response.text else None
        if not token:
            self.log.error(
                f"Could not authenticate the patron (authorization identifier: '{patron.authorization_identifier}'). "
                f"Empty response from {url}, expected an authentication token.",
                extra={
                    "palace_template_parameters": parameters,
                },
            )
            raise CannotFulfill()

        return token

    def fulfill_token_auth(
        self,
        patron: Patron,
        licensepool: LicensePool,
        fulfillment: RedirectFulfillment,
    ) -> RedirectFulfillment:
        templated = URITemplate(fulfillment.content_link)
        if "authentication_token" not in templated.variable_names:
            self.log.warning(
                "No authentication_token variable found in content_link, unable to fulfill via OPDS2 token auth."
            )
            return fulfillment

        if not self.token_auth_configuration:
            self.log.warning(
                "No token auth configuration found, unable to fulfill via OPDS2 token auth."
            )
            return fulfillment

        token = self.get_authentication_token(
            patron, licensepool.data_source, self.token_auth_configuration
        )
        fulfillment.content_link = templated.expand(authentication_token=token)
        return fulfillment

    def fulfill(
        self,
        patron: Patron,
        pin: str,
        licensepool: LicensePool,
        delivery_mechanism: LicensePoolDeliveryMechanism,
        **kwargs: Unpack[BaseCirculationAPI.FulfillKwargs],
    ) -> RedirectFulfillment:
        fulfillment = super().fulfill(
            patron, pin, licensepool, delivery_mechanism, **kwargs
        )
        if self.token_auth_configuration:
            fulfillment = self.fulfill_token_auth(patron, licensepool, fulfillment)
        return fulfillment

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        from palace.manager.celery.tasks.opds2 import import_collection

        return import_collection.s(collection_id, force=force)

    @classmethod
    def update_collection_token_auth_url(cls, collection: Collection, url: str) -> bool:
        """
        Update the collection's integration context with the token authentication URL.

        This method checks if the provided URL matches the current token authentication
        URL in the collection's integration context. If it does not match, it updates
        the context with the new URL and returns True. If it matches, it returns False
        without making any changes.
        """
        integration = collection.integration_configuration
        if integration.context.get(cls.TOKEN_AUTH_CONFIG_KEY) == url:
            # No change, so we don't need to update the context.
            return False

        integration.context_update({cls.TOKEN_AUTH_CONFIG_KEY: url})
        return True
