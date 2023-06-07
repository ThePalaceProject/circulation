import logging
import urllib
from typing import Optional

import sqlalchemy
from sqlalchemy.orm import Session

from api.circulation import CirculationFulfillmentPostProcessor, FulfillmentInfo
from api.saml.credential import SAMLCredentialManager
from core.exceptions import BaseError
from core.model import Collection, get_one
from core.model.configuration import ExternalIntegration, HasExternalIntegration
from core.saml.wayfless import SAMLWAYFlessConfigurationTrait


class SAMLWAYFlessFulfillmentError(BaseError):
    pass


class SAMLWAYFlessAcquisitionLinkProcessor(
    CirculationFulfillmentPostProcessor, HasExternalIntegration
):
    """Interface indicating that the collection implementing it has templated links.

    Example of templated links may be a WAYFless acquisition link.
    A WAYFless URL, is specific to an institution with associated users and to a web-based service or resource.
    It enables a user from an institution to gain federated SAML access to the service or resource in a way
    that bypasses the "Where Are You From?" (WAYF) page or Discovery Service step in
    SAML based authentication and access protocols.
    """

    _wayfless_url_template: Optional[str]

    def __init__(self, collection: Collection) -> None:
        """Initialize a new instance of WAYFlessAcquisitionLinkProcessor class.

        :param collection: Circulation collection
        """
        if not isinstance(collection, Collection):
            raise ValueError(
                f"Argument 'collection' must be an instance {Collection} class"
            )
        if not collection.external_integration_id:
            raise ValueError(
                f"Collection {collection} does not have an external integration"
            )

        external: ExternalIntegration = collection.external_integration
        self._wayfless_url_template: Optional[
            str
        ] = collection.integration_configuration.get(
            SAMLWAYFlessConfigurationTrait.WAYFLESS_URL_TEMPLATE_KEY
        )

        self._external_integration_id: Optional[
            int
        ] = collection.external_integration_id
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._saml_credential_manager: SAMLCredentialManager = SAMLCredentialManager()

    def external_integration(
        self, db: sqlalchemy.orm.session.Session
    ) -> ExternalIntegration:
        """Return an ExternalIntegration object associated with the collection with a WAYFless url.

        :param db: SQLAlchemy session
        :return: ExternalIntegration object
        """
        return get_one(db, ExternalIntegration, id=self._external_integration_id)

    def fulfill(
        self, patron, pin, licensepool, delivery_mechanism, fulfillment: FulfillmentInfo
    ) -> FulfillmentInfo:

        self._logger.debug(
            f"WAYFless acquisition link template: {self._wayfless_url_template}"
        )

        if self._wayfless_url_template:
            db = Session.object_session(patron)
            saml_credential = self._saml_credential_manager.lookup_saml_token_by_patron(
                db, patron
            )

            self._logger.debug(f"SAML credentials: {saml_credential}")

            if not saml_credential:
                raise SAMLWAYFlessFulfillmentError(
                    f"There are no existing SAML credentials for patron {patron}"
                )

            saml_subject = self._saml_credential_manager.extract_saml_token(
                saml_credential
            )

            self._logger.debug(f"SAML subject: {saml_subject}")

            if not saml_subject.idp:
                raise SAMLWAYFlessFulfillmentError(
                    f"SAML subject {saml_subject} does not contain an IdP's entityID"
                )

            acquisition_link = self._wayfless_url_template.replace(
                SAMLWAYFlessConfigurationTrait.IDP_PLACEHOLDER,
                urllib.parse.quote(saml_subject.idp, safe=""),
            )
            acquisition_link = acquisition_link.replace(
                SAMLWAYFlessConfigurationTrait.ACQUISITION_LINK_PLACEHOLDER,
                urllib.parse.quote(fulfillment.content_link, safe=""),
            )

            self._logger.debug(
                f"Old acquisition link {fulfillment.content_link} has been transformed to {acquisition_link}"
            )

            fulfillment.content_link = acquisition_link

        return fulfillment
