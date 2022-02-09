import logging
import urllib
from contextlib import contextmanager
from typing import Optional

import sqlalchemy
from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session

from api.circulation import CirculationFulfillmentPostProcessor, FulfillmentInfo
from api.saml.credential import SAMLCredentialManager
from core.exceptions import BaseError
from core.model import Collection, get_one
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationFactory,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationStorage,
    ExternalIntegration,
    HasExternalIntegration,
)


class SAMLWAYFlessConfiguration(ConfigurationGrouping):
    IDP_PLACEHOLDER = "{idp}"
    ACQUISITION_LINK_PLACEHOLDER = "{targetUrl}"

    wayfless_url_template = ConfigurationMetadata(
        key="saml_wayfless_url_template",
        label=_("SAML WAYFless URL Template"),
        description=_(
            "<b>This configuration setting should be used ONLY when the authentication protocol is SAML.</b>"
            "<br>"
            "The phrase 'Where Are You From?' (WAYF) is often used to characterise identity provider discovery."
            "<br>"
            "Generally speaking, a <i>discovery service</i> is a solution to the "
            "<a href='https://wiki.shibboleth.net/confluence/display/SHIB2/IdPDiscovery'>identity provider discovery</a> problem, "
            "a longstanding problem in the federated identity management space "
            "when there are multiple identity providers available each corresponding to a specific organisation."
            "<br>"
            "To avoid having to use the 'Where Are You From' (WAYF) page it is possible to link directly to "
            "publication on the content provider's site. "
            "If the user is already logged in they will be taken directly to the article, "
            "otherwise they will be taken directly to your login page and then onto the article after logging in. "
            "These links are created using the following format:"
            "<br>"
            "https://fsso.springer.com/saml/login?idp={idp}&targetUrl={targetUrl}"
            "<br>"
            " - <b>idp</b> is an entityID of the SAML Identity Provider. "
            "Circulation Manager will substitute it with the entity ID of the 'active' IdP, "
            "i.e., the IdP that the patron is currently authenticated against."
            "<br>"
            " - <b>targetUrl</b> is substituted with the an encoded direct link to the publication."
        ),
        type=ConfigurationAttributeType.TEXT,
        required=False,
        default=None,
    )


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

        self._external_integration_id: Optional[
            int
        ] = collection.external_integration_id
        self._logger: logging.Logger = logging.getLogger(__name__)
        self._saml_credential_manager: SAMLCredentialManager = SAMLCredentialManager()
        self._configuration_storage: ConfigurationStorage = ConfigurationStorage(self)
        self._configuration_factory: ConfigurationFactory = ConfigurationFactory()

    @contextmanager
    def _get_configuration(
        self, db: sqlalchemy.orm.session.Session
    ) -> SAMLWAYFlessConfiguration:
        """Return the WAYFless configuration object.

        :param db: SQLAlchemy session
        :return: SAMLWAYFlessConfiguration object
        """
        with self._configuration_factory.create(
            self._configuration_storage, db, SAMLWAYFlessConfiguration
        ) as configuration:
            yield configuration

    def _get_wayfless_url_template(
        self, db: sqlalchemy.orm.session.Session
    ) -> Optional[str]:
        """Return a templated acquisition link.

        :param db: SQLAlchemy session
        :return: Templated acquisition link
        """
        with self._get_configuration(db) as configuration:
            return configuration.wayfless_url_template

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
        db = Session.object_session(patron)
        acquisition_link_template = self._get_wayfless_url_template(db)

        self._logger.debug(
            f"WAYFless acquisition link template: {acquisition_link_template}"
        )

        if acquisition_link_template:
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

            acquisition_link = acquisition_link_template.replace(
                SAMLWAYFlessConfiguration.IDP_PLACEHOLDER,
                urllib.parse.quote(saml_subject.idp, safe=""),
            )
            acquisition_link = acquisition_link.replace(
                SAMLWAYFlessConfiguration.ACQUISITION_LINK_PLACEHOLDER,
                urllib.parse.quote(fulfillment.content_link, safe=""),
            )

            self._logger.debug(
                f"Old acquisition link {fulfillment.content_link} has been transformed to {acquisition_link}"
            )

            fulfillment.content_link = acquisition_link

        return fulfillment
