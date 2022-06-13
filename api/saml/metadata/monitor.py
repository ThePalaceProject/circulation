import datetime
import logging

from api.saml.metadata.federations.model import SAMLFederation
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model import ExternalIntegration
from core.monitor import Monitor
from core.util.datetime_helpers import utc_now


class SAMLMetadataMonitor(Monitor):
    SERVICE_NAME = "SAML Metadata Monitor"

    MAX_AGE = datetime.timedelta(days=1)

    def __init__(self, db, loader):
        """Initialize a new instance of SAMLMetadataMonitor class.

        :param loader: IdP loader
        :type loader: api.saml.loader.SAMLFederatedIdPLoader
        """
        super().__init__(db)

        self._loader = loader
        self._logger = logging.getLogger(__name__)

    def _update_saml_federation_idps_metadata(self, saml_federation):
        """Update IdPs' metadata belonging to the specified SAML federation.

        :param saml_federation: SAML federation
        :type saml_federation: api.saml.metadata.federations.model.SAMLFederation
        """
        self._logger.info(f"Started processing {saml_federation}")

        for existing_identity_provider in saml_federation.identity_providers:
            self._db.delete(existing_identity_provider)

        new_identity_providers = self._loader.load(saml_federation)

        for new_identity_provider in new_identity_providers:
            self._db.add(new_identity_provider)

        saml_federation.last_updated_at = utc_now()

        self._logger.info(f"Finished processing {saml_federation}")

    def _check_if_saml_protocol_is_used(self):
        saml_integrations = (
            self._db.query(ExternalIntegration)
            .filter(
                ExternalIntegration.protocol
                == SAMLWebSSOAuthenticationProvider.__module__
            )
            .all()
        )

        if saml_integrations:
            return True

        return False

    def run_once(self, progress):
        self._logger.info("Started running the SAML metadata monitor")

        is_saml_used = self._check_if_saml_protocol_is_used()

        if not is_saml_used:
            self._logger.info(
                "Finished running the SAML metadata monitor - SAML protocol not used"
            )
            return

        with self._db.begin(subtransactions=True):
            saml_federations = self._db.query(SAMLFederation).all()

            self._logger.info(f"Found {len(saml_federations)} SAML federations")

            for outdated_saml_federation in saml_federations:
                self._update_saml_federation_idps_metadata(outdated_saml_federation)

        self._logger.info("Finished running the SAML metadata monitor")
