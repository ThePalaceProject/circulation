from unittest.mock import MagicMock, create_autospec

import pytest

from api.saml.metadata.federations.loader import SAMLFederatedIdentityProviderLoader
from api.saml.metadata.federations.model import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from api.saml.metadata.monitor import SAMLMetadataMonitor
from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model.configuration import ExternalIntegration
from tests.api.saml import saml_strings
from tests.fixtures.database import DatabaseTransactionFixture


class SAMLMetadataMonitorFixture:
    db: DatabaseTransactionFixture
    integration: ExternalIntegration
    authentication_provider: SAMLWebSSOAuthenticationProvider

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.integration = self.db.external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
        self.authentication_provider = SAMLWebSSOAuthenticationProvider(
            self.db.default_library(), self.integration
        )


@pytest.fixture(scope="function")
def saml_metadata_monitor_fixture(
    db: DatabaseTransactionFixture,
) -> SAMLMetadataMonitorFixture:
    return SAMLMetadataMonitorFixture(db)


class TestSAMLMetadataMonitor:
    def test(self, saml_metadata_monitor_fixture: SAMLMetadataMonitorFixture):
        # Arrange
        expected_federation = SAMLFederation(
            "Test federation", "http://incommon.org/metadata"
        )
        expected_federated_identity_providers = [
            SAMLFederatedIdentityProvider(
                expected_federation,
                saml_strings.IDP_1_ENTITY_ID,
                saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
                saml_strings.CORRECT_XML_WITH_IDP_1,
            ),
            SAMLFederatedIdentityProvider(
                expected_federation,
                saml_strings.IDP_2_ENTITY_ID,
                saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME,
                saml_strings.CORRECT_XML_WITH_IDP_2,
            ),
        ]

        saml_metadata_monitor_fixture.db.session.add_all([expected_federation])
        saml_metadata_monitor_fixture.db.session.add_all(
            expected_federated_identity_providers
        )

        loader = create_autospec(spec=SAMLFederatedIdentityProviderLoader)
        loader.load = MagicMock(return_value=expected_federated_identity_providers)

        monitor = SAMLMetadataMonitor(saml_metadata_monitor_fixture.db.session, loader)

        # Act
        monitor.run_once(None)

        # Assert
        identity_providers = saml_metadata_monitor_fixture.db.session.query(
            SAMLFederatedIdentityProvider
        ).all()
        assert expected_federated_identity_providers == identity_providers
