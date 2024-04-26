from unittest.mock import MagicMock, create_autospec

from palace.manager.api.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
)
from palace.manager.api.saml.metadata.monitor import SAMLMetadataMonitor
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks import saml_strings


class TestSAMLMetadataMonitor:
    def test(self, db: DatabaseTransactionFixture):
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

        db.session.add_all([expected_federation])
        db.session.add_all(expected_federated_identity_providers)

        loader = create_autospec(spec=SAMLFederatedIdentityProviderLoader)
        loader.load = MagicMock(return_value=expected_federated_identity_providers)

        monitor = SAMLMetadataMonitor(db.session, loader)

        # Act
        monitor.run_once(None)

        # Assert
        identity_providers = db.session.query(SAMLFederatedIdentityProvider).all()
        assert expected_federated_identity_providers == identity_providers
