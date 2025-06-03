from unittest.mock import MagicMock, create_autospec, patch

from fixtures.celery import CeleryFixture
from fixtures.database import DatabaseTransactionFixture
from mocks import saml_strings

from palace.manager.api.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
)
from palace.manager.celery.tasks.saml import update_saml_federation_idps_metadata
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)


def test(db: DatabaseTransactionFixture, celery_fixture: CeleryFixture):
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

    with patch(
        "palace.manager.celery.tasks.saml._create_saml_federated_identity_provider_loader"
    ) as create_loader:
        create_loader.return_value = loader

        # Invoke
        update_saml_federation_idps_metadata.delay().wait()
        # Assert
        identity_providers = db.session.query(SAMLFederatedIdentityProvider).all()
        assert expected_federated_identity_providers == identity_providers
