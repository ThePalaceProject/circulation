from unittest.mock import MagicMock, create_autospec, patch

from palace.manager.api.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
)
from palace.manager.celery.tasks.saml import (
    _create_saml_federated_identity_provider_loader,
    update_saml_federation_idps_metadata,
)
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks import saml_strings


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

    current_time = utc_now()
    with patch(
        "palace.manager.celery.tasks.saml._create_saml_federated_identity_provider_loader"
    ) as create_loader:
        create_loader.return_value = loader

        # Invoke
        update_saml_federation_idps_metadata.delay().wait()
        # Assert
        identity_providers: list[SAMLFederatedIdentityProvider] = db.session.query(
            SAMLFederatedIdentityProvider
        ).all()
        assert expected_federated_identity_providers == identity_providers


def test_create_saml_federated_identity_provider_loader():

    assert _create_saml_federated_identity_provider_loader()
