from unittest.mock import MagicMock, create_autospec, patch

from palace.manager.celery.tasks.saml import (
    _create_saml_federated_identity_provider_loader,
    update_saml_federation_idps_metadata,
)
from palace.manager.integration.patron_auth.saml.metadata.federations.loader import (
    SAMLFederatedIdentityProviderLoader,
)
from palace.manager.sqlalchemy.model.saml import (
    SAMLFederatedIdentityProvider,
    SAMLFederation,
)
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks import saml_strings


def test_update_saml_federation_idps_metadata_task(
    db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
):
    # Add a federation to the database and commit it.
    # This mimics what we currently do when setting up a SAML federation
    # using `bin/configuration/add_saml_federations.py`.
    saml_federation = SAMLFederation("Test federation", "http://incommon.org/metadata")
    db.session.add(saml_federation)
    db.session.commit()

    # We shouldn't have any IdPs in any federation yet.
    preexisting_idps: list[SAMLFederatedIdentityProvider] = db.session.query(
        SAMLFederatedIdentityProvider
    ).all()
    assert not preexisting_idps

    # Now we'll set up some mocking for the update.
    idps_to_add = [
        SAMLFederatedIdentityProvider(
            saml_federation,
            saml_strings.IDP_1_ENTITY_ID,
            saml_strings.IDP_1_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_1,
        ),
        SAMLFederatedIdentityProvider(
            saml_federation,
            saml_strings.IDP_2_ENTITY_ID,
            saml_strings.IDP_2_UI_INFO_EN_DISPLAY_NAME,
            saml_strings.CORRECT_XML_WITH_IDP_2,
        ),
    ]

    loader = create_autospec(spec=SAMLFederatedIdentityProviderLoader)
    loader.load = MagicMock(return_value=idps_to_add)

    with patch(
        "palace.manager.celery.tasks.saml._create_saml_federated_identity_provider_loader"
    ) as create_loader:
        create_loader.return_value = loader

        # Run the actual update task.
        update_saml_federation_idps_metadata.delay().wait()

    # The added IdPs should remain in the database after the task runs.
    identity_providers: list[SAMLFederatedIdentityProvider] = db.session.query(
        SAMLFederatedIdentityProvider
    ).all()
    assert len(identity_providers) == 2
    assert idps_to_add == identity_providers


def test_create_saml_federated_identity_provider_loader():

    assert _create_saml_federated_identity_provider_loader()
