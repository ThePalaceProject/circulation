from __future__ import annotations

import pytest

from core.model import IntegrationConfiguration, Library, create
from core.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStatus,
)
from tests.fixtures.database import (
    DatabaseTransactionFixture,
    IntegrationConfigurationFixture,
)


class VendorIDFixture:
    """
    A fixture that knows how to set up a registry that provides an
    Adobe vendor id, and allows libraries to generate short client
    tokens for verification by the registry.
    """

    TEST_VENDOR_ID = "vendor id"

    db: DatabaseTransactionFixture
    registry: IntegrationConfiguration
    registration: DiscoveryServiceRegistration

    def initialize_adobe(
        self,
        vendor_id_library: Library,
    ):
        self.registry = self.integration_configuration.discovery_service()
        self.registration, _ = create(
            self.db.session,
            DiscoveryServiceRegistration,
            library=vendor_id_library,
            integration=self.registry,
            # The integration knows which Adobe Vendor ID server it gets its Adobe IDs from.
            vendor_id=self.TEST_VENDOR_ID,
        )

        # The library given to this fixture will be setup to be able to generate
        # Short Client Tokens.
        assert vendor_id_library.short_name is not None
        short_name = vendor_id_library.short_name + "token"
        secret = vendor_id_library.short_name + " token secret"
        self.registration.short_name = short_name
        self.registration.shared_secret = secret
        self.registration.status = RegistrationStatus.SUCCESS

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        integration_configuration: IntegrationConfigurationFixture,
    ) -> None:
        assert isinstance(db, DatabaseTransactionFixture)
        self.db = db
        self.integration_configuration = integration_configuration


@pytest.fixture(scope="function")
def vendor_id_fixture(
    db: DatabaseTransactionFixture,
    create_integration_configuration: IntegrationConfigurationFixture,
) -> VendorIDFixture:
    return VendorIDFixture(db, create_integration_configuration)
