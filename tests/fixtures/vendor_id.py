from __future__ import annotations

import pytest

from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
    RegistrationStatus,
)
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.util import create
from tests.fixtures.database import DatabaseTransactionFixture


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
        self.registry = self.db.discovery_service_integration()
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
    ) -> None:
        assert isinstance(db, DatabaseTransactionFixture)
        self.db = db


@pytest.fixture(scope="function")
def vendor_id_fixture(
    db: DatabaseTransactionFixture,
) -> VendorIDFixture:
    return VendorIDFixture(db)
