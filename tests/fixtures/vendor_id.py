from __future__ import annotations

import pytest

from api.adobe_vendor_id import AuthdataUtility
from api.discovery.constants import RegistrationConstants
from core.model import ConfigurationSetting, ExternalIntegration, Library
from tests.fixtures.database import DatabaseTransactionFixture


class VendorIDFixture:
    """
    A fixture that knows how to set up a registry that provides an
    Adobe vendor id, and allows libraries to generate short client
    tokens for verification by the registry.
    """

    TEST_VENDOR_ID = "vendor id"

    db: DatabaseTransactionFixture
    registry: ExternalIntegration

    def initialize_adobe(
        self,
        vendor_id_library: Library,
    ):
        # The libraries will share a registry integration.
        self.registry = self.db.external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            libraries=[vendor_id_library],
        )

        # The integration knows which Adobe Vendor ID server it gets its Adobe IDs from.
        self.registry.set_setting(AuthdataUtility.VENDOR_ID_KEY, self.TEST_VENDOR_ID)

        # The library given to this fixture will be setup to be able to generate
        # Short Client Tokens.
        assert vendor_id_library.short_name is not None
        short_name = vendor_id_library.short_name + "token"
        secret = vendor_id_library.short_name + " token secret"
        ConfigurationSetting.for_library_and_externalintegration(
            self.db.session,
            ExternalIntegration.USERNAME,
            vendor_id_library,
            self.registry,
        ).value = short_name
        ConfigurationSetting.for_library_and_externalintegration(
            self.db.session,
            ExternalIntegration.PASSWORD,
            vendor_id_library,
            self.registry,
        ).value = secret
        ConfigurationSetting.for_library_and_externalintegration(
            self.db.session,
            RegistrationConstants.LIBRARY_REGISTRATION_STATUS,
            vendor_id_library,
            self.registry,
        ).value = RegistrationConstants.SUCCESS_STATUS

    def __init__(self, db: DatabaseTransactionFixture):
        assert isinstance(db, DatabaseTransactionFixture)
        self.db = db


@pytest.fixture(scope="function")
def vendor_id_fixture(db: DatabaseTransactionFixture) -> VendorIDFixture:
    return VendorIDFixture(db)
