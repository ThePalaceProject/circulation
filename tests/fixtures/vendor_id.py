from __future__ import annotations

from typing import List

import pytest

from api.adobe_vendor_id import AuthdataUtility
from api.config import Configuration
from api.registration.constants import RegistrationConstants
from core.model import ConfigurationSetting, ExternalIntegration, Library
from tests.fixtures.database import DatabaseTransactionFixture


class VendorIDFixture:
    """A fixture that knows how to set up an Adobe Vendor ID integration."""

    TEST_VENDOR_ID = "vendor id"

    db: DatabaseTransactionFixture
    registry: ExternalIntegration

    def initialize_adobe(
        self,
        vendor_id_libraries: Library | List[Library],
    ):
        if isinstance(vendor_id_libraries, Library):
            vendor_id_libraries = [vendor_id_libraries]

        # The libraries will share a registry integration.
        self.registry = self.db.external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            libraries=vendor_id_libraries,
        )

        # The integration knows which Adobe Vendor ID server it gets its Adobe IDs from.
        self.registry.set_setting(AuthdataUtility.VENDOR_ID_KEY, self.TEST_VENDOR_ID)

        # Every library given to this fixture will be setup to be able to generate
        # Short Client Tokens.
        for library in vendor_id_libraries:
            # Each library will get a slightly different short
            # name and secret for generating Short Client Tokens.
            library_uri = self.db.fresh_url()
            assert library.short_name is not None
            short_name = library.short_name + "token"
            secret = library.short_name + " token secret"
            ConfigurationSetting.for_library_and_externalintegration(
                self.db.session, ExternalIntegration.USERNAME, library, self.registry
            ).value = short_name
            ConfigurationSetting.for_library_and_externalintegration(
                self.db.session, ExternalIntegration.PASSWORD, library, self.registry
            ).value = secret
            ConfigurationSetting.for_library_and_externalintegration(
                self.db.session,
                RegistrationConstants.LIBRARY_REGISTRATION_STATUS,
                library,
                self.registry,
            ).value = RegistrationConstants.SUCCESS_STATUS

            library.setting(Configuration.WEBSITE_URL).value = library_uri

    def __init__(self, db: DatabaseTransactionFixture):
        assert isinstance(db, DatabaseTransactionFixture)
        self.db = db


@pytest.fixture(scope="function")
def vendor_id_fixture(db: DatabaseTransactionFixture) -> VendorIDFixture:
    return VendorIDFixture(db)
