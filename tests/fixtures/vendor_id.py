import json
from typing import List

import pytest

from api.adobe_vendor_id import AuthdataUtility
from api.config import Configuration
from api.registration.constants import RegistrationConstants
from core.model import ConfigurationSetting, ExternalIntegration, Library
from tests.fixtures.database import DatabaseTransactionFixture


class VendorIDFixture:
    """A fixture that knows how to set up an Adobe Vendor ID
    integration.
    """

    TEST_VENDOR_ID = "vendor id"
    TEST_NODE_VALUE = 114740953091845

    db: DatabaseTransactionFixture
    adobe_vendor_id: ExternalIntegration
    registry: ExternalIntegration

    def initialize_adobe(
        self, vendor_id_library: Library, short_token_libraries: List[Library] = []
    ):
        short_token_libraries = list(short_token_libraries)
        if not vendor_id_library in short_token_libraries:
            short_token_libraries.append(vendor_id_library)

        # The first library acts as an Adobe Vendor ID server.
        self.adobe_vendor_id = self.db.external_integration(
            ExternalIntegration.ADOBE_VENDOR_ID,
            ExternalIntegration.DRM_GOAL,
            username=VendorIDFixture.TEST_VENDOR_ID,
            libraries=[vendor_id_library],
        )

        # The other libraries will share a registry integration.
        self.registry = self.db.external_integration(
            ExternalIntegration.OPDS_REGISTRATION,
            ExternalIntegration.DISCOVERY_GOAL,
            libraries=short_token_libraries,
        )

        # The integration knows which Adobe Vendor ID server it
        # gets its Adobe IDs from.
        self.registry.set_setting(
            AuthdataUtility.VENDOR_ID_KEY, self.adobe_vendor_id.username
        )

        # As we give libraries their Short Client Token settings,
        # we build the 'other_libraries' setting we'll apply to the
        # Adobe Vendor ID integration.
        other_libraries = dict()

        # Every library in the system can generate Short Client
        # Tokens.
        for library in short_token_libraries:
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

            # Each library's Short Client Token configuration will be registered
            # with that Adobe Vendor ID server.
            if library != vendor_id_library:
                other_libraries[library_uri] = (short_name, secret)

        # Tell the Adobe Vendor ID server about the other libraries.
        other_libraries_str = json.dumps(other_libraries)
        self.adobe_vendor_id.set_setting(
            AuthdataUtility.OTHER_LIBRARIES_KEY, other_libraries_str
        )

    def __init__(self, db: DatabaseTransactionFixture):
        assert isinstance(db, DatabaseTransactionFixture)
        self.db = db


@pytest.fixture(scope="function")
def vendor_id_fixture(db: DatabaseTransactionFixture) -> VendorIDFixture:
    return VendorIDFixture(db)
