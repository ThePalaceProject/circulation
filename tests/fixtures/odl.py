import types

import pytest

from api.odl import ODLAPI
from core.model import Collection, get_one_or_create
from tests.fixtures.database import DatabaseTransactionFixture


class ODLFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.library = self.db.default_library()

        """Create a mock ODL collection to use in tests."""
        self.collection, ignore = get_one_or_create(
            self.db.session,
            Collection,
            name="Test ODL Collection",
            create_method_kwargs=dict(
                external_account_id="http://odl",
            ),
        )
        integration = self.collection.create_external_integration(
            protocol=self.integration_protocol()
        )
        integration.username = "a"
        integration.password = "b"
        integration.url = "http://metadata"
        self.collection.external_integration.set_setting(
            Collection.DATA_SOURCE_NAME_SETTING, "Feedbooks"
        )
        self.library.collections.append(self.collection)
        self.work = self.db.work(with_license_pool=True, collection=self.collection)

        def setup(self, available, concurrency, left=None, expires=None):
            self.checkouts_available = available
            self.checkouts_left = left
            self.terms_concurrency = concurrency
            self.expires = expires
            self.license_pool.update_availability_from_licenses()

        self.pool = self.work.license_pools[0]
        self.license = self.db.license(
            self.pool,
            checkout_url="https://loan.feedbooks.net/loan/get/{?id,checkout_id,expires,patron_id,notification_url,hint,hint_url}",
            checkouts_available=1,
            terms_concurrency=1,
        )
        self.license.setup = types.MethodType(setup, self.license)
        self.pool.update_availability_from_licenses()
        self.patron = self.db.patron()

    @staticmethod
    def integration_protocol():
        return ODLAPI.NAME


@pytest.fixture(scope="function")
def odl_fixture(db: DatabaseTransactionFixture) -> ODLFixture:
    return ODLFixture(db)
