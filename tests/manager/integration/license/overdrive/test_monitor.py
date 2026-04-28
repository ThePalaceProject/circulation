from __future__ import annotations

from palace.manager.integration.license.overdrive.monitor import (
    OverdriveCollectionReaper,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.mocks.overdrive import MockOverdriveAPI


class TestReaper:
    def test_instantiate(
        self, overdrive_api_fixture: OverdriveAPIFixture, db: DatabaseTransactionFixture
    ):
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveCollectionReaper(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
