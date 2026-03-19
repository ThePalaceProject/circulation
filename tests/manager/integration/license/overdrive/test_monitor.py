from __future__ import annotations

from typing import cast

from palace.manager.integration.license.overdrive.monitor import (
    OverdriveCollectionReaper,
    OverdriveFormatSweep,
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


class TestOverdriveFormatSweep:
    def test_process_item(self, overdrive_api_fixture: OverdriveAPIFixture):
        db = overdrive_api_fixture.db
        # Validate the standard CollectionMonitor interface.
        monitor = OverdriveFormatSweep(
            db.session, overdrive_api_fixture.collection, api_class=MockOverdriveAPI
        )
        overdrive_api_fixture.queue_collection_token()
        # We're not testing that the work actually gets done (that's
        # tested in test_update_formats), only that the monitor
        # implements the expected process_item API without crashing.
        overdrive_api_fixture.mock_http.queue_response(404)
        edition, pool = db.edition(with_license_pool=True)
        monitor.process_item(pool.identifier)

    def test_process_item_multiple_licence_pools(
        self, overdrive_api_fixture: OverdriveAPIFixture
    ):
        db = overdrive_api_fixture.db
        # Make sure that we only call update_formats once when an item
        # is part of multiple licensepools.

        class MockApi(MockOverdriveAPI):
            update_format_calls = 0

            def update_formats(self, licensepool):
                self.update_format_calls += 1

        monitor = OverdriveFormatSweep(
            db.session, overdrive_api_fixture.collection, api_class=MockApi
        )
        overdrive_api_fixture.queue_collection_token()
        overdrive_api_fixture.mock_http.queue_response(404)
        mock_api = cast(MockApi, monitor.api)

        edition = db.edition()
        collection1 = db.collection(name="Collection 1")
        pool1 = db.licensepool(edition, collection=collection1)

        collection2 = db.collection(name="Collection 2")
        pool2 = db.licensepool(edition, collection=collection2)

        monitor.process_item(pool1.identifier)
        assert mock_api.update_format_calls == 1
