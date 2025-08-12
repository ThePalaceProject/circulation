# from palace.manager.integration.license.opds.for_distributors.importer import (
#     OPDSForDistributorsImporter,
# )
# from palace.manager.integration.license.opds.for_distributors.monitor import (
#     OPDSForDistributorsImportMonitor,
#     OPDSForDistributorsReaperMonitor,
# )
# from palace.manager.sqlalchemy.model.coverage import Timestamp
# from palace.manager.sqlalchemy.model.identifier import Identifier
# from palace.manager.sqlalchemy.model.licensing import LicensePool
# from palace.manager.sqlalchemy.util import create
# from palace.manager.util.opds_writer import OPDSFeed
# from tests.manager.integration.license.opds.for_distributors.conftest import (
#     OPDSForDistributorsAPIFixture,
# )
# from tests.mocks.mock import MockRequestsResponse
#
#
# class TestOPDSForDistributorsReaperMonitor:
#     def test_reaper(self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture):
#         feed = opds_dist_api_fixture.files.sample_data("biblioboard_mini_feed.opds")
#
#         class MockOPDSForDistributorsReaperMonitor(OPDSForDistributorsReaperMonitor):
#             """An OPDSForDistributorsReaperMonitor that overrides _get."""
#
#             def _get(self, url, headers):
#                 return MockRequestsResponse(
#                     200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, feed
#                 )
#
#         collection = opds_dist_api_fixture.mock_collection()
#         monitor = MockOPDSForDistributorsReaperMonitor(
#             opds_dist_api_fixture.db.session,
#             collection,
#             OPDSForDistributorsImporter,
#         )
#
#         # There's a license pool in the database that isn't in the feed anymore.
#         edition, now_gone = opds_dist_api_fixture.db.edition(
#             identifier_type=Identifier.URI,
#             with_license_pool=True,
#             collection=collection,
#         )
#         now_gone.licenses_owned = LicensePool.UNLIMITED_ACCESS
#         now_gone.licenses_available = LicensePool.UNLIMITED_ACCESS
#
#         edition, still_there = opds_dist_api_fixture.db.edition(
#             identifier_type=Identifier.URI,
#             identifier_id="urn:uuid:04377e87-ab69-41c8-a2a4-812d55dc0952",
#             with_license_pool=True,
#             collection=collection,
#         )
#         still_there.licenses_owned = LicensePool.UNLIMITED_ACCESS
#         still_there.licenses_available = LicensePool.UNLIMITED_ACCESS
#
#         progress = monitor.run_once(monitor.timestamp().to_data())
#
#         # One LicensePool has been cleared out.
#         assert 0 == now_gone.licenses_owned
#         assert 0 == now_gone.licenses_available
#
#         # The other is still around.
#         assert LicensePool.UNLIMITED_ACCESS == still_there.licenses_owned
#         assert LicensePool.UNLIMITED_ACCESS == still_there.licenses_available
#
#         # The TimestampData returned by run_once() describes its
#         # achievements.
#         assert "License pools removed: 1." == progress.achievements
#
#         # The TimestampData does not include any timing information --
#         # that will be applied by run().
#         assert None == progress.start
#         assert None == progress.finish
#
#
# class TestOPDSForDistributorsImportMonitor:
#     def test_opds_import_has_db_failure(
#         self, opds_dist_api_fixture: OPDSForDistributorsAPIFixture
#     ):
#         feed = opds_dist_api_fixture.files.sample_data("biblioboard_mini_feed.opds")
#
#         class MockOPDSForDistributorsImportMonitor(OPDSForDistributorsImportMonitor):
#             """An OPDSForDistributorsImportMonitor that overrides _get."""
#
#             def _get(self, url, headers):
#                 # This should cause a database failure on commit
#                 ts = create(self._db, Timestamp)
#                 return (200, {"content-type": OPDSFeed.ACQUISITION_FEED_TYPE}, feed)
#
#         collection = opds_dist_api_fixture.mock_collection()
#         monitor = MockOPDSForDistributorsImportMonitor(
#             opds_dist_api_fixture.db.session,
#             collection,
#             OPDSForDistributorsImporter,
#         )
#
#         monitor.run()
#
#         assert monitor.timestamp().exception is not None
