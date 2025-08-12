from __future__ import annotations

# class OPDSImporterFixture:
#     def __init__(
#         self,
#         db: DatabaseTransactionFixture,
#         opds_files_fixture: OPDSFilesFixture,
#         work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
#     ):
#         self.db = db
#         self.content_server_feed = opds_files_fixture.sample_data("content_server.opds")
#         self.content_server_mini_feed = opds_files_fixture.sample_text(
#             "content_server_mini.opds"
#         )
#         self.audiobooks_opds = opds_files_fixture.sample_data("audiobooks.opds")
#         self.wayfless_feed = opds_files_fixture.sample_data("wayfless.opds")
#         self.feed_with_id_and_dcterms_identifier = opds_files_fixture.sample_data(
#             "feed_with_id_and_dcterms_identifier.opds"
#         )
#         self.importer = partial(
#             OPDSImporter, _db=self.db.session, collection=self.db.default_collection()
#         )
#
#         self.work_policy_recalc_fixture = work_policy_recalc_fixture
#
#
# @pytest.fixture()
# def opds_importer_fixture(
#     db: DatabaseTransactionFixture,
#     opds_files_fixture: OPDSFilesFixture,
#     work_policy_recalc_fixture: WorkIdPolicyQueuePresentationRecalculationFixture,
# ) -> OPDSImporterFixture:
#     data = OPDSImporterFixture(db, opds_files_fixture, work_policy_recalc_fixture)
#     return data
