import json

from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2 import OPDS2FeedParserFactory

from api.odl2 import ODL2Importer
from core.model.configuration import ExternalIntegration
from core.model.datasource import DataSource
from core.opds2_import import OPDS2Importer, RWPMManifestParser
from core.opds_schema import ODL2SchemaValidation, OPDS2SchemaValidation
from tests.core_new.test_opds2_import import OPDS2Test
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.opds_files import OPDSFilesFixture


class TestOPDS2Validation(OPDS2Test):
    def test_opds2_schema(
        self,
        database_transaction: DatabaseTransactionFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        transaction, session = database_transaction, database_transaction.session()

        transaction.default_collection().protocol = ExternalIntegration.OPDS2_IMPORT
        transaction.default_collection().data_source = DataSource.FEEDBOOKS
        validator = OPDS2SchemaValidation(
            session,
            collection=transaction.default_collection(),
            import_class=OPDS2Importer,
            parser=RWPMManifestParser(OPDS2FeedParserFactory()),
        )

        bookshelf_opds2 = json.loads(opds_files_fixture.sample_text("opds2_feed.json"))
        validator.import_one_feed(bookshelf_opds2)


class TestODL2Validation(OPDS2Test):
    def test_odl2_schema(
        self,
        database_transaction: DatabaseTransactionFixture,
        opds_files_fixture: OPDSFilesFixture,
    ):
        transaction, session = database_transaction, database_transaction.session()

        transaction.default_collection().protocol = ExternalIntegration.ODL2
        transaction.default_collection().data_source = DataSource.FEEDBOOKS
        validator = ODL2SchemaValidation(
            session,
            collection=transaction.default_collection(),
            import_class=ODL2Importer,
            parser=RWPMManifestParser(ODLFeedParserFactory()),
        )

        bookshelf_odl2 = opds_files_fixture.sample_text("odl2_feed.json")
        imported, failures = validator.import_one_feed(bookshelf_odl2)
        assert (len(imported), len(failures)) == (0, 0)
