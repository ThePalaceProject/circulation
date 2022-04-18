import json

from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2 import OPDS2FeedParserFactory

from api.odl2 import ODL2Importer
from core.model.configuration import ExternalIntegration
from core.model.datasource import DataSource
from core.opds2_import import OPDS2Importer, RWPMManifestParser
from core.opds_schema import ODL2SchemaValidation, OPDS2SchemaValidation
from tests.core.test_opds2_import import OPDS2Test


class TestOPDS2Validation(OPDS2Test):
    def test_opds2_schema(self):
        self._default_collection.protocol = ExternalIntegration.OPDS2_IMPORT
        self._default_collection.data_source = DataSource.FEEDBOOKS
        validator = OPDS2SchemaValidation(
            self._db,
            collection=self._default_collection,
            import_class=OPDS2Importer,
            parser=RWPMManifestParser(OPDS2FeedParserFactory()),
        )
        with open("tests/core/files/opds/opds2_feed.json") as fp:
            bookshelf_opds2 = json.load(fp)

        validator.import_one_feed(bookshelf_opds2)


class TestODL2Validation(OPDS2Test):
    def test_odl2_schema(self):
        self._default_collection.protocol = ExternalIntegration.ODL2
        self._default_collection.data_source = DataSource.FEEDBOOKS
        validator = ODL2SchemaValidation(
            self._db,
            collection=self._default_collection,
            import_class=ODL2Importer,
            parser=RWPMManifestParser(ODLFeedParserFactory()),
        )
        with open("tests/core/files/opds/odl2_feed.json") as fp:
            bookshelf_odl2 = fp.read()

        imported, failures = validator.import_one_feed(bookshelf_odl2)

        assert (len(imported), len(failures)) == (0, 0)
