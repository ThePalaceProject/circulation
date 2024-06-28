import json
from contextlib import nullcontext

import pytest
from jsonschema.exceptions import ValidationError
from webpub_manifest_parser.odl import ODLFeedParserFactory
from webpub_manifest_parser.opds2 import OPDS2FeedParserFactory

from palace.manager.api.odl2 import ODL2API, ODL2Importer
from palace.manager.core.opds2_import import OPDS2API, OPDS2Importer, RWPMManifestParser
from palace.manager.core.opds_schema import ODL2SchemaValidation, OPDS2SchemaValidation
from palace.manager.sqlalchemy.model.datasource import DataSource
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import ODL2APIFilesFixture, OPDS2FilesFixture


class TestOPDS2Validation:
    @pytest.mark.parametrize(
        "feed_name, fail",
        [
            ("feed.json", False),
            ("feed2.json", False),
            ("bad_feed.json", True),
            ("bad_feed2.json", True),
        ],
    )
    def test_opds2_schema(
        self,
        feed_name: str,
        fail: bool,
        db: DatabaseTransactionFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ):
        collection = db.collection(
            protocol=OPDS2API.label(),
            data_source_name=DataSource.FEEDBOOKS,
            settings={
                "external_account_id": "http://example.com/feed",
            },
        )
        validator = OPDS2SchemaValidation(
            db.session,
            collection=collection,
            import_class=OPDS2Importer,
            parser=RWPMManifestParser(OPDS2FeedParserFactory()),
        )

        context = pytest.raises(ValidationError) if fail else nullcontext()

        feed = json.loads(opds2_files_fixture.sample_text(feed_name))
        with context:
            validator.import_one_feed(feed)


class TestODL2Validation:
    @pytest.mark.parametrize(
        "feed_name, fail",
        [
            ("feed.json", True),
            ("feed2.json", False),
        ],
    )
    def test_odl2_schema(
        self,
        feed_name: str,
        fail: bool,
        db: DatabaseTransactionFixture,
        api_odl2_files_fixture: ODL2APIFilesFixture,
    ):
        collection = db.collection(
            protocol=ODL2API.label(),
            data_source_name=DataSource.FEEDBOOKS,
            settings={
                "username": "username",
                "password": "password",
                "external_account_id": "http://example.com/feed",
            },
        )
        validator = ODL2SchemaValidation(
            db.session,
            collection=collection,
            import_class=ODL2Importer,
            parser=RWPMManifestParser(ODLFeedParserFactory()),
        )

        context = pytest.raises(ValidationError) if fail else nullcontext()

        feed = api_odl2_files_fixture.sample_text(feed_name)
        with context:
            imported, failures = validator.import_one_feed(feed)
            assert (len(imported), len(failures)) == (0, 0)
