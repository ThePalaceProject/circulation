from contextlib import nullcontext

import pytest
from pydantic import ValidationError

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.odl.importer import OPDS2WithODLImporter
from palace.manager.core.opds2_import import OPDS2API, OPDS2Importer
from palace.manager.core.opds_schema import (
    OPDS2SchemaValidation,
    OPDS2WithODLSchemaValidation,
)
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture, OPDS2WithODLFilesFixture


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
            protocol=OPDS2API,
        )
        validator = OPDS2SchemaValidation(
            db.session,
            collection=collection,
            import_class=OPDS2Importer,
        )

        context = pytest.raises(ValidationError) if fail else nullcontext()

        feed = opds2_files_fixture.sample_text(feed_name)
        with context:
            validator.import_one_feed(feed)


class TestOPDS2WithODLValidation:
    @pytest.mark.parametrize(
        "feed_name, fail",
        [
            ("feed.json", True),
            ("feed2.json", False),
        ],
    )
    def test_opds2_with_odl_schema(
        self,
        feed_name: str,
        fail: bool,
        db: DatabaseTransactionFixture,
        opds2_with_odl_files_fixture: OPDS2WithODLFilesFixture,
    ):
        collection = db.collection(
            protocol=OPDS2WithODLApi,
        )
        validator = OPDS2WithODLSchemaValidation(
            db.session,
            collection=collection,
            import_class=OPDS2WithODLImporter,
        )

        context = pytest.raises(ValidationError) if fail else nullcontext()

        feed = opds2_with_odl_files_fixture.sample_text(feed_name)
        with context:
            imported, failures = validator.import_one_feed(feed)
            assert (len(imported), len(failures)) == (0, 0)
