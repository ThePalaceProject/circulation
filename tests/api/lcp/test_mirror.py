from unittest.mock import ANY, create_autospec, patch

import pytest

from api.lcp.importer import LCPImporter
from api.lcp.mirror import LCPMirror
from core.model import (
    Collection,
    DataSource,
    ExternalIntegration,
    Identifier,
    Representation,
)
from core.s3 import MinIOUploaderConfiguration, S3UploaderConfiguration
from tests.fixtures.database import DatabaseTransactionFixture


class LCPMirrorFixture:
    db: DatabaseTransactionFixture
    lcp_collection: Collection
    lcp_mirror: LCPMirror

    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db

        settings = {
            S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "encrypted-books",
            MinIOUploaderConfiguration.ENDPOINT_URL: "http://minio",
        }
        integration = self.db.external_integration(
            ExternalIntegration.LCP,
            goal=ExternalIntegration.STORAGE_GOAL,
            settings=settings,
        )
        self.lcp_collection = self.db.collection(protocol=ExternalIntegration.LCP)
        self.lcp_mirror = LCPMirror(integration)


@pytest.fixture(scope="function")
def lcp_mirror_fixture(db: DatabaseTransactionFixture) -> LCPMirrorFixture:
    return LCPMirrorFixture(db)


class TestLCPMirror:
    def test_book_url(self, lcp_mirror_fixture: LCPMirrorFixture):
        # Arrange
        data_source = DataSource.lookup(
            lcp_mirror_fixture.db.session, DataSource.LCP, autocreate=True
        )
        identifier = Identifier(identifier="12345", type=Identifier.ISBN)

        # Act
        result = lcp_mirror_fixture.lcp_mirror.book_url(
            identifier, data_source=data_source
        )

        # Assert
        assert result == "http://encrypted-books.minio/12345"

    def test_mirror_one(self, lcp_mirror_fixture: LCPMirrorFixture):
        # Arrange
        expected_identifier = "12345"
        mirror_url = "http://encrypted-books.minio/" + expected_identifier
        lcp_importer = create_autospec(spec=LCPImporter)
        representation, _ = lcp_mirror_fixture.db.representation(
            media_type=Representation.EPUB_MEDIA_TYPE, content="12345"
        )

        # Act
        with patch("api.lcp.mirror.LCPImporter") as lcp_importer_constructor:
            lcp_importer_constructor.return_value = lcp_importer
            lcp_mirror_fixture.lcp_mirror.mirror_one(
                representation,
                mirror_to=mirror_url,
                collection=lcp_mirror_fixture.lcp_collection,
            )

            # Assert
            lcp_importer.import_book.assert_called_once_with(
                lcp_mirror_fixture.db.session, ANY, expected_identifier
            )
