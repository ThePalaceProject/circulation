from unittest.mock import ANY, create_autospec, patch

from api.lcp.importer import LCPImporter
from api.lcp.mirror import LCPMirror
from core.model import DataSource, ExternalIntegration, Identifier, Representation
from core.s3 import MinIOUploaderConfiguration, S3UploaderConfiguration
from tests.api.lcp.database_test import DatabaseTest


class TestLCPMirror(DatabaseTest):
    def setup_method(self):
        super().setup_method()

        settings = {
            S3UploaderConfiguration.PROTECTED_CONTENT_BUCKET_KEY: "encrypted-books",
            MinIOUploaderConfiguration.ENDPOINT_URL: "http://minio",
        }
        integration = self._external_integration(
            ExternalIntegration.LCP,
            goal=ExternalIntegration.STORAGE_GOAL,
            settings=settings,
        )
        self._lcp_collection = self._collection(protocol=ExternalIntegration.LCP)
        self._lcp_mirror = LCPMirror(integration)

    def test_book_url(self):
        # Arrange
        data_source = DataSource.lookup(self._db, DataSource.LCP, autocreate=True)
        identifier = Identifier(identifier="12345", type=Identifier.ISBN)

        # Act
        result = self._lcp_mirror.book_url(identifier, data_source=data_source)

        # Assert
        assert result == "http://encrypted-books.minio/12345"

    def test_mirror_one(self):
        # Arrange
        expected_identifier = "12345"
        mirror_url = "http://encrypted-books.minio/" + expected_identifier
        lcp_importer = create_autospec(spec=LCPImporter)
        representation, _ = self._representation(
            media_type=Representation.EPUB_MEDIA_TYPE, content="12345"
        )

        # Act
        with patch("api.lcp.mirror.LCPImporter") as lcp_importer_constructor:
            lcp_importer_constructor.return_value = lcp_importer
            self._lcp_mirror.mirror_one(
                representation, mirror_to=mirror_url, collection=self._lcp_collection
            )

            # Assert
            lcp_importer.import_book.assert_called_once_with(
                self._db, ANY, expected_identifier
            )
