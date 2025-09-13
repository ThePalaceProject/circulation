import datetime
import json
import uuid
from typing import Any
from unittest.mock import MagicMock

from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.importer import (
    importer_from_collection,
)
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.odl import License
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OPDS2FilesFixture
from tests.fixtures.http import MockAsyncClientFixture
from tests.fixtures.services import ServicesFixture


class TestOpdsImporter:
    async def test_fetch_license_document(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        async_http_client: MockAsyncClientFixture,
    ) -> None:
        """Ensure that OPDS2WithODLImporter correctly retrieves license data from a license document."""

        def license_info_dict() -> dict[str, Any]:
            return LicenseInfo(
                identifier=str(uuid.uuid4()),
                status=LicenseStatus.available,
                checkouts=Checkouts(
                    available=10,
                ),
            ).model_dump(mode="json", exclude_none=True)

        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        # Use the real AsyncClient but with mocked transport via async_http_client fixture

        # Create a mock license with required structure
        def create_mock_license(identifier: str) -> License:
            mock_license = MagicMock(spec=License)
            mock_license.metadata = MagicMock()
            mock_license.metadata.identifier = identifier
            mock_link = MagicMock()
            mock_link.href = "http://example.org/license"
            mock_license.links = MagicMock()
            mock_license.links.get = MagicMock(return_value=mock_link)
            return mock_license

        # Test bad status code - need multiple responses due to retries
        # Worker client has 3 retries, so need 4 responses total (1 initial + 3 retries)
        for _ in range(4):
            async_http_client.queue_response(400, content=b"Bad Request")

        license_mock = create_mock_license("test-id-1")
        result = await importer._fetch_license_document(license_mock)
        assert result is None
        # Should have multiple requests due to retries (1 initial + 3 retries = 4 total)
        assert len(async_http_client.requests) == 4
        assert all(
            str(req.url) == "http://example.org/license"
            for req in async_http_client.requests
        )

        # Reset for next test
        async_http_client.reset_mock()

        # 200 status - parses response body
        expiry = utc_now() + datetime.timedelta(days=1)
        license_id = str(uuid.uuid4())
        license_helper = LicenseInfo(
            identifier=license_id,
            status=LicenseStatus.available,
            checkouts=Checkouts(
                available=10,
                left=4,
            ),
            terms=Terms(
                concurrency=11,
                expires=expiry,
            ),
        )
        async_http_client.queue_response(200, content=license_helper.model_dump_json())
        license_mock = create_mock_license(license_id)
        result = await importer._fetch_license_document(license_mock)

        assert result is not None
        identifier, parsed = result
        assert identifier == license_id
        assert parsed.checkouts.available == 10
        assert parsed.checkouts.left == 4
        assert parsed.terms.concurrency == 11
        assert parsed.terms.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.identifier

        # Reset for next test
        async_http_client.reset_mock()

        # 201 status - parses response body
        async_http_client.queue_response(201, content=license_helper.model_dump_json())
        license_mock = create_mock_license(license_id)
        result = await importer._fetch_license_document(license_mock)

        assert result is not None
        identifier, parsed = result
        assert identifier == license_id
        assert parsed.checkouts.available == 10
        assert parsed.checkouts.left == 4
        assert parsed.terms.concurrency == 11
        assert parsed.terms.expires == expiry
        assert parsed.status == LicenseStatus.available
        assert parsed.identifier == license_helper.identifier

        # Reset for next test
        async_http_client.reset_mock()

        # Bad data
        async_http_client.queue_response(201, content="{}")
        license_mock = create_mock_license("test-id-bad")
        result = await importer._fetch_license_document(license_mock)
        assert result is None

        # Reset for next test
        async_http_client.reset_mock()

        # No identifier
        license_dict = license_info_dict()
        license_dict.pop("identifier")
        async_http_client.queue_response(201, content=json.dumps(license_dict))
        license_mock = create_mock_license("test-no-id")
        result = await importer._fetch_license_document(license_mock)
        assert result is None

        # Reset for next test
        async_http_client.reset_mock()

        # No status
        license_dict = license_info_dict()
        license_dict.pop("status")
        async_http_client.queue_response(201, content=json.dumps(license_dict))
        license_mock = create_mock_license("test-no-status")
        result = await importer._fetch_license_document(license_mock)
        assert result is None

        # Reset for next test
        async_http_client.reset_mock()

        # Bad status
        license_dict = license_info_dict()
        license_dict["status"] = "bad"
        async_http_client.queue_response(201, content=json.dumps(license_dict))
        license_mock = create_mock_license("test-bad-status")
        result = await importer._fetch_license_document(license_mock)
        assert result is None

        # Reset for next test
        async_http_client.reset_mock()

        # No available
        license_dict = license_info_dict()
        license_dict["checkouts"].pop("available")
        async_http_client.queue_response(201, content=json.dumps(license_dict))
        license_mock = create_mock_license("test-no-available")
        result = await importer._fetch_license_document(license_mock)
        assert result is None

        # Reset for next test
        async_http_client.reset_mock()

        # Format str
        license_dict = license_info_dict()
        license_dict["format"] = "single format"
        license_dict["identifier"] = "format-test-1"
        async_http_client.queue_response(201, content=json.dumps(license_dict))
        license_mock = create_mock_license("format-test-1")
        result = await importer._fetch_license_document(license_mock)
        assert result is not None
        identifier, parsed = result
        assert parsed.formats == ("single format",)

        # Reset for next test
        async_http_client.reset_mock()

        # Format list
        license_dict = license_info_dict()
        license_dict["format"] = ["format1", "format2"]
        license_dict["identifier"] = "format-test-2"
        async_http_client.queue_response(201, content=json.dumps(license_dict))
        license_mock = create_mock_license("format-test-2")
        result = await importer._fetch_license_document(license_mock)
        assert result is not None
        identifier, parsed = result
        assert parsed.formats == ("format1", "format2")

    def test__extract_publications_from_feed(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        async_http_client: MockAsyncClientFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        opds2_feed["publications"] = [opds2_feed["publications"][0], {}]
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)

        # Queue multiple empty responses for license document fetching (if any)
        # Since the sample feed might have license URLs, we need to handle those
        # Need multiple responses in case of retries or multiple license documents
        for _ in range(10):  # Queue enough responses to handle any license requests
            async_http_client.queue_response(200, content="{}")

        successful, failed = importer._extract_publications_from_feed(feed)

        # Only the first publication is valid, so it is the one returned
        assert len(successful) == 1
        [(identifier, bibliographic)] = list(successful.items())

        assert identifier.type == Identifier.ISBN
        assert identifier.identifier == "978-3-16-148410-0"
        assert bibliographic.primary_identifier_data == identifier

        # The second publication is invalid, so it is in the failed list
        assert len(failed) == 1
        [failed_publication] = failed
        assert failed_publication.error_message == "Error validating publication"
        assert failed_publication.identifier is None
        assert failed_publication.title is None
        assert failed_publication.publication_data == "{}"

    def test__validate_and_filter_publications(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        """Test the first phase: validation and filtering of publications."""
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        # Add an invalid publication
        opds2_feed["publications"] = [opds2_feed["publications"][0], {}]
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)

        valid_results, failures = importer._validate_and_filter_publications(feed)

        # Should have one valid result
        assert len(valid_results) == 1
        identifier, publication, license_urls = valid_results[0]
        assert identifier.type == Identifier.ISBN
        assert identifier.identifier == "978-3-16-148410-0"

        # Should have one failure
        assert len(failures) == 1
        failed_publication = failures[0]
        assert failed_publication.error_message == "Error validating publication"

    def test__fetch_all_license_documents(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        async_http_client: MockAsyncClientFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        """Test the second phase: fetching license documents concurrently."""
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        # Create mock input data (what would come from phase 1)
        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)
        valid_results, _ = importer._validate_and_filter_publications(feed)

        # Mock license document responses (empty since no ODL licenses in sample)
        # Queue multiple responses to handle any license document requests
        for _ in range(10):
            async_http_client.queue_response(200, content="{}")

        results_with_licenses = importer._fetch_all_license_documents(valid_results)

        # Should have same number of results
        assert len(results_with_licenses) == len(valid_results)
        identifier, publication, license_info = results_with_licenses[0]
        assert identifier.type == Identifier.ISBN
        assert identifier.identifier == "978-3-16-148410-0"

    def test__extract_bibliographic_data(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        """Test the third phase: extracting bibliographic data."""
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        # Create mock input data (what would come from phase 2)
        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        # Use only the first publication (like the other tests)
        opds2_feed["publications"] = [opds2_feed["publications"][0]]
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)
        valid_results, _ = importer._validate_and_filter_publications(feed)

        # Simulate phase 2 output with empty license info
        from palace.manager.data_layer.identifier import IdentifierData
        from palace.manager.opds.odl.info import LicenseInfo
        from palace.manager.opds.odl.odl import Opds2OrOpds2WithOdlPublication

        publications_with_licenses: list[
            tuple[
                IdentifierData, Opds2OrOpds2WithOdlPublication, dict[str, LicenseInfo]
            ]
        ] = [
            (identifier, publication, {})
            for identifier, publication, license_urls in valid_results
        ]

        bibliographic_data, extraction_failures = importer._extract_bibliographic_data(
            publications_with_licenses
        )

        # Should successfully extract bibliographic data
        assert len(bibliographic_data) == 1
        assert len(extraction_failures) == 0

        [(identifier, bibliographic)] = list(bibliographic_data.items())
        assert identifier.type == Identifier.ISBN
        assert identifier.identifier == "978-3-16-148410-0"
        assert bibliographic.primary_identifier_data == identifier
