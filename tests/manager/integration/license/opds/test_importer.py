import asyncio
import datetime
import json
import uuid
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.importer import (
    importer_from_collection,
)
from palace.manager.opds.odl.info import Checkouts, LicenseInfo, LicenseStatus
from palace.manager.opds.odl.odl import (
    License,
    Opds2OrOpds2WithOdlPublication,
    Publication,
)
from palace.manager.opds.odl.terms import Terms
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.http.exception import BadResponseException
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

        # Set the backoff to None to avoid delays in tests
        importer._async_http_client._backoff = None

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
        with pytest.raises(
            BadResponseException, match="Got status code 400 from external server"
        ):
            await importer._fetch_license_document(license_mock)

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

        # Create mock input data (which would come from phase 2)
        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))

        # Use only the first publication (like the other tests)
        opds2_feed["publications"] = [opds2_feed["publications"][0]]
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)
        valid_results, _ = importer._validate_and_filter_publications(feed)

        # Simulate phase 2 output with empty license info
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

    async def test__fetch_license_documents_concurrently_cancels_on_bad_response(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        async_http_client: MockAsyncClientFixture,
        opds2_files_fixture: OPDS2FilesFixture,
    ) -> None:
        """Test that when one fetch returns BadResponseException, all other outstanding tasks are cancelled."""
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        # Set the backoff to None to avoid delays in tests
        importer._async_http_client._backoff = None

        # Create a mock publication with licenses
        def create_mock_publication_with_license(
            identifier: str,
        ) -> tuple[Publication, list[License]]:
            mock_publication = MagicMock()
            mock_license = MagicMock()
            mock_license.metadata.identifier = identifier
            mock_license.metadata.availability.available = True
            mock_link = MagicMock()
            mock_link.href = f"http://example.org/license/{identifier}"
            mock_license.links.get = MagicMock(return_value=mock_link)
            return mock_publication, [mock_license]

        # Create test input data
        results = []
        for i in range(4):
            identifier = IdentifierData(type="Test Identifier", identifier=f"{i}")
            publication, licenses = create_mock_publication_with_license(f"license-{i}")
            license_urls: dict[str, str] = {
                f"license-{i}": f"http://example.org/license/{i}"
            }
            results.append((identifier, publication, license_urls))

        # Track which tasks get cancelled
        cancelled_tasks: list[asyncio.Task[Any]] = []
        created_tasks: list[asyncio.Task[Any]] = []

        # Track asyncio task creation and cancellation
        original_create_task = asyncio.create_task

        def track_create_task(coro: Any) -> asyncio.Task[Any]:
            task = original_create_task(coro)
            created_tasks.append(task)

            # Wrap the cancel method to track cancellations
            original_cancel = task.cancel

            def tracked_cancel(msg: Any = None) -> bool:
                result = original_cancel(msg)
                if result:
                    cancelled_tasks.append(task)
                return result

            task.cancel = tracked_cancel
            return task

        # Mock _fetch_license_documents to simulate one task failing
        call_count = 0

        async def mock_fetch_license_documents(
            publication: Any,
        ) -> dict[str, LicenseInfo]:
            nonlocal call_count
            call_count += 1

            # First call completes successfully
            if call_count == 1:
                return {}

            # Second call raises BadResponseException
            elif call_count == 2:
                await asyncio.sleep(0.01)  # Small delay to ensure other tasks start
                # Create a mock response with status_code for the exception
                mock_response = MagicMock()
                mock_response.status_code = 500
                mock_response.text = "Test error response"
                raise BadResponseException(
                    "http://test.com", "Test error", mock_response
                )

            # Other calls should be long-running (will be cancelled)
            else:
                await asyncio.sleep(10)
                return {}

        # Apply the mocks
        with (
            patch.object(asyncio, "create_task", side_effect=track_create_task),
            patch.object(
                importer,
                "_fetch_license_documents",
                side_effect=mock_fetch_license_documents,
            ),
        ):
            # Call the method and expect it to raise BadResponseException
            with pytest.raises(BadResponseException, match="Test error"):
                await importer._fetch_license_documents_concurrently(results)

            # Verify that tasks were created
            assert len(created_tasks) == 4

            # After the exception, the other tasks should have been cancelled
            assert len(cancelled_tasks) == 2

    @pytest.mark.parametrize("import_even_if_unchanged", [True, False])
    def test_import_feed_replacement_policy(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
        opds2_files_fixture: OPDS2FilesFixture,
        import_even_if_unchanged: bool,
    ) -> None:
        """Test that import_feed creates and passes the correct ReplacementPolicy to apply_bibliographic."""
        collection = db.collection(
            protocol=OPDS2WithODLApi,
            settings=db.opds2_odl_settings(data_source="test collection"),
        )
        registry = services_fixture.services.integration_registry().license_providers()
        importer = importer_from_collection(collection, registry)

        # Create mock feed data
        opds2_feed = json.loads(opds2_files_fixture.sample_text("feed.json"))
        opds2_feed["publications"] = [opds2_feed["publications"][0]]
        feed = PublicationFeedNoValidation.model_validate(opds2_feed)

        # Create mock bibliographic data
        mock_identifier = IdentifierData(
            type=Identifier.ISBN, identifier="978-3-16-148410-0"
        )
        mock_bibliographic = MagicMock(spec=BibliographicData)
        mock_bibliographic.has_changed.return_value = True
        mock_bibliographic.circulation = None

        # Mock the feed fetching and extraction
        with (
            patch.object(importer, "_fetch_feed", return_value=feed),
            patch.object(
                importer,
                "_extract_publications_from_feed",
                return_value=({mock_identifier: mock_bibliographic}, []),
            ),
        ):
            mock_apply_bibliographic = MagicMock()
            importer.import_feed(
                collection,
                apply_bibliographic=mock_apply_bibliographic,
                import_even_if_unchanged=import_even_if_unchanged,
            )

        # Verify apply_bibliographic was called with the correct ReplacementPolicy
        assert mock_apply_bibliographic.call_count == 1
        call_args = mock_apply_bibliographic.call_args
        assert call_args is not None
        assert "replace" in call_args.kwargs
        replacement_policy = call_args.kwargs["replace"]
        assert isinstance(replacement_policy, ReplacementPolicy)
        assert (
            replacement_policy.even_if_not_apparently_updated
            is import_even_if_unchanged
        )
