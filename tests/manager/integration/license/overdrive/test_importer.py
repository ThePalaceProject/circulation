"""Tests for the OverdriveImporter class."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
)
from palace.manager.integration.license.overdrive.importer import (
    FeedImportResult,
    OverdriveImporter,
)
from palace.manager.integration.license.overdrive.representation import (
    OverdriveRepresentationExtractor,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.services import ServicesFixture


class TestOverdriveImporter:
    """Tests for the OverdriveImporter class."""

    def test_init_success(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test successful initialization of OverdriveImporter."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        assert importer._db == db.session
        assert importer._collection == collection
        assert importer._import_all is False
        assert importer._identifier_set is None
        assert importer._parent_identifiers is None
        assert isinstance(importer._api, OverdriveAPI)
        assert isinstance(importer._extractor, OverdriveRepresentationExtractor)

    def test_init_with_api_provided(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test initialization when API instance is provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_api = Mock(spec=OverdriveAPI)

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry, api=mock_api
        )

        assert importer._api == mock_api

    def test_init_with_import_all(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test initialization with import_all flag set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            import_all=True,
        )

        assert importer._import_all is True

    def test_init_with_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test initialization with identifier_set provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_identifier_set = Mock(spec=IdentifierSet)

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            identifier_set=mock_identifier_set,
        )

        assert importer._identifier_set == mock_identifier_set

    def test_init_with_parent_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test initialization with parent_identifier_set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        # Create mock identifiers
        mock_id1 = Mock()
        mock_id1.identifier = "id1"
        mock_id2 = Mock()
        mock_id2.identifier = "id2"

        mock_parent_set = Mock(spec=IdentifierSet)
        mock_parent_set.get.return_value = [mock_id1, mock_id2]

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            parent_identifier_set=mock_parent_set,
        )

        assert importer._parent_identifiers == {"id1", "id2"}

    def test_init_invalid_collection_protocol(
        self,
        db: DatabaseTransactionFixture,
        services_fixture: ServicesFixture,
    ):
        """Test that initialization fails with invalid collection protocol."""
        # Create a collection with wrong protocol
        collection = db.collection(protocol="Not Overdrive")
        registry = services_fixture.services.integration_registry.license_providers()

        with pytest.raises(PalaceValueError) as exc:
            OverdriveImporter(db=db.session, collection=collection, registry=registry)

        assert "is not an OverDrive collection" in str(exc.value)

    def test_get_timestamp(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test get_timestamp creates or retrieves a timestamp."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        # First call should create a new timestamp
        timestamp1 = importer.get_timestamp()
        assert isinstance(timestamp1, Timestamp)
        assert timestamp1.service == "OverDrive Import"
        assert timestamp1.service_type == Timestamp.TASK_TYPE
        assert timestamp1.collection == collection

        # Second call should retrieve the same timestamp
        timestamp2 = importer.get_timestamp()
        assert timestamp1.id == timestamp2.id

    def test_get_start_time_default(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _get_start_time returns default when timestamp.start is None."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        timestamp = importer.get_timestamp()
        # timestamp.start should be None initially
        assert timestamp.start is None

        start_time = importer._get_start_time(timestamp)
        assert start_time == OverdriveImporter.DEFAULT_START_TIME

    def test_get_start_time_with_import_all(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _get_start_time returns default when import_all is True."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry, import_all=True
        )

        timestamp = importer.get_timestamp()
        # Set a start time, but it should be ignored due to import_all
        timestamp.start = datetime_utc(2020, 1, 1)

        start_time = importer._get_start_time(timestamp)
        assert start_time == OverdriveImporter.DEFAULT_START_TIME

    def test_get_start_time_with_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _get_start_time returns default when identifier_set is provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_identifier_set = Mock(spec=IdentifierSet)

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            identifier_set=mock_identifier_set,
        )

        timestamp = importer.get_timestamp()
        # Set a start time, but it should be ignored due to identifier_set
        timestamp.start = datetime_utc(2020, 1, 1)

        start_time = importer._get_start_time(timestamp)
        assert start_time == OverdriveImporter.DEFAULT_START_TIME

    def test_get_start_time_uses_timestamp_start(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _get_start_time returns timestamp.start in normal case."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        timestamp = importer.get_timestamp()
        expected_start = datetime_utc(2022, 6, 15)
        timestamp.start = expected_start

        start_time = importer._get_start_time(timestamp)
        assert start_time == expected_start

    @patch("asyncio.run")
    def test_import_collection_basic(
        self,
        mock_asyncio_run: MagicMock,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection with basic book data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        # Mock the API and extractor
        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data returned from API
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Test Book"},
                "availabilityV2": {"copiesOwned": 1, "copiesAvailable": 1},
            }
        ]
        mock_next_endpoint = BookInfoEndpoint(url="http://next.page")

        mock_asyncio_run.return_value = (mock_book_data, mock_next_endpoint)

        # Mock extractor methods
        mock_bibliographic = Mock(spec=BibliographicData)
        mock_bibliographic.has_changed.return_value = True
        mock_circulation = Mock(spec=CirculationData)
        mock_circulation.has_changed.return_value = True

        importer._extractor.book_info_to_bibliographic = Mock(
            return_value=mock_bibliographic
        )
        importer._extractor.book_info_to_circulation = Mock(
            return_value=mock_circulation
        )

        # Run import
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify result
        assert isinstance(result, FeedImportResult)
        assert result.processed_count == 1
        assert result.next_page == mock_next_endpoint

        # Verify apply functions were called
        assert mock_apply_bib.call_count == 1
        assert mock_apply_circ.call_count == 1

        # Verify identifier was created
        identifier, _ = Identifier.for_foreign_id(
            db.session,
            foreign_id="overdrive-id-1",
            foreign_identifier_type=Identifier.OVERDRIVE_ID,
        )
        assert identifier is not None

    @patch("asyncio.run")
    def test_import_collection_with_endpoint_provided(
        self,
        mock_asyncio_run: MagicMock,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection when endpoint is provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)
        custom_endpoint = BookInfoEndpoint(url="http://custom.endpoint")

        # Mock empty book data
        mock_asyncio_run.return_value = ([], None)

        # Run import with custom endpoint
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
            endpoint=custom_endpoint,
        )

        # Verify the custom endpoint was used
        assert result.current_page == custom_endpoint
        assert result.processed_count == 0

    @patch("asyncio.run")
    def test_import_collection_with_identifier_set(
        self,
        mock_asyncio_run: MagicMock,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection adds identifiers to identifier_set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_identifier_set = Mock(spec=IdentifierSet)

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            identifier_set=mock_identifier_set,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Test Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]
        mock_asyncio_run.return_value = (mock_book_data, None)

        # Mock extractor
        mock_bibliographic = Mock(spec=BibliographicData)
        mock_bibliographic.has_changed.return_value = True
        mock_circulation = Mock(spec=CirculationData)
        mock_circulation.has_changed.return_value = True

        importer._extractor.book_info_to_bibliographic = Mock(
            return_value=mock_bibliographic
        )
        importer._extractor.book_info_to_circulation = Mock(
            return_value=mock_circulation
        )

        # Run import
        importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify identifier was added to set
        mock_identifier_set.add.assert_called_once()

    @patch("asyncio.run")
    def test_import_collection_skips_unchanged_metadata(
        self,
        mock_asyncio_run: MagicMock,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection skips unchanged bibliographic data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Unchanged Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]
        mock_asyncio_run.return_value = (mock_book_data, None)

        # Mock extractor - bibliographic hasn't changed
        mock_bibliographic = Mock(spec=BibliographicData)
        mock_bibliographic.has_changed.return_value = False  # Not changed
        mock_circulation = Mock(spec=CirculationData)
        mock_circulation.has_changed.return_value = True

        importer._extractor.book_info_to_bibliographic = Mock(
            return_value=mock_bibliographic
        )
        importer._extractor.book_info_to_circulation = Mock(
            return_value=mock_circulation
        )

        # Run import
        importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Bibliographic should not be applied
        assert mock_apply_bib.call_count == 0
        # But circulation should still be applied
        assert mock_apply_circ.call_count == 1

    @patch("asyncio.run")
    def test_import_collection_with_import_all_flag(
        self,
        mock_asyncio_run: MagicMock,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection with import_all=True imports even unchanged data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry, import_all=True
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Test Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]
        mock_asyncio_run.return_value = (mock_book_data, None)

        # Mock extractor - data hasn't changed but should be imported anyway
        mock_bibliographic = Mock(spec=BibliographicData)
        mock_bibliographic.has_changed.return_value = False  # Not changed
        mock_circulation = Mock(spec=CirculationData)
        mock_circulation.has_changed.return_value = False  # Not changed

        importer._extractor.book_info_to_bibliographic = Mock(
            return_value=mock_bibliographic
        )
        importer._extractor.book_info_to_circulation = Mock(
            return_value=mock_circulation
        )

        # Run import
        importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Both should be applied despite not having changed
        assert mock_apply_bib.call_count == 1
        assert mock_apply_circ.call_count == 1

    @patch("asyncio.run")
    def test_import_collection_handles_missing_metadata(
        self,
        mock_asyncio_run: MagicMock,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection handles books with missing metadata."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data without metadata
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": None,  # Missing metadata
                "availabilityV2": {"copiesOwned": 1},
            }
        ]
        mock_asyncio_run.return_value = (mock_book_data, None)

        mock_circulation = Mock(spec=CirculationData)
        mock_circulation.has_changed.return_value = True

        importer._extractor.book_info_to_circulation = Mock(
            return_value=mock_circulation
        )

        # Run import
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Bibliographic should not be called due to missing metadata
        assert mock_apply_bib.call_count == 0
        # But circulation should still be processed
        assert mock_apply_circ.call_count == 1
        assert result.processed_count == 1


class TestFeedImportResult:
    """Tests for the FeedImportResult dataclass."""

    def test_feed_import_result_basic(self):
        """Test FeedImportResult creation with basic data."""
        current_page = BookInfoEndpoint(url="http://current.page")
        result = FeedImportResult(current_page=current_page)

        assert result.current_page == current_page
        assert result.next_page is None
        assert result.processed_count == 0

    def test_feed_import_result_with_all_fields(self):
        """Test FeedImportResult with all fields populated."""
        current_page = BookInfoEndpoint(url="http://current.page")
        next_page = BookInfoEndpoint(url="http://next.page")

        result = FeedImportResult(
            current_page=current_page, next_page=next_page, processed_count=42
        )

        assert result.current_page == current_page
        assert result.next_page == next_page
        assert result.processed_count == 42

    def test_feed_import_result_frozen(self):
        """Test that FeedImportResult is immutable (frozen)."""
        current_page = BookInfoEndpoint(url="http://current.page")
        result = FeedImportResult(current_page=current_page)

        # Should raise an error when trying to modify
        with pytest.raises(AttributeError):
            result.processed_count = 100
