"""Tests for the OverdriveImporter class."""

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

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

    def test_import_collection_basic(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection with basic book data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
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

        api.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, mock_next_endpoint)
        )

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

    def test_import_collection_with_endpoint_provided(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection when endpoint is provided."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)
        custom_endpoint = BookInfoEndpoint(url="http://custom.endpoint")

        # Mock empty book data
        api.fetch_book_info_list = AsyncMock(return_value=([], None))

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

    def test_import_collection_with_identifier_set(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection adds identifiers to identifier_set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        mock_identifier_set = Mock(spec=IdentifierSet)
        api = overdrive_api_fixture.api
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            identifier_set=mock_identifier_set,
            api=api,
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
        api.fetch_book_info_list = AsyncMock(return_value=(mock_book_data, None))

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

    def test_import_collection_skips_unchanged_metadata(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection skips unchanged bibliographic data."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api
        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
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
        api.fetch_book_info_list = AsyncMock(return_value=(mock_book_data, None))

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

    def test_import_collection_skips_next_page_when_all_books_out_of_scope(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test that next page is not returned when all books are out of scope."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        # Set modified_since to a date after all books were added
        modified_since = datetime_utc(2023, 6, 1)

        # Mock book data where all books have date_added before modified_since
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Old Book 1"},
                "availabilityV2": {"copiesOwned": 1},
                "date_added": "2023-01-15T00:00:00Z",  # Before modified_since
            },
            {
                "id": "overdrive-id-2",
                "metadata": {"title": "Old Book 2"},
                "availabilityV2": {"copiesOwned": 1},
                "date_added": "2023-02-10T00:00:00Z",  # Before modified_since
            },
            {
                "id": "overdrive-id-3",
                "metadata": {"title": "Old Book 3"},
                "availabilityV2": {"copiesOwned": 1},
                "date_added": "2023-03-20T00:00:00Z",  # Before modified_since
            },
        ]
        # API would normally return a next page, but it should be ignored
        mock_next_endpoint = BookInfoEndpoint(url="http://next.page")
        api.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, mock_next_endpoint)
        )

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
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify that books were still processed
        assert result.processed_count == 3

        # Verify that next_page is None even though API returned one
        # because all books are out of scope
        assert result.next_page is None

    def test_import_collection_with_import_all_ignores_out_of_scope_check(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test that import_all bypasses the out-of-scope check and continues to next page."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
            import_all=True,  # This should bypass the out-of-scope check
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 6, 1)

        # Mock book data where all books are out of scope
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Old Book 1"},
                "availabilityV2": {"copiesOwned": 1},
                "date_added": "2023-01-15T00:00:00Z",  # Before modified_since
            },
            {
                "id": "overdrive-id-2",
                "metadata": {"title": "Old Book 2"},
                "availabilityV2": {"copiesOwned": 1},
                "date_added": "2023-02-10T00:00:00Z",  # Before modified_since
            },
        ]
        # API returns a next page
        mock_next_endpoint = BookInfoEndpoint(url="http://next.page")
        api.fetch_book_info_list = AsyncMock(
            return_value=(mock_book_data, mock_next_endpoint)
        )

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
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify that next_page is still returned even though all books are out of scope
        # because import_all=True bypasses the check
        assert result.next_page == mock_next_endpoint
        assert result.processed_count == 2

    def test_import_collection_handles_missing_metadata(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection handles books with missing metadata."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
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

        api.fetch_book_info_list = AsyncMock(return_value=(mock_book_data, None))

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

    def test_import_collection_with_parent_identifiers_fetches_metadata_upfront(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection without parent identifiers fetches metadata upfront."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
            # No parent_identifier_set provided
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data - metadata should be included since fetch_metadata=True
        mock_book_data = [
            {
                "id": "overdrive-id-1",
                "metadata": {"title": "Test Book"},
                "availabilityV2": {"copiesOwned": 1},
            }
        ]

        api.fetch_book_info_list = AsyncMock(return_value=(mock_book_data, None))
        api.metadata_lookup = Mock(return_value={"title": "New Book"})
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
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify fetch_book_info_list was called with fetch_metadata=True
        api.fetch_book_info_list.assert_called_once()
        call_kwargs = api.fetch_book_info_list.call_args.kwargs
        assert call_kwargs["fetch_metadata"] is True
        assert call_kwargs["fetch_availability"] is True

        # Verify metadata_lookup was NOT called (metadata was fetched upfront)
        api.metadata_lookup.assert_not_called()

        assert result.processed_count == 1

    def test_import_collection_with_parent_identifiers_skips_metadata_for_known_identifiers(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test import_collection with parent identifiers skips fetching metadata for books in parent set."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()
        api = overdrive_api_fixture.api

        # Create a mock parent identifier set
        mock_parent_id1 = Mock()
        mock_parent_id1.identifier = "overdrive-id-1"
        mock_parent_id2 = Mock()
        mock_parent_id2.identifier = "overdrive-id-2"

        mock_parent_set = Mock(spec=IdentifierSet)
        mock_parent_set.get.return_value = [mock_parent_id1, mock_parent_id2]

        importer = OverdriveImporter(
            db=db.session,
            collection=collection,
            registry=registry,
            api=api,
            parent_identifier_set=mock_parent_set,
        )

        mock_apply_bib = Mock()
        mock_apply_circ = Mock()
        modified_since = datetime_utc(2023, 1, 1)

        # Mock book data - one book in parent set, one not
        # Metadata should NOT be included since fetch_metadata=False with parent set
        mock_book_data = [
            {
                "id": "overdrive-id-1",  # In parent set
                "metadata": None,  # No metadata fetched upfront
                "availabilityV2": {"copiesOwned": 1},
            },
            {
                "id": "overdrive-id-3",  # NOT in parent set
                "metadata": None,  # No metadata fetched upfront
                "availabilityV2": {"copiesOwned": 1},
            },
        ]

        api.fetch_book_info_list = AsyncMock(return_value=(mock_book_data, None))

        # Mock metadata_lookup to return metadata for the book not in parent set
        api.metadata_lookup = Mock(return_value={"title": "New Book"})

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
        result = importer.import_collection(
            apply_bibliographic=mock_apply_bib,
            apply_circulation=mock_apply_circ,
            modified_since=modified_since,
        )

        # Verify fetch_book_info_list was called with fetch_metadata=False
        api.fetch_book_info_list.assert_called_once()
        call_kwargs = api.fetch_book_info_list.call_args.kwargs
        assert call_kwargs["fetch_metadata"] is False
        assert call_kwargs["fetch_availability"] is True

        # Verify metadata_lookup was called once for the book NOT in parent set
        assert api.metadata_lookup.call_count == 1

        # Bibliographic should be applied once (only for the new book with metadata)
        assert mock_apply_bib.call_count == 1

        # Circulation should be applied for both books
        assert mock_apply_circ.call_count == 2

        assert result.processed_count == 2


class TestAllBooksOutOfScope:
    """Tests for the _all_books_out_of_scope method."""

    def test_all_books_out_of_scope_returns_true(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _all_books_out_of_scope returns True when all books are before modified_since."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        modified_since = datetime_utc(2023, 6, 1)
        book_data = [
            {"date_added": "2023-01-15T00:00:00Z"},
            {"date_added": "2023-02-10T00:00:00Z"},
            {"date_added": "2023-03-20T00:00:00Z"},
        ]

        result = importer._all_books_out_of_scope(modified_since, book_data)
        assert result is True

    def test_all_books_out_of_scope_returns_false_when_one_in_scope(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _all_books_out_of_scope returns False when at least one book is after modified_since."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        modified_since = datetime_utc(2023, 6, 1)
        book_data = [
            {"date_added": "2023-01-15T00:00:00Z"},  # Out of scope
            {"date_added": "2023-07-10T00:00:00Z"},  # In scope
            {"date_added": "2023-03-20T00:00:00Z"},  # Out of scope
        ]

        result = importer._all_books_out_of_scope(modified_since, book_data)
        assert result is False

    def test_all_books_out_of_scope_handles_missing_date_added(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _all_books_out_of_scope handles books without date_added field."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        modified_since = datetime_utc(2023, 6, 1)
        book_data = [
            {"date_added": "2023-01-15T00:00:00Z"},  # Out of scope
            {},  # No date_added - skipped
            {"date_added": "2023-03-20T00:00:00Z"},  # Out of scope
        ]

        # Should return False because not all books have date_added
        # (out_of_scope_count=2, len(book_data)=3)
        result = importer._all_books_out_of_scope(modified_since, book_data)
        assert result is False

    def test_all_books_out_of_scope_empty_list(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        services_fixture: ServicesFixture,
    ):
        """Test _all_books_out_of_scope with empty book list."""
        collection = overdrive_api_fixture.collection
        registry = services_fixture.services.integration_registry.license_providers()

        importer = OverdriveImporter(
            db=db.session, collection=collection, registry=registry
        )

        modified_since = datetime_utc(2023, 6, 1)
        book_data = []

        # Empty list: 0 == 0 should be True
        result = importer._all_books_out_of_scope(modified_since, book_data)
        assert result is True


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
