"""Tests for Overdrive Celery tasks."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, patch

import pytest

from palace.manager.celery.tasks import overdrive
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
)
from palace.manager.integration.license.overdrive.importer import (
    FeedImportResult,
    OverdriveImporter,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.constants import IdentifierType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.redis import RedisFixture


class OverdriveImportFixture:
    """Fixture for testing Overdrive import tasks."""

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        apply_fixture: ApplyTaskFixture,
    ):
        self.db = db
        self.collection = overdrive_api_fixture.collection
        self.api_fixture = overdrive_api_fixture
        self.apply = apply_fixture

    def run_import_task(
        self,
        collection: Collection | None = None,
        import_all: bool = False,
        apply: bool = False,
    ) -> None:
        """Run the import_collection task."""
        collection = collection if collection is not None else self.collection
        overdrive.import_collection.delay(collection.id, import_all=import_all).wait()
        if apply:
            self.apply.process_apply_queue()


@pytest.fixture
def overdrive_import_fixture(
    db: DatabaseTransactionFixture,
    overdrive_api_fixture: OverdriveAPIFixture,
    apply_task_fixture: ApplyTaskFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
) -> OverdriveImportFixture:
    return OverdriveImportFixture(
        db,
        overdrive_api_fixture,
        apply_task_fixture,
    )


class TestImportCollection:
    """Tests for the import_collection Celery task."""

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_basic(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test basic import_collection task execution."""
        collection = overdrive_import_fixture.collection

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        # Mock import result
        current_endpoint = BookInfoEndpoint(url="http://test.com/books")
        mock_result = FeedImportResult(
            current_page=current_endpoint, next_page=None, processed_count=5
        )
        mock_importer.import_collection.return_value = mock_result

        mock_importer_class.return_value = mock_importer

        # Run the task
        result = overdrive.import_collection.delay(collection.id).wait()

        # Verify importer was created with correct parameters
        mock_importer_class.assert_called_once()
        call_kwargs = mock_importer_class.call_args.kwargs
        assert call_kwargs["collection"].id == collection.id
        assert call_kwargs["identifier_set"] is not None

        # Verify import was executed
        mock_importer.import_collection.assert_called_once()

        # Verify timestamp was updated (since next_page is None)
        assert mock_timestamp.start is not None
        assert mock_timestamp.finish is not None
        assert mock_timestamp.finish > mock_timestamp.start

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_with_import_all(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_collection with import_all=True.

        When import_all=True, modified_since should be set to None,
        which bypasses the out-of-scope check in the importer.
        """
        collection = overdrive_import_fixture.collection

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com"),
            next_page=None,
            processed_count=10,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task with import_all=True
        overdrive.import_collection.delay(collection.id, import_all=True).wait()

        # Verify importer was created WITHOUT import_all parameter (removed)
        call_kwargs = mock_importer_class.call_args.kwargs
        assert "import_all" not in call_kwargs

        # Verify modified_since is None when import_all is True (bypasses out-of-scope check)
        import_call = mock_importer.import_collection.call_args
        assert import_call.kwargs["modified_since"] is None

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_with_next_page(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_collection replaces itself when there's a next page."""
        collection = overdrive_import_fixture.collection

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        # Mock result with next page
        next_endpoint = BookInfoEndpoint(url="http://test.com/books/page2")
        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com/books"),
            next_page=next_endpoint,
            processed_count=5,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Mock the task to capture the replace call
        with patch.object(overdrive.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("Task replaced")

            with pytest.raises(Exception, match="Task replaced"):
                overdrive.import_collection.delay(collection.id).wait()

            # Verify replace was called with next page URL
            mock_replace.assert_called_once()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_with_endpoint_not_none(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_collection with custom page URL."""
        collection = overdrive_import_fixture.collection
        endpoint_url = "http://custom.endpoint.com/books"

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url=endpoint_url),
            next_page=None,
            processed_count=3,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task with custom page
        overdrive.import_collection.delay(collection.id, page=endpoint_url).wait()

        # Verify the custom endpoint was used
        import_call = mock_importer.import_collection.call_args
        assert import_call.kwargs["endpoint"] == BookInfoEndpoint(url=endpoint_url)

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_modified_since(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_collection with custom modified_since datetime."""
        collection = overdrive_import_fixture.collection
        custom_modified = datetime_utc(2023, 6, 15, 10, 30)

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = datetime_utc(2023, 1, 1)
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com"),
            next_page=None,
            processed_count=7,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task with custom modified_since
        overdrive.import_collection.delay(
            collection.id, modified_since=custom_modified
        ).wait()

        # Verify modified_since was used
        import_call = mock_importer.import_collection.call_args
        assert import_call.kwargs["modified_since"] == custom_modified

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_identifier_set_tracking(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
    ):
        """Test that import_collection tracks identifiers in Redis set."""
        collection = overdrive_import_fixture.collection

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com"),
            next_page=None,
            processed_count=5,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task
        result = overdrive.import_collection.delay(
            collection.id, return_identifiers=True
        ).wait()

        # Verify identifier_set was created and passed to importer
        call_kwargs = mock_importer_class.call_args.kwargs
        assert call_kwargs["identifier_set"] is not None
        assert isinstance(call_kwargs["identifier_set"], IdentifierSet)

        # Verify result is the identifier set info object
        assert result["key"]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_no_identifier_tracking(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_collection with return_identifiers=False."""
        collection = overdrive_import_fixture.collection

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com"),
            next_page=None,
            processed_count=3,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task without identifier tracking
        result = overdrive.import_collection.delay(
            collection.id, return_identifiers=False
        ).wait()

        # Verify identifier_set was NOT created
        call_kwargs = mock_importer_class.call_args.kwargs
        assert call_kwargs["identifier_set"] is None

        # Result should be None when not tracking identifiers
        assert result is None


class TestImportCollectionGroup:
    """Tests for the import_collection_group Celery task."""

    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_import_collection_group_basic(
        self,
        mock_cleanup_chord: MagicMock,
        mock_import_collection: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group chains parent and children import."""
        collection = overdrive_import_fixture.collection

        # Mock the task signatures
        mock_import_sig = Mock()
        mock_import_sig.apply_async = Mock()
        mock_import_collection.s.return_value = mock_import_sig

        mock_cleanup_sig = Mock()
        mock_cleanup_chord.s.return_value = mock_cleanup_sig

        # Run the task
        overdrive.import_collection_group.delay(collection.id).wait()

        # Verify import_collection task signature was created
        mock_import_collection.s.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
            page=None,
            parent_identifiers=None,
            return_identifiers=True,
            modified_since=None,
            start_time=None,
        )

        # Verify it was applied with link to cleanup chord
        mock_import_sig.apply_async.assert_called_once()
        call_kwargs = mock_import_sig.apply_async.call_args.kwargs
        assert "link" in call_kwargs

    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_import_collection_group_with_import_all(
        self,
        mock_cleanup_chord: MagicMock,
        mock_import_collection: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group with import_all flag."""
        collection = overdrive_import_fixture.collection

        # Mock the task signatures
        mock_import_sig = Mock()
        mock_import_sig.apply_async = Mock()
        mock_import_collection.s.return_value = mock_import_sig

        mock_cleanup_sig = Mock()
        mock_cleanup_chord.s.return_value = mock_cleanup_sig

        # Run the task with import_all=True
        overdrive.import_collection_group.delay(collection.id, import_all=True).wait()

        # Verify import_all was passed through
        call_args = mock_import_collection.s.call_args.kwargs
        assert call_args["import_all"] is True

    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_import_collection_group_with_custom_dates(
        self,
        mock_cleanup_chord: MagicMock,
        mock_import_collection: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group with custom modified_since and start_time."""
        collection = overdrive_import_fixture.collection
        modified_since = datetime_utc(2023, 1, 1)
        start_time = datetime_utc(2023, 6, 1)

        # Mock the task signatures
        mock_import_sig = Mock()
        mock_import_sig.apply_async = Mock()
        mock_import_collection.s.return_value = mock_import_sig

        mock_cleanup_sig = Mock()
        mock_cleanup_chord.s.return_value = mock_cleanup_sig

        # Run the task with custom dates
        overdrive.import_collection_group.delay(
            collection.id, modified_since=modified_since, start_time=start_time
        ).wait()

        # Verify dates were passed through
        call_args = mock_import_collection.s.call_args.kwargs
        assert call_args["modified_since"] == modified_since
        assert call_args["start_time"] == start_time


class TestRehydrateIdentifierSet:
    """Tests for the rehydrate_identifier_set helper function."""

    def test_rehydrate_identifier_set(
        self, celery_fixture: CeleryFixture, redis_fixture: RedisFixture
    ):
        """Test rehydrating an IdentifierSet from dict."""
        # Create a mock task with services
        mock_task = Mock()
        mock_task.services.redis().client.return_value = redis_fixture.client

        # Create identifier set info
        identifier_set_info = {"key": ["test", "key", "parts"]}

        # Rehydrate the set
        result = overdrive.rehydrate_identifier_set(mock_task, identifier_set_info)

        # Verify it returns an IdentifierSet
        assert isinstance(result, IdentifierSet)
        assert result._supplied_key == ["test", "key", "parts"]


class TestImportChildrenAndCleanupChord:
    """Tests for the import_children_and_cleanup_chord Celery task."""

    @patch("palace.manager.celery.tasks.overdrive.chord")
    @patch("palace.manager.celery.tasks.overdrive.group")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.remove_identifier_set")
    @patch("palace.manager.celery.tasks.overdrive.rehydrate_identifier_set")
    def test_import_children_and_cleanup_chord_with_children(
        self,
        mock_rehydrate: MagicMock,
        mock_remove: MagicMock,
        mock_import: MagicMock,
        mock_group: MagicMock,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_children_and_cleanup_chord with child collections."""
        # Create parent and child collections
        parent_collection = overdrive_import_fixture.collection
        child1 = db.collection(
            name="Child 1",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="child1"),
        )
        child1.parent = parent_collection

        child2 = db.collection(
            name="Child 2",
            protocol=OverdriveAPI,
            settings=db.overdrive_settings(external_account_id="child2"),
        )
        child2.parent = parent_collection

        # Mock identifier set
        mock_identifier_set = Mock(spec=IdentifierSet)
        mock_rehydrate.return_value = mock_identifier_set

        # Mock task signatures
        mock_import.si.return_value = Mock()
        mock_remove.si.return_value = Mock()

        # Mock chord and group
        mock_group_result = Mock()
        mock_group.return_value = mock_group_result

        mock_chord_result = Mock()
        mock_chord_result.id = "test-chord-id"
        mock_async_result = Mock()
        mock_async_result.id = "test-chord-id"
        mock_chord_result.apply_async.return_value = mock_async_result
        mock_chord.return_value = mock_chord_result

        # Run the task
        identifier_set_info = {"key": ["test", "key"]}
        modified_since = datetime_utc(2023, 1, 1)

        result = overdrive.import_children_and_cleanup_chord.delay(
            identifier_set_info=identifier_set_info,
            collection_id=parent_collection.id,
            import_all=False,
            modified_since=modified_since,
        ).wait()

        # Verify group was created with import tasks for each child
        assert mock_import.si.call_count == 2

        # Verify chord was created
        mock_chord.assert_called_once()

        # Verify result contains chord_id
        assert result["chord_id"] == "test-chord-id"

    @patch("palace.manager.celery.tasks.overdrive.chord")
    @patch("palace.manager.celery.tasks.overdrive.group")
    @patch("palace.manager.celery.tasks.overdrive.rehydrate_identifier_set")
    def test_import_children_and_cleanup_chord_no_children(
        self,
        mock_rehydrate: MagicMock,
        mock_group: MagicMock,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test import_children_and_cleanup_chord with no child collections."""
        collection = overdrive_import_fixture.collection

        # Mock identifier set
        mock_identifier_set = Mock(spec=IdentifierSet)
        mock_rehydrate.return_value = mock_identifier_set

        # Mock chord
        mock_chord_result = Mock()
        mock_async_result = Mock()
        mock_async_result.id = "test-chord-id"
        mock_chord_result.apply_async.return_value = mock_async_result
        mock_chord.return_value = mock_chord_result

        # Run the task
        identifier_set_info = {"key": ["test", "key"]}
        modified_since = datetime_utc(2023, 1, 1)

        result = overdrive.import_children_and_cleanup_chord.delay(
            identifier_set_info=identifier_set_info,
            collection_id=collection.id,
            import_all=False,
            modified_since=modified_since,
        ).wait()

        # Verify group was created with empty list (no children)
        mock_group.assert_called_once()
        call_args = mock_group.call_args[0][0]
        assert len(call_args) == 0

        # Chord should still be created for cleanup
        mock_chord.assert_called_once()


class TestRemoveIdentifierSet:
    """Tests for the remove_identifier_set Celery task."""

    @patch("palace.manager.celery.tasks.overdrive.rehydrate_identifier_set")
    def test_remove_identifier_set_success(
        self,
        mock_rehydrate: MagicMock,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ):
        """Test successful removal of identifier set."""
        # Create a real identifier set in Redis
        identifier_set_info = {"key": ["test", "cleanup", "key"]}
        identifier_set = IdentifierSet(redis_fixture.client, identifier_set_info["key"])

        # Add some data to make it exist
        identifier = IdentifierData(
            identifier="test-id", type=IdentifierType.OVERDRIVE_ID
        )
        identifier_set.add(identifier)
        assert identifier_set.exists()

        # Mock rehydrate to return the real set
        mock_rehydrate.return_value = identifier_set

        # Run the task
        overdrive.remove_identifier_set.delay(
            identifier_set_info=identifier_set_info
        ).wait()

        # Verify the set was deleted
        assert not identifier_set.exists()

    @patch("palace.manager.celery.tasks.overdrive.rehydrate_identifier_set")
    def test_remove_identifier_set_assertion_error(
        self,
        mock_rehydrate: MagicMock,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ):
        """Test that remove_identifier_set raises AssertionError if set doesn't exist."""
        # Create an identifier set that doesn't exist
        identifier_set_info = {"key": ["test", "nonexistent", "key"]}
        identifier_set = IdentifierSet(redis_fixture.client, identifier_set_info["key"])
        assert not identifier_set.exists()

        # Mock rehydrate to return the non-existent set
        mock_rehydrate.return_value = identifier_set

        # Run the task - should raise AssertionError
        with pytest.raises(AssertionError):
            overdrive.remove_identifier_set.delay(
                identifier_set_info=identifier_set_info
            ).wait()


class TestIntegration:
    """Integration tests for Overdrive import tasks."""

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_full_import_workflow_single_page(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
    ):
        """Test complete import workflow with single page."""
        collection = overdrive_import_fixture.collection

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_timestamp.elapsed = "10 seconds"
        mock_importer.get_timestamp.return_value = mock_timestamp

        # Single page result
        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com/books"),
            next_page=None,
            processed_count=50,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task
        result = overdrive.import_collection.delay(collection.id).wait()

        # Verify timestamp was finalized
        assert mock_timestamp.start is not None
        assert mock_timestamp.finish is not None

        # Verify result is identifier set info
        assert result["key"]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_full_import_workflow_with_parent_identifiers(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
    ):
        """Test import with parent identifiers provided."""
        collection = overdrive_import_fixture.collection

        # Create a parent identifier set
        parent_set = IdentifierSet(redis_fixture.client, ["parent", "ids"])
        identifier = IdentifierData(
            identifier="test-id", type=IdentifierType.OVERDRIVE_ID
        )
        parent_set.add(identifier)
        assert parent_set.exists()

        # Mock the importer
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com"),
            next_page=None,
            processed_count=25,
        )
        mock_importer.import_collection.return_value = mock_result
        mock_importer_class.return_value = mock_importer

        # Run the task with parent identifiers
        overdrive.import_collection.delay(
            collection.id, parent_identifiers=parent_set
        ).wait()

        # Verify parent_identifier_set was created and passed to importer
        call_kwargs = mock_importer_class.call_args.kwargs
        assert call_kwargs["parent_identifier_set"] is not None
        assert isinstance(call_kwargs["parent_identifier_set"], IdentifierSet)
        assert call_kwargs["parent_identifier_set"]._key == parent_set._key
