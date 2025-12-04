"""Tests for Overdrive Celery tasks."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, Mock, call, patch

import pytest

from palace.manager.celery.tasks import overdrive
from palace.manager.celery.tasks.overdrive import import_collection_group
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
)
from palace.manager.integration.license.overdrive.importer import (
    FeedImportResult,
    OverdriveImporter,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.constants import IdentifierType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import datetime_utc
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.redis import RedisFixture
from tests.mocks.overdrive import MockOverdriveAPI


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
        """Run the import_collection task.

        :param collection: Collection to import (defaults to self.collection)
        :param import_all: Whether to import all books
        :param apply: Whether to process the apply queue after import
        """
        collection = collection if collection is not None else self.collection
        overdrive.import_collection.delay(collection.id, import_all=import_all).wait()
        if apply:
            self.apply.process_apply_queue()

    @staticmethod
    def create_mock_importer(
        next_page: BookInfoEndpoint | None = None, processed_count: int = 5
    ) -> tuple[Mock, Mock]:
        """Create a mock importer with standard setup.

        :param next_page: Next page endpoint (None means last page)
        :param processed_count: Number of items processed
        :return: Tuple of (mock_importer, mock_timestamp)
        """
        mock_importer = Mock(spec=OverdriveImporter)
        mock_timestamp = Mock(spec=Timestamp)
        mock_timestamp.start = None
        mock_timestamp.elapsed = "10 seconds"
        mock_importer.get_timestamp.return_value = mock_timestamp

        mock_result = FeedImportResult(
            current_page=BookInfoEndpoint(url="http://test.com/books"),
            next_page=next_page,
            processed_count=processed_count,
        )
        mock_importer.import_collection.return_value = mock_result
        return mock_importer, mock_timestamp


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
    ):
        """Test basic import_collection task execution."""
        collection = overdrive_import_fixture.collection

        # Create mock importer with standard setup
        mock_importer, mock_timestamp = overdrive_import_fixture.create_mock_importer()
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
    ):
        """Test import_collection with import_all=True.

        When import_all=True, modified_since should be set to None,
        which bypasses the out-of-scope check in the importer.
        """
        collection = overdrive_import_fixture.collection

        # Create mock importer
        mock_importer, _ = overdrive_import_fixture.create_mock_importer()
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
    ):
        """Test import_collection replaces itself when there's a next page."""
        collection = overdrive_import_fixture.collection

        # Create mock importer with next page
        next_endpoint = BookInfoEndpoint(url="http://test.com/books/page2")
        mock_importer, _ = overdrive_import_fixture.create_mock_importer(
            next_page=next_endpoint
        )
        mock_importer_class.return_value = mock_importer

        # Mock the task to capture the replace call
        with patch.object(overdrive.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("Task replaced")

            with pytest.raises(Exception, match="Task replaced"):
                overdrive.import_collection.delay(collection.id).wait()

            # Verify replace was called with next page URL
            mock_replace.assert_called_once()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_with_next_page_and_parent_identifiers(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """Test import_collection serializes parent_identifiers when replacing task with next page."""
        collection = overdrive_import_fixture.collection

        # Create a parent identifier set
        parent_set = IdentifierSet(redis_fixture.client, ["parent", "test", "key"])
        identifier = IdentifierData(
            identifier="parent-id", type=IdentifierType.OVERDRIVE_ID
        )
        parent_set.add(identifier)
        assert parent_set.exists()

        # Create mock importer with next page
        next_endpoint = BookInfoEndpoint(url="http://test.com/books/page2")
        mock_importer, _ = overdrive_import_fixture.create_mock_importer(
            next_page=next_endpoint
        )
        mock_importer_class.return_value = mock_importer

        # Mock the task to capture the replace call
        with patch.object(overdrive.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("Task replaced")

            with pytest.raises(Exception, match="Task replaced"):
                overdrive.import_collection.delay(
                    collection.id, parent_identifiers=parent_set
                ).wait()

            # Verify replace was called
            mock_replace.assert_called_once()

            # Get the signature passed to replace
            replace_sig = mock_replace.call_args[0][0]
            replace_kwargs = replace_sig.kwargs

            # Verify parent_identifiers was serialized to dict format
            assert replace_kwargs["parent_identifiers"] is not None
            assert isinstance(replace_kwargs["parent_identifiers"], dict)
            assert "key" in replace_kwargs["parent_identifiers"]
            assert replace_kwargs["parent_identifiers"]["key"] == [
                "parent",
                "test",
                "key",
            ]
            assert "expire_time" in replace_kwargs["parent_identifiers"]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_with_endpoint_not_none(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection with custom page URL."""
        collection = overdrive_import_fixture.collection
        endpoint_url = "http://custom.endpoint.com/books"

        # Create mock importer
        mock_importer, _ = overdrive_import_fixture.create_mock_importer(
            processed_count=3
        )
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
    ):
        """Test import_collection with custom modified_since datetime."""
        collection = overdrive_import_fixture.collection
        custom_modified = datetime_utc(2023, 6, 15, 10, 30)

        # Create mock importer
        mock_importer, mock_timestamp = overdrive_import_fixture.create_mock_importer(
            processed_count=7
        )
        mock_timestamp.start = datetime_utc(2023, 1, 1)
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
    ):
        """Test that import_collection tracks identifiers in Redis set."""
        collection = overdrive_import_fixture.collection

        # Create mock importer
        mock_importer, _ = overdrive_import_fixture.create_mock_importer()
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
    def test_import_collection_with_parent_identifiers_dict(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """Test that import_collection properly rehydrates parent_identifiers from dict."""
        collection = overdrive_import_fixture.collection

        # Create a real parent identifier set in Redis
        parent_key = ["parent", "dict", "test"]
        parent_set = IdentifierSet(redis_fixture.client, parent_key)
        identifier = IdentifierData(
            identifier="parent-dict-id", type=IdentifierType.OVERDRIVE_ID
        )
        parent_set.add(identifier)
        assert parent_set.exists()

        # Serialize it as if coming from a previous task
        parent_identifiers_dict = parent_set.__json__()

        # Create mock importer
        mock_importer, _ = overdrive_import_fixture.create_mock_importer()
        mock_importer_class.return_value = mock_importer

        # Run the task with serialized parent_identifiers
        overdrive.import_collection.delay(
            collection.id, parent_identifiers=parent_identifiers_dict
        ).wait()

        # Verify parent_identifier_set was rehydrated and passed to importer
        call_kwargs = mock_importer_class.call_args.kwargs
        assert call_kwargs["parent_identifier_set"] is not None
        assert isinstance(call_kwargs["parent_identifier_set"], IdentifierSet)

        # Verify it's the same identifier set by checking the key
        assert call_kwargs["parent_identifier_set"]._supplied_key == parent_key

        # Verify the data is accessible
        assert identifier in call_kwargs["parent_identifier_set"]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_no_identifier_tracking(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection with return_identifiers=False."""
        collection = overdrive_import_fixture.collection

        # Create mock importer
        mock_importer, _ = overdrive_import_fixture.create_mock_importer(
            processed_count=3
        )
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

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_import_collection_marked_for_deletion(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """Test import_collection skips import when collection is marked for deletion."""
        collection = overdrive_import_fixture.collection
        collection.marked_for_deletion = True

        # Set up logging capture
        caplog.set_level(LogLevel.warning)

        # Run the task
        result = overdrive.import_collection.delay(
            collection.id, return_identifiers=False
        ).wait()

        # Verify result is None
        assert result is None

        # Verify warning log message was logged
        assert "This collection is marked for deletion" in caplog.text
        assert f"Skipping import of '{collection.name}'" in caplog.text

        # Verify importer was NOT created or called
        mock_importer_class.assert_not_called()


class TestImportCollectionGroup:
    """Tests for the import_collection_group Celery task."""

    @staticmethod
    def setup_chain_mocks(
        mock_import_collection: MagicMock,
        mock_cleanup_chord: MagicMock,
        mock_chain: MagicMock,
    ) -> Mock:
        """Set up mock chain and task signatures for testing.

        :param mock_import_collection: Mock for import_collection task
        :param mock_cleanup_chord: Mock for cleanup chord task
        :param mock_chain: Mock for chain function
        :return: Mock chain result
        """
        mock_import_sig = Mock()
        mock_import_collection.s.return_value = mock_import_sig

        mock_cleanup_sig = Mock()
        mock_cleanup_chord.s.return_value = mock_cleanup_sig

        mock_chain_result = Mock()
        mock_chain.return_value = mock_chain_result

        return mock_chain_result

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_import_collection_group_basic(
        self,
        mock_cleanup_chord: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group chains parent and children import."""
        collection = overdrive_import_fixture.collection

        # Set up chain mocks
        mock_chain_result = self.setup_chain_mocks(
            mock_import_collection, mock_cleanup_chord, mock_chain
        )

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

        # Verify cleanup chord signature was created
        mock_cleanup_chord.s.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
            modified_since=None,
        )

        # Verify chain was created and called
        assert mock_chain.call_count == 1
        mock_chain_result.assert_called_once()

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_import_collection_group_with_import_all(
        self,
        mock_cleanup_chord: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group with import_all flag."""
        collection = overdrive_import_fixture.collection

        # Set up chain mocks
        self.setup_chain_mocks(mock_import_collection, mock_cleanup_chord, mock_chain)

        # Run the task with import_all=True
        overdrive.import_collection_group.delay(collection.id, import_all=True).wait()

        # Verify import_all was passed through
        call_args = mock_import_collection.s.call_args.kwargs
        assert call_args["import_all"] is True

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_import_collection_group_with_custom_dates(
        self,
        mock_cleanup_chord: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group with custom modified_since and start_time."""
        collection = overdrive_import_fixture.collection
        modified_since = datetime_utc(2023, 1, 1)
        start_time = datetime_utc(2023, 6, 1)

        # Set up chain mocks
        self.setup_chain_mocks(mock_import_collection, mock_cleanup_chord, mock_chain)

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

    @staticmethod
    def setup_chord_mocks(
        mock_group: MagicMock, mock_chord: MagicMock
    ) -> tuple[Mock, str]:
        """Set up chord and group mocks.

        :param mock_group: Mock for group function
        :param mock_chord: Mock for chord function
        :return: Tuple of (mock_chord_result, chord_id)
        """
        mock_group_result = Mock()
        mock_group.return_value = mock_group_result

        chord_id = "test-chord-id"
        mock_chord_result = Mock()
        mock_async_result = Mock()
        mock_async_result.id = chord_id
        mock_chord_result.apply_async.return_value = mock_async_result
        mock_chord.return_value = mock_chord_result

        return mock_chord_result, chord_id

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

        # Set up chord and group mocks
        _, chord_id = self.setup_chord_mocks(mock_group, mock_chord)

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
        assert result["chord_id"] == chord_id

    @patch("palace.manager.celery.tasks.overdrive.chord")
    @patch("palace.manager.celery.tasks.overdrive.group")
    @patch("palace.manager.celery.tasks.overdrive.rehydrate_identifier_set")
    def test_import_children_and_cleanup_chord_no_children(
        self,
        mock_rehydrate: MagicMock,
        mock_group: MagicMock,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_children_and_cleanup_chord with no child collections."""
        collection = overdrive_import_fixture.collection

        # Mock identifier set
        mock_identifier_set = Mock(spec=IdentifierSet)
        mock_rehydrate.return_value = mock_identifier_set

        # Set up chord mocks
        self.setup_chord_mocks(mock_group, mock_chord)

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
    def test_remove_identifier_set_nonexistent_set(
        self,
        mock_rehydrate: MagicMock,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ):
        """Test that remove_identifier_set logs warning and skips cleanup if set doesn't exist."""
        # Create an identifier set that doesn't exist
        identifier_set_info = {"key": ["test", "nonexistent", "key"]}
        identifier_set = IdentifierSet(redis_fixture.client, identifier_set_info["key"])
        assert not identifier_set.exists()

        # Mock rehydrate to return the non-existent set
        mock_rehydrate.return_value = identifier_set

        # Run the task - should complete without error and log a warning
        overdrive.remove_identifier_set.delay(
            identifier_set_info=identifier_set_info
        ).wait()

        # Verify the set still doesn't exist (no error was raised)
        assert not identifier_set.exists()


class TestImportAllCollections:
    def test_import_all_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        import_all = True
        caplog.set_level(LogLevel.info)
        decoy_collection = db.default_collection()
        collection1 = db.collection(protocol=OverdriveAPI)
        collection2 = db.collection(protocol=OverdriveAPI)
        child_collection = db.collection(protocol=OverdriveAPI)
        child_collection.parent = collection1

        with patch.object(
            overdrive, "import_collection_group"
        ) as import_collection_group:
            overdrive.import_all_collections.delay(import_all=import_all).wait()

        import_collection_group.s.assert_called_once_with(import_all=import_all)
        import_collection_group.s.return_value.delay.assert_has_calls(
            [call(collection_id=collection1.id), call(collection_id=collection2.id)],
            any_order=True,
        )
        assert "Queued 2 collections for import." in caplog.text


class TestIntegration:
    """Integration tests for Overdrive import tasks."""

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_full_import_workflow_single_page(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test complete import workflow with single page."""
        collection = overdrive_import_fixture.collection

        # Create mock importer
        mock_importer, mock_timestamp = overdrive_import_fixture.create_mock_importer(
            processed_count=50
        )
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

        # Create mock importer
        mock_importer, _ = overdrive_import_fixture.create_mock_importer(
            processed_count=25
        )
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

    @patch(
        target="palace.manager.integration.license.overdrive.importer.OverdriveAPI",
        new=MockOverdriveAPI,
    )
    def test_full_import_flow_with_parent_identifiers_and_overdrive_data(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ):
        """Test import with parent identifiers provided and Overdrive data."""
        collection = overdrive_api_fixture.collection
        availability_data, availability_json = overdrive_api_fixture.sample_json(
            "overdrive_availability_information.json"
        )
        metadata_data, metadata_json = overdrive_api_fixture.sample_json(
            "bibliographic_information_book_list_test.json"
        )
        (
            overdrive_book_list_with_next_link_data,
            overdrive_book_list_with_next_link_json,
        ) = overdrive_api_fixture.sample_json("overdrive_book_list_with_next_link.json")

        book = overdrive_book_list_with_next_link_json["products"][0]

        (
            overdrive_book_list_last_page_no_products_data,
            overdrive_book_list_last_page_no_products_json,
        ) = overdrive_api_fixture.sample_json(
            "overdrive_book_list_last_page_no_products.json"
        )
        mock_async_client = overdrive_api_fixture.mock_async_client
        mock_async_client.queue_response(
            200, content=overdrive_book_list_with_next_link_data
        )

        mock_async_client.queue_response(200, content=metadata_data)
        mock_async_client.queue_response(200, content=availability_data)

        mock_async_client.queue_response(
            200, content=overdrive_book_list_last_page_no_products_data
        )

        # sanity check that the identifier does not exist
        assert not self._get_identifier(book, overdrive_api_fixture)

        with patch(
            "palace.manager.service.integration_registry.license_providers.LicenseProvidersRegistry.equivalent"
        ) as equivalent:
            equivalent.return_value = True

            import_collection_group.delay(
                collection_id=collection.id, import_all=True
            ).wait()

            # wait a second before proceeding.  For reasons that aren't clear to me,
            # this delay seems to be necessary to keep the test from flapping.
            time.sleep(1)
            # verify that the identifier is now in the database.
            assert self._get_identifier(book, overdrive_api_fixture)

    def _get_identifier(self, book, overdrive_api_fixture):
        identifier, _ = Identifier.for_foreign_id(
            overdrive_api_fixture.db.session,
            foreign_id=book["id"],
            foreign_identifier_type=Identifier.OVERDRIVE_ID,
            autocreate=False,
        )
        return identifier
