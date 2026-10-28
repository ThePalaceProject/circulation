"""Tests for Overdrive Celery tasks."""

from __future__ import annotations

import datetime
import json
from unittest.mock import MagicMock, Mock, call, patch
from uuid import uuid4

import pytest
from celery.result import AsyncResult
from sqlalchemy.orm import Session

from palace.util.datetime_helpers import datetime_utc, utc_now
from palace.util.log import LogLevel

from palace.manager.celery.importer import import_workflow_lock, reap_workflow_lock
from palace.manager.celery.tasks import overdrive
from palace.manager.celery.tasks.overdrive import import_collection_group
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.integration.license.overdrive.importer import (
    FeedImportResult,
    OverdriveImporter,
)
from palace.manager.integration.license.overdrive.requests import BookInfoEndpoint
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.constants import IdentifierType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.files import OverdriveFilesFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.redis import RedisFixture
from tests.mocks.mock import MockRequestsResponse


class SeededOverdriveAPI(OverdriveAPI):
    """OverdriveAPI with pre-seeded token caches.

    Stands in for OverdriveAPI where a test patches the class itself, so every
    instance the code under test creates skips the token and library document
    requests.
    """

    def __init__(self, _db: Session, collection: Collection) -> None:
        super().__init__(_db, collection)
        OverdriveAPIFixture.seed_token_caches(self)


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
    def test_workflow_lock_blocks_duplicate_import(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """When the workflow lock is held, a new import skips without running the importer."""
        collection = overdrive_import_fixture.collection
        lock_value = str(uuid4())
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, lock_value
        )
        workflow_lock.acquire()

        result = overdrive.import_collection.delay(collection.id).wait()

        mock_importer_class.assert_not_called()
        assert result == overdrive._import_skipped_payload()
        workflow_lock.release()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_workflow_lock_released_on_final_page(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """After a single-page import completes, the workflow lock is released."""
        collection = overdrive_import_fixture.collection

        mock_importer, _ = overdrive_import_fixture.create_mock_importer()
        mock_importer_class.return_value = mock_importer

        overdrive.import_collection.delay(collection.id).wait()

        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert not workflow_lock.locked()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_workflow_lock_released_on_marked_for_deletion(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """When collection is marked for deletion, the workflow lock is still released on exit."""
        collection = overdrive_import_fixture.collection
        collection.marked_for_deletion = True

        overdrive.import_collection.delay(
            collection.id, return_identifiers=False
        ).wait()

        mock_importer_class.assert_not_called()
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert not workflow_lock.locked()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_replace_raised_with_next_page(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """When more pages remain, task.replace() is raised with the next page. The
        workflow lock is keyed on the task id (which replace preserves), so nothing
        needs to be threaded through the signature's kwargs."""
        collection = overdrive_import_fixture.collection

        next_endpoint = BookInfoEndpoint(url="http://test.com/books/page2")
        mock_importer, _ = overdrive_import_fixture.create_mock_importer(
            next_page=next_endpoint
        )
        mock_importer_class.return_value = mock_importer

        with patch.object(overdrive.import_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("Task replaced")

            with pytest.raises(Exception, match="Task replaced"):
                overdrive.import_collection.delay(collection.id).wait()

            replace_sig = mock_replace.call_args[0][0]
            assert replace_sig.kwargs["page"] == "http://test.com/books/page2"
            assert "lock_value" not in replace_sig.kwargs

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_workflow_lock_continuation_reacquires_own_lock(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """A continuation page carries the same task id, so it re-acquires the workflow
        lock it already holds and runs the importer."""
        collection = overdrive_import_fixture.collection
        # Simulate the lock still held by this run's own task id from a prior page.
        task_id = str(uuid4())
        import_workflow_lock(redis_fixture.client, collection.id, task_id).acquire()

        mock_importer, _ = overdrive_import_fixture.create_mock_importer()
        mock_importer_class.return_value = mock_importer

        result = overdrive.import_collection.apply_async(
            args=(collection.id,),
            kwargs={"page": "http://test.com/page1"},
            task_id=task_id,
        ).wait()

        mock_importer_class.assert_called_once()
        assert result is not None

    @patch("palace.manager.celery.tasks.overdrive.OverdriveImporter")
    def test_workflow_lock_not_released_on_autoretry(
        self,
        mock_importer_class: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
        celery_fixture: CeleryFixture,
    ):
        """A retryable failure holds the workflow lock and each retry re-runs the import.

        The workflow lock is keyed on ``task.request.id``, which Celery preserves across
        retries, so every retry re-acquires the same workflow lock and re-runs the import,
        rather than skipping as if another run were in progress. The lock stays held the
        whole time so no concurrent run can start.
        """
        collection = overdrive_import_fixture.collection
        mock_importer, _ = overdrive_import_fixture.create_mock_importer()
        mock_response = MockRequestsResponse(500, content="Internal Server Error")
        mock_importer.import_collection.side_effect = BadResponseException(
            "http://test.com", "Bad response", mock_response
        )
        mock_importer_class.return_value = mock_importer

        with celery_fixture.patch_retry_backoff():
            overdrive.import_collection.delay(collection.id).get(propagate=False)

        # The import was re-run on every retry (1 initial attempt + max_retries=4),
        # not skipped as an "already in progress" run.
        assert mock_importer.import_collection.call_count == 5
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

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
        mock_router: MagicMock,
        mock_chain: MagicMock,
    ) -> Mock:
        """Set up mock chain and task signatures for testing.

        :param mock_import_collection: Mock for import_collection task
        :param mock_router: Mock for import_result_router task
        :param mock_chain: Mock for chain function
        :return: Mock chain result
        """
        mock_import_sig = Mock()
        mock_import_collection.s.return_value = mock_import_sig

        mock_router_sig = Mock()
        mock_router.s.return_value = mock_router_sig

        # Mock the async result returned when the chain is called
        mock_async_result = Mock()
        mock_async_result.id = "test-chain-id"

        mock_chain_result = Mock()
        mock_chain_result.return_value = mock_async_result
        mock_chain.return_value = mock_chain_result

        return mock_chain_result

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_result_router")
    def test_import_collection_group_basic(
        self,
        mock_router: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group chains parent import and router."""
        collection = overdrive_import_fixture.collection

        # Set up chain mocks
        mock_chain_result = self.setup_chain_mocks(
            mock_import_collection, mock_router, mock_chain
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

        # Verify router signature was created
        mock_router.s.assert_called_once_with(
            collection_id=collection.id,
            import_all=False,
            modified_since=None,
        )

        # Verify chain was created and called
        assert mock_chain.call_count == 1
        mock_chain_result.assert_called_once()

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_result_router")
    def test_import_collection_group_with_import_all(
        self,
        mock_router: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group with import_all flag."""
        collection = overdrive_import_fixture.collection

        # Set up chain mocks
        self.setup_chain_mocks(mock_import_collection, mock_router, mock_chain)

        # Run the task with import_all=True
        overdrive.import_collection_group.delay(collection.id, import_all=True).wait()

        # Verify import_all was passed through
        call_args = mock_import_collection.s.call_args.kwargs
        assert call_args["import_all"] is True

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_result_router")
    def test_import_collection_group_with_custom_dates(
        self,
        mock_router: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
    ):
        """Test import_collection_group with custom modified_since and start_time."""
        collection = overdrive_import_fixture.collection
        modified_since = datetime_utc(2023, 1, 1)
        start_time = datetime_utc(2023, 6, 1)

        # Set up chain mocks
        self.setup_chain_mocks(mock_import_collection, mock_router, mock_chain)

        # Run the task with custom dates
        overdrive.import_collection_group.delay(
            collection.id, modified_since=modified_since, start_time=start_time
        ).wait()

        # Verify dates were passed through
        call_args = mock_import_collection.s.call_args.kwargs
        assert call_args["modified_since"] == modified_since
        assert call_args["start_time"] == start_time

    @patch("palace.manager.celery.tasks.overdrive.chain")
    @patch("palace.manager.celery.tasks.overdrive.import_collection")
    @patch("palace.manager.celery.tasks.overdrive.import_result_router")
    def test_import_collection_group_skips_when_lock_held(
        self,
        mock_router: MagicMock,
        mock_import_collection: MagicMock,
        mock_chain: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        redis_fixture: RedisFixture,
    ):
        """When the workflow lock is held, import_collection_group skips chain creation."""
        collection = overdrive_import_fixture.collection
        lock_value = str(uuid4())
        workflow_lock = import_workflow_lock(
            redis_fixture.client, collection.id, lock_value
        )
        workflow_lock.acquire()

        result = overdrive.import_collection_group.delay(collection.id).wait()

        mock_chain.assert_not_called()
        assert result == overdrive._import_skipped_payload()
        workflow_lock.release()


class TestImportResultRouter:
    """Tests for the import_result_router Celery task."""

    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_router_short_circuits_when_skipped(
        self,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        celery_fixture: CeleryFixture,
    ):
        """When import_result has import_skipped, router returns early without invoking chord."""
        collection = overdrive_import_fixture.collection
        import_result = overdrive._import_skipped_payload()

        result = overdrive.import_result_router.delay(
            import_result=import_result,
            collection_id=collection.id,
            import_all=False,
            modified_since=None,
        ).wait()

        mock_chord.apply_async.assert_not_called()
        assert result == overdrive._import_skipped_payload()

    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_router_invokes_chord_when_import_completed(
        self,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ):
        """When import_result is valid identifier set info, router invokes the chord."""
        collection = overdrive_import_fixture.collection
        identifier_set_info = {"key": ["test", "key"], "expire_time": 43200}
        mock_async_result = Mock()
        mock_async_result.id = "chord-id"
        mock_chord.apply_async.return_value = mock_async_result

        result = overdrive.import_result_router.delay(
            import_result=identifier_set_info,
            collection_id=collection.id,
            import_all=False,
            modified_since=datetime_utc(2023, 1, 1),
        ).wait()

        mock_chord.apply_async.assert_called_once()
        call_args = mock_chord.apply_async.call_args.kwargs["args"]
        assert call_args[0] == identifier_set_info
        assert call_args[1] == collection.id
        assert call_args[2] is False
        assert call_args[3] == datetime_utc(2023, 1, 1)
        assert result == {"chord_id": "chord-id"}

    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_router_serializes_identifier_set_before_invoking_chord(
        self,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
    ):
        """When import_result is an IdentifierSet, router serializes it via __json__() before invoking chord."""
        collection = overdrive_import_fixture.collection
        identifier_set = IdentifierSet(
            redis_fixture.client, ["test", "router", "key"]
        )  # key segments
        mock_async_result = Mock()
        mock_async_result.id = "chord-id"
        mock_chord.apply_async.return_value = mock_async_result

        result = overdrive.import_result_router.delay(
            import_result=identifier_set,
            collection_id=collection.id,
            import_all=False,
            modified_since=None,
        ).wait()

        mock_chord.apply_async.assert_called_once()
        call_args = mock_chord.apply_async.call_args.kwargs["args"]
        # Verify the IdentifierSet was serialized to a dict (not passed as an object)
        serialized = call_args[0]
        assert isinstance(serialized, dict)
        assert serialized == identifier_set.__json__()
        assert result == {"chord_id": "chord-id"}

    @patch("palace.manager.celery.tasks.overdrive.import_children_and_cleanup_chord")
    def test_router_skips_chord_when_import_result_is_none(
        self,
        mock_chord: MagicMock,
        overdrive_import_fixture: OverdriveImportFixture,
        celery_fixture: CeleryFixture,
    ):
        """When import_result is None, router returns without invoking chord."""
        collection = overdrive_import_fixture.collection

        result = overdrive.import_result_router.delay(
            import_result=None,
            collection_id=collection.id,
            import_all=False,
            modified_since=None,
        ).wait()

        mock_chord.apply_async.assert_not_called()
        assert result == {"chord_id": None}


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

        # Verify each child import received the parent identifier set
        for call in mock_import.si.call_args_list:
            assert call.kwargs["parent_identifiers"] == mock_identifier_set

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
        new=SeededOverdriveAPI,
    )
    def test_full_import_flow_with_parent_identifiers_and_overdrive_data(
        self,
        overdrive_api_fixture: OverdriveAPIFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        apply_task_fixture: ApplyTaskFixture,
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

            result = import_collection_group.delay(
                collection_id=collection.id, import_all=True
            ).wait()

            # Wait for the import chain to complete. The import_collection_group task
            # starts an async chain and returns immediately with the chain_id. We need
            # to wait for that chain to finish before processing apply tasks.
            chain_result = AsyncResult(result["chain_id"]).wait()

            # The chain's last task (import_result_router) fires off
            # import_children_and_cleanup_chord asynchronously. We must wait for
            # that chord to finish as well, otherwise its worker thread may still
            # hold a savepoint on the shared test session when we call
            # process_apply_queue(), causing a ResourceClosedError.
            if chord_id := chain_result.get("chord_id"):
                AsyncResult(chord_id).wait()

            # Process the queued apply tasks synchronously. The import task fires off
            # bibliographic_apply and circulation_apply tasks asynchronously, which are
            # captured by the ApplyTaskFixture. Processing them here ensures the database
            # records are created before we verify them.
            apply_task_fixture.process_apply_queue()

            # Verify that the identifier is now in the database.
            assert self._get_identifier(book, overdrive_api_fixture)

    def _get_identifier(self, book, overdrive_api_fixture):
        identifier, _ = Identifier.for_foreign_id(
            overdrive_api_fixture.db.session,
            foreign_id=book["id"],
            foreign_identifier_type=Identifier.OVERDRIVE_ID,
            autocreate=False,
        )
        return identifier


class TestOverdriveReaper:
    """Tests for the reap_all_collections and reap_collection Celery tasks."""

    def test_reap_all_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ):
        """reap_all_collections queues reap_collection for every Overdrive collection,
        including child (Advantage) collections."""
        db.default_collection()  # non-Overdrive, should be ignored
        collection1 = db.collection(protocol=OverdriveAPI)
        collection2 = db.collection(protocol=OverdriveAPI)
        child_collection = db.collection(protocol=OverdriveAPI)
        child_collection.parent = collection1

        with patch.object(overdrive, "reap_collection") as mock_reap:
            overdrive.reap_all_collections.delay().wait()

        mock_reap.delay.assert_has_calls(
            [
                call(collection1.id),
                call(collection2.id),
                call(child_collection.id),
            ],
            any_order=True,
        )
        assert mock_reap.delay.call_count == 3

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_empty(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """When no identifiers exist for a collection, reap_collection logs complete and returns."""
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.info)

        overdrive.reap_collection.delay(collection.id).wait()

        mock_api_class.return_value.update_licensepool.assert_not_called()
        assert "complete" in caplog.text

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_processes_batch(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """reap_collection calls update_licensepool for each identifier in the batch."""
        collection = overdrive_api_fixture.collection
        edition1 = db.edition()
        edition2 = db.edition()
        pool1 = db.licensepool(edition1, collection=collection)
        pool2 = db.licensepool(edition2, collection=collection)
        db.session.flush()

        overdrive.reap_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        identifiers_called = {
            c.args[0] for c in mock_api.update_licensepool.call_args_list
        }
        assert pool1.identifier.identifier in identifiers_called
        assert pool2.identifier.identifier in identifiers_called

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_replaces_when_full_batch(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """When a full batch is processed, task.replace is raised with the next offset."""
        collection = overdrive_api_fixture.collection
        # Create exactly batch_size identifiers to trigger replace
        batch_size = 3
        pools = [
            db.licensepool(db.edition(), collection=collection)
            for _ in range(batch_size)
        ]
        db.session.flush()
        last_id = max(p.identifier.id for p in pools)

        with patch.object(overdrive.reap_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                overdrive.reap_collection.delay(
                    collection.id, batch_size=batch_size
                ).wait()

            replace_sig = mock_replace.call_args[0][0]
            assert replace_sig.kwargs["offset"] == last_id
            assert "lock_value" not in replace_sig.kwargs

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_stops_on_partial_batch(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """When fewer identifiers than batch_size exist, the task completes without replacing."""
        collection = overdrive_api_fixture.collection
        batch_size = 10
        db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        with patch.object(overdrive.reap_collection, "replace") as mock_replace:
            overdrive.reap_collection.delay(collection.id, batch_size=batch_size).wait()

        mock_replace.assert_not_called()

    def test_reap_collection_lock_not_released_on_autoretry(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """A retryable failure holds the workflow lock and each retry re-runs the reap.

        The workflow lock is keyed on ``task.request.id``, which Celery preserves across
        retries, so every retry re-acquires the same workflow lock and re-runs the batch,
        rather than skipping as if another run were in progress. The lock stays held so no
        concurrent run can start.
        """
        collection = overdrive_api_fixture.collection
        db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        mock_response = MockRequestsResponse(500, content="Internal Server Error")

        with patch(
            "palace.manager.celery.tasks.overdrive.OverdriveAPI"
        ) as mock_api_class:
            mock_api_class.return_value.update_licensepool.side_effect = (
                BadResponseException("http://test.com", "Bad response", mock_response)
            )

            with celery_fixture.patch_retry_backoff():
                overdrive.reap_collection.delay(collection.id).get(propagate=False)

            # The reap was re-run on every retry (1 initial attempt + max_retries=4),
            # not skipped as an "already in progress" run.
            assert mock_api_class.return_value.update_licensepool.call_count == 5

        # Lock is still held after retries exhaust; it expires via the Redis TTL.
        workflow_lock = reap_workflow_lock(
            redis_fixture.client, collection.id, random_value="any"
        )
        assert workflow_lock.locked()

    def test_reap_collection_skips_when_lock_held(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """On the first batch, if the workflow lock is already held, the task skips without processing."""
        collection = overdrive_api_fixture.collection
        db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        lock_value = str(uuid4())
        workflow_lock = reap_workflow_lock(
            redis_fixture.client, collection.id, lock_value
        )
        workflow_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.celery.tasks.overdrive.OverdriveAPI"
        ) as mock_api_class:
            overdrive.reap_collection.delay(collection.id).wait()
            mock_api_class.return_value.update_licensepool.assert_not_called()

        assert "skipped" in caplog.text
        assert "already in progress" in caplog.text
        workflow_lock.release()


class TestBackfillCollection:
    """Tests for the Overdrive backfill tasks."""

    @staticmethod
    def _healthy_pool(db: DatabaseTransactionFixture, collection):
        """Build a pool showing none of the three damage symptoms."""
        edition = db.edition(authors=["Author, An"])
        edition.publisher = "A Publisher"
        work = db.work(presentation_edition=edition)
        work.presentation_ready = True
        pool = db.licensepool(
            edition,
            collection=collection,
            with_open_access_download=True,
            set_edition_as_presentation=True,
            work=work,
        )
        db.session.flush()
        return pool

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_skips_healthy_pools(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """A pool with none of the three symptoms is left alone."""
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.info)
        self._healthy_pool(db, collection)

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api_class.return_value.ensure_bibliographic_coverage.assert_not_called()
        assert "complete" in caplog.text

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_selects_pool_with_no_delivery_mechanism(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """Symptom (a): nothing a patron could open."""
        collection = overdrive_api_fixture.collection
        pool = self._healthy_pool(db, collection)
        for lpdm in list(pool.delivery_mechanisms):
            lpdm.delete()
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        assert [
            c.args[0] for c in mock_api.ensure_bibliographic_coverage.call_args_list
        ] == [pool]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_selects_pool_without_presentation_ready_work(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """Symptom (b): the title cannot surface in the search index."""
        collection = overdrive_api_fixture.collection
        pool = self._healthy_pool(db, collection)
        pool.work.presentation_ready = False
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        assert [
            c.args[0] for c in mock_api.ensure_bibliographic_coverage.call_args_list
        ] == [pool]

    @pytest.mark.parametrize("symptom", ["no_publisher", "no_contributions"])
    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_selects_pool_with_degraded_bibliographic_data(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        symptom: str,
    ):
        """Symptom (c): fields the product-list summary never carried."""
        collection = overdrive_api_fixture.collection
        pool = self._healthy_pool(db, collection)
        edition = pool.presentation_edition
        if symptom == "no_publisher":
            edition.publisher = None
        else:
            for contribution in list(edition.contributions):
                db.session.delete(contribution)
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        assert [
            c.args[0] for c in mock_api.ensure_bibliographic_coverage.call_args_list
        ] == [pool]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_visits_a_multi_symptom_pool_once(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """The selectors are OR'd, so overlapping damage is still paid for once."""
        collection = overdrive_api_fixture.collection
        pool = self._healthy_pool(db, collection)
        # All three symptoms at once.
        for lpdm in list(pool.delivery_mechanisms):
            lpdm.delete()
        pool.work.presentation_ready = False
        pool.presentation_edition.publisher = None
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        assert mock_api.ensure_bibliographic_coverage.call_count == 1

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_forces_the_apply(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """The stored fingerprint cannot see this damage, so the apply must be forced."""
        collection = overdrive_api_fixture.collection
        db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        assert mock_api.ensure_bibliographic_coverage.call_args.kwargs == {
            "force": True
        }

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_ignores_other_collections(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        collection = overdrive_api_fixture.collection
        mine = db.licensepool(db.edition(), collection=collection)
        db.licensepool(db.edition(), collection=db.collection())
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id).wait()

        mock_api = mock_api_class.return_value
        assert [
            c.args[0] for c in mock_api.ensure_bibliographic_coverage.call_args_list
        ] == [mine]

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_keeps_repairs_made_before_a_mid_batch_failure(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """A failure part way through a batch does not discard what came before.

        Each title costs a blocking Overdrive request, so losing a batch's worth of
        completed repairs to one failure means paying for them all again. Committing
        per title also means the retry, which restarts at the same offset, skips them:
        a repaired pool no longer matches the damage query.
        """
        collection = overdrive_api_fixture.collection
        pools = []
        for _ in range(3):
            pool = self._healthy_pool(db, collection)
            # Symptom (c): a presentation edition with no publisher.
            pool.presentation_edition.publisher = None
            pools.append(pool)
        db.session.flush()
        assert overdrive.count_backfill_candidates(db.session, collection.id) == 3

        def repair_then_fail(pool: LicensePool, *, force: bool = False) -> None:
            if pool.id == pools[-1].id:
                raise ValueError("Overdrive fell over")
            pool.presentation_edition.publisher = "A Publisher"

        mock_api_class.return_value.ensure_bibliographic_coverage.side_effect = (
            repair_then_fail
        )

        with pytest.raises(ValueError, match="fell over"):
            overdrive.backfill_collection.delay(collection.id).wait()
        db.session.expire_all()

        # The two repaired before the failure are committed and no longer selected.
        assert [pool.presentation_edition.publisher for pool in pools] == [
            "A Publisher",
            "A Publisher",
            None,
        ]
        assert overdrive.count_backfill_candidates(db.session, collection.id) == 1

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_replaces_when_full_batch(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """A full batch re-queues the task at the next offset."""
        collection = overdrive_api_fixture.collection
        batch_size = 3
        pools = [
            db.licensepool(db.edition(), collection=collection)
            for _ in range(batch_size)
        ]
        db.session.flush()
        last_id = max(p.id for p in pools)

        with patch.object(overdrive.backfill_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                overdrive.backfill_collection.delay(
                    collection.id, batch_size=batch_size
                ).wait()

            replace_sig = mock_replace.call_args[0][0]
            assert replace_sig.kwargs["offset"] == last_id
            assert "lock_value" not in replace_sig.kwargs

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_stops_on_partial_batch(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        collection = overdrive_api_fixture.collection
        db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        with patch.object(overdrive.backfill_collection, "replace") as mock_replace:
            overdrive.backfill_collection.delay(collection.id, batch_size=10).wait()

        mock_replace.assert_not_called()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_max_titles_caps_the_whole_run(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """``max_titles`` caps titles, not batches, and stops rather than re-queuing.

        It has to bound the query as well as the loop: every pool the query returns
        costs one blocking Overdrive request, so fetching a full batch and then
        discarding the surplus would pay for titles it never meant to process.
        """
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.info)
        for _ in range(5):
            db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        with patch.object(overdrive.backfill_collection, "replace") as mock_replace:
            overdrive.backfill_collection.delay(
                collection.id, batch_size=10, max_titles=2
            ).wait()

        coverage = mock_api_class.return_value.ensure_bibliographic_coverage
        assert coverage.call_count == 2
        mock_replace.assert_not_called()
        assert "reached the 2-title cap" in caplog.text

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_max_titles_counts_down_across_batches(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """A cap larger than the batch hands the remainder to the next batch."""
        collection = overdrive_api_fixture.collection
        for _ in range(5):
            db.licensepool(db.edition(), collection=collection)
        db.session.flush()

        with patch.object(overdrive.backfill_collection, "replace") as mock_replace:
            mock_replace.side_effect = Exception("replaced")
            with pytest.raises(Exception, match="replaced"):
                overdrive.backfill_collection.delay(
                    collection.id, batch_size=2, max_titles=5
                ).wait()

        assert mock_replace.call_args[0][0].kwargs["max_titles"] == 3

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_imported_since_bounds_the_run(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """Only titles written at or after the bound are repaired.

        The damage came from a known release, so bounding by when we last wrote a
        record isolates the titles that release touched.
        """
        collection = overdrive_api_fixture.collection
        cutoff = utc_now() - datetime.timedelta(days=7)

        old_edition = db.edition()
        old_edition.updated_at = cutoff - datetime.timedelta(days=1)
        old_pool = db.licensepool(
            old_edition, collection=collection, set_edition_as_presentation=True
        )
        recent_edition = db.edition()
        recent_edition.updated_at = cutoff + datetime.timedelta(days=1)
        recent_pool = db.licensepool(
            recent_edition, collection=collection, set_edition_as_presentation=True
        )
        db.session.flush()

        overdrive.backfill_collection.delay(collection.id, imported_since=cutoff).wait()

        coverage = mock_api_class.return_value.ensure_bibliographic_coverage
        repaired = {call.args[0].id for call in coverage.call_args_list}
        assert repaired == {recent_pool.id}

    def test_count_backfill_candidates_matches_what_the_run_processes(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
    ):
        """The dry-run count and the real query must not drift apart.

        They share :func:`backfill_query`, and this is what holds them to it.
        """
        collection = overdrive_api_fixture.collection
        self._healthy_pool(db, collection)
        damaged = [
            db.licensepool(db.edition(), collection=collection) for _ in range(3)
        ]
        db.session.flush()

        assert overdrive.count_backfill_candidates(db.session, collection.id) == len(
            damaged
        )

        # And it honours the same date bound.
        cutoff = utc_now() + datetime.timedelta(days=1)
        assert (
            overdrive.count_backfill_candidates(db.session, collection.id, cutoff) == 0
        )

    @patch(
        target="palace.manager.celery.tasks.overdrive.OverdriveAPI",
        new=SeededOverdriveAPI,
    )
    def test_backfill_actually_clears_the_damage(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """A repaired pool stops being selected.

        Every other test here patches the API away, so none of them check that a
        repair actually fixes anything. It matters: the batch moves past a pool
        whether or not the repair worked, and the run logs "complete" either way, so
        a repair that left the pool damaged would look like a success.
        """
        collection = overdrive_api_fixture.collection
        metadata = json.loads(
            overdrive_files_fixture.sample_data("overdrive_metadata.json")
        )
        pool, _ = LicensePool.for_foreign_id(
            db.session,
            DataSource.OVERDRIVE,
            Identifier.OVERDRIVE_ID,
            metadata["id"],
            collection=collection,
        )
        db.session.flush()
        assert overdrive.count_backfill_candidates(db.session, collection.id) == 1

        overdrive_api_fixture.mock_http.queue_response(
            200, content=json.dumps(metadata)
        )
        overdrive.backfill_collection.delay(collection.id).wait()
        db.session.expire_all()

        assert any(lpdm.available for lpdm in pool.delivery_mechanisms)
        assert overdrive.count_backfill_candidates(db.session, collection.id) == 0

    def test_backfill_repairs_a_shared_title_for_every_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        overdrive_files_fixture: OverdriveFilesFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """Repairing a title through one collection repairs it for all of them.

        The whole run is sequenced on this: collections go in one chain, and each
        one is cheaper than the last because titles the previous collection already
        repaired no longer match. If a repair only fixed the collection it ran
        through, every other collection holding the title would still select it and
        pay for it again.

        Worth pinning because the second pool is fixed indirectly. Its own
        ``calculate_work`` never runs -- by the time it would, the shared work is
        already presentation-ready -- so it depends on ``Work.calculate_presentation``
        setting a presentation edition on every pool attached to the work, not just
        the one the repair was reached through.
        """
        parent = overdrive_api_fixture.collection
        child = db.collection(protocol=OverdriveAPI)
        child.parent = parent

        metadata = json.loads(
            overdrive_files_fixture.sample_data("overdrive_metadata.json")
        )
        pools = [
            LicensePool.for_foreign_id(
                db.session,
                DataSource.OVERDRIVE,
                Identifier.OVERDRIVE_ID,
                metadata["id"],
                collection=collection,
            )[0]
            for collection in (parent, child)
        ]
        db.session.flush()

        # Two damaged pools, one title.
        collections = [parent.id, child.id]
        assert overdrive.count_backfill_candidates(db.session, collections) == 2
        assert overdrive.count_backfill_titles(db.session, collections) == 1

        # Repair through the parent only, and only one metadata response is queued,
        # so a second request would fail the test rather than pass silently.
        overdrive_api_fixture.queue_access_token_response()
        overdrive_api_fixture.queue_collection_token()
        overdrive_api_fixture.mock_http.queue_response(
            200, content=json.dumps(metadata)
        )
        overdrive.backfill_collection.delay(parent.id).wait()
        db.session.expire_all()

        # Both pools are whole, including the one in the collection that never ran.
        for pool in pools:
            assert pool.presentation_edition is not None
            assert pool.work is not None
            assert pool.work.presentation_ready
            assert any(lpdm.available for lpdm in pool.delivery_mechanisms)

        # And neither collection would spend another request on it.
        assert overdrive.count_backfill_candidates(db.session, collections) == 0

    def test_counts_distinguish_shared_titles_from_pools(
        self,
        db: DatabaseTransactionFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
    ) -> None:
        """A title in two collections is two pools but only one repair.

        Every damage symptom is keyed on the title, not the collection: delivery
        mechanisms belong to a book and a distributor, and pools sharing an identifier
        share a work and an edition. So repairing the title through one collection
        stops the other from selecting it, and summing per-collection counts
        overstates what a run costs.
        """
        parent = overdrive_api_fixture.collection
        child = db.collection(protocol=OverdriveAPI)
        child.parent = parent

        shared = db.identifier(identifier_type=Identifier.OVERDRIVE_ID)
        for collection in (parent, child):
            LicensePool.for_foreign_id(
                db.session,
                DataSource.OVERDRIVE,
                Identifier.OVERDRIVE_ID,
                shared.identifier,
                collection=collection,
            )
        db.session.flush()

        collections = [parent.id, child.id]
        # Two damaged pools...
        assert overdrive.count_backfill_candidates(db.session, collections) == 2
        # ...but one title, and so one Overdrive request.
        assert overdrive.count_backfill_titles(db.session, collections) == 1

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_backfill_skips_suspected_magazines_unless_asked(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ) -> None:
        """A title whose series is its own title is left alone by default.

        Overdrive serves magazines under a format id we do not map, so repairing one
        spends a blocking request to leave it exactly as damaged as it was.
        """
        collection = overdrive_api_fixture.collection
        magazine = self._healthy_pool(db, collection)
        magazine.presentation_edition.series = magazine.presentation_edition.title
        for lpdm in list(magazine.delivery_mechanisms):
            lpdm.delete()
        db.session.flush()

        mock_api = mock_api_class.return_value
        overdrive.backfill_collection.delay(collection.id).wait()
        mock_api.ensure_bibliographic_coverage.assert_not_called()

        overdrive.backfill_collection.delay(
            collection.id, include_suspected_magazines=True
        ).wait()
        assert [
            c.args[0] for c in mock_api.ensure_bibliographic_coverage.call_args_list
        ] == [magazine]

    @patch("palace.manager.celery.tasks.overdrive.chain")
    def test_backfill_all_collections_runs_children_after_their_parent(
        self,
        mock_chain: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
    ) -> None:
        """A parent and its Advantage children must not run at the same time.

        They share delivery mechanisms and an edition, so running them together
        means paying twice for the same metadata request and writing the same rows
        from two workers -- which can deadlock, and which the task does not retry.
        """
        parent = overdrive_api_fixture.collection
        child = db.collection(protocol=OverdriveAPI)
        child.parent = parent
        db.session.flush()

        overdrive.backfill_all_collections.delay().wait()

        assert mock_chain.call_count == 1
        queued = [signature.args[0] for signature in mock_chain.call_args.args]
        assert queued == [parent.id, child.id]

    @patch("palace.manager.celery.tasks.overdrive.chain")
    def test_backfill_all_collections_names_what_a_failure_would_skip(
        self,
        mock_chain: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
    ) -> None:
        """Each collection carries an error callback naming what follows it.

        A chain stops at its first failure, so without this the collections queued
        behind a failed one would never run and never be mentioned again.
        """
        first = overdrive_api_fixture.collection
        second = db.collection(protocol=OverdriveAPI)
        db.session.flush()

        overdrive.backfill_all_collections.delay().wait()

        queued = list(mock_chain.call_args.args)
        # Every signature has one, and each names only the collections after it.
        reported = [
            (
                signature.options["link_error"][0].kwargs["failed_collection_id"],
                signature.options["link_error"][0].kwargs["skipped_collection_ids"],
            )
            for signature in queued
        ]
        order = [signature.args[0] for signature in queued]
        assert reported == [
            (collection_id, order[position + 1 :])
            for position, collection_id in enumerate(order)
        ]
        # The last one has nothing behind it.
        assert reported[-1][1] == []
        assert set(order) == {first.id, second.id}

    def test_report_backfill_stopped_names_the_skipped_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The report names the failed collection and the ones left unstarted."""
        caplog.set_level(LogLevel.error)
        failed = overdrive_api_fixture.collection
        skipped = db.collection(protocol=OverdriveAPI)
        db.session.flush()

        overdrive.report_backfill_stopped.delay(
            failed_collection_id=failed.id,
            skipped_collection_ids=[skipped.id],
        ).wait()

        assert f"collection '{failed.name}'" in caplog.text
        assert f"'{skipped.name}'" in caplog.text
        assert "1 collection(s) queued behind it did not start" in caplog.text
        assert "Re-running the backfill is safe" in caplog.text

    def test_report_backfill_stopped_when_nothing_followed(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A failure on the last collection skipped nothing."""
        caplog.set_level(LogLevel.error)
        failed = overdrive_api_fixture.collection
        db.session.flush()

        overdrive.report_backfill_stopped.delay(
            failed_collection_id=failed.id,
            skipped_collection_ids=[],
        ).wait()

        assert "It was the last one, so nothing was skipped." in caplog.text

    @patch.object(OverdriveAPI, "ensure_bibliographic_coverage")
    def test_backfill_all_collections_reports_a_run_that_stopped_early(
        self,
        mock_coverage: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A real failure reaches the error callback, not just the wiring.

        The other tests here check that the callback is attached and what it says.
        This one lets a collection actually fail and confirms Celery runs it.
        """
        caplog.set_level(LogLevel.error)
        first = overdrive_api_fixture.collection
        self._healthy_pool(db, first).presentation_edition.publisher = None
        second = db.collection(protocol=OverdriveAPI)
        db.session.flush()

        mock_coverage.side_effect = ValueError("Overdrive fell over")
        # The parent only queues the chain, so the collection's failure surfaces
        # through the error callback rather than here.
        overdrive.backfill_all_collections.delay().wait()

        assert "Overdrive backfill stopped" in caplog.text
        assert f"collection '{first.name}'" in caplog.text
        assert f"'{second.name}'" in caplog.text
        assert "Re-running the backfill is safe" in caplog.text

    def test_backfill_all_collections_queues_each_collection(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        collection = overdrive_api_fixture.collection

        with patch.object(overdrive.backfill_collection, "si") as mock_signature:
            overdrive.backfill_all_collections.delay().wait()

        queued = {c.args[0] for c in mock_signature.call_args_list}
        assert collection.id in queued
        assert "Queued" in caplog.text

    @patch("palace.manager.celery.tasks.overdrive.chain")
    def test_backfill_all_collections_queues_one_chain_for_every_collection(
        self,
        mock_chain: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
    ) -> None:
        """Unrelated collections must not run at the same time either.

        Two libraries' separate collections on one manager can hold the same title,
        and a shared title brings one edition and one set of delivery mechanisms with
        it. So everything goes in a single chain, not one chain per family.
        """
        first = overdrive_api_fixture.collection
        second = db.collection(protocol=OverdriveAPI)
        child = db.collection(protocol=OverdriveAPI)
        child.parent = first
        db.session.flush()

        overdrive.backfill_all_collections.delay().wait()

        assert mock_chain.call_count == 1
        queued = [signature.args[0] for signature in mock_chain.call_args.args]
        assert set(queued) == {first.id, child.id, second.id}
        assert queued.index(child.id) == queued.index(first.id) + 1
