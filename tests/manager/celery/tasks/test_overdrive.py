"""Tests for Overdrive Celery tasks."""

from __future__ import annotations

from unittest.mock import MagicMock, Mock, call, patch
from uuid import uuid4

import pytest
from celery.result import AsyncResult

from palace.util.datetime_helpers import datetime_utc
from palace.util.log import LogLevel

from palace.manager.celery.importer import (
    import_workflow_lock,
    reap_key,
    reap_workflow_lock,
)
from palace.manager.celery.tasks import identifiers, overdrive
from palace.manager.celery.tasks.overdrive import import_collection_group
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.integration.license.overdrive.api import (
    BookInfoEndpoint,
    OverdriveAPI,
    ProductPage,
)
from palace.manager.integration.license.overdrive.importer import (
    FeedImportResult,
    OverdriveImporter,
)
from palace.manager.service.redis.models.set import IdentifierSet
from palace.manager.sqlalchemy.constants import IdentifierType
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.sqlalchemy.model.licensing import LicensePool, LicensePoolStatus
from palace.manager.util.http.exception import BadResponseException
from tests.fixtures.celery import ApplyTaskFixture, CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.overdrive import OverdriveAPIFixture
from tests.fixtures.redis import RedisFixture
from tests.mocks.mock import MockRequestsResponse
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
        new=MockOverdriveAPI,
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


def product_page(
    *,
    listed: tuple[IdentifierData, ...] = (),
    unowned: tuple[IdentifierData, ...] = (),
    total_items: int | None = None,
    limit: int = 2000,
) -> ProductPage:
    """Build a ProductPage.

    `total_items` is passed through as given, so a test can ask for the case where
    Overdrive omits it.
    """
    return ProductPage(
        listed=listed,
        unowned=unowned,
        total_items=total_items,
        limit=limit,
    )


def mock_crawl(mock_api_class: MagicMock, *pages: ProductPage) -> MagicMock:
    """Point a mocked OverdriveAPI at the given pages, walked in order.

    The mocked API has to be told where the walk ends; otherwise
    `next_product_offset` returns a Mock and the crawl never finishes.
    """
    api = mock_api_class.return_value
    api.fetch_product_page.side_effect = list(pages)
    api.next_product_offset.side_effect = [
        (index + 1) * 100 for index in range(len(pages) - 1)
    ] + [None]
    return api


def overdrive_identifiers(*identifiers: str) -> tuple[IdentifierData, ...]:
    return tuple(
        IdentifierData(type=Identifier.OVERDRIVE_ID, identifier=identifier)
        for identifier in identifiers
    )


def overdrive_pool(
    db: DatabaseTransactionFixture, collection: Collection, identifier: str
) -> LicensePool:
    """Create an available Overdrive license pool with the given identifier."""
    edition = db.edition(
        data_source_name=DataSource.OVERDRIVE,
        identifier_type=Identifier.OVERDRIVE_ID,
        identifier_id=identifier,
    )
    return db.licensepool(
        edition,
        collection=collection,
        data_source_name=DataSource.OVERDRIVE,
        open_access=False,
    )


class TestOverdriveReaper:
    """Tests for the reap_all_collections and reap_collection Celery tasks."""

    def test_reap_all_collections(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ):
        """reap_all_collections queues a reap for every Overdrive collection,
        including child (Advantage) collections."""
        db.default_collection()  # non-Overdrive, should be ignored
        collection1 = db.collection(protocol=OverdriveAPI)
        collection2 = db.collection(protocol=OverdriveAPI)
        child_collection = db.collection(protocol=OverdriveAPI)
        child_collection.parent = collection1

        with patch.object(OverdriveAPI, "reap_task") as mock_reap_task:
            overdrive.reap_all_collections.delay().wait()

        mock_reap_task.assert_has_calls(
            [
                call(collection1.id),
                call(collection2.id),
                call(child_collection.id),
            ],
            any_order=True,
        )
        assert mock_reap_task.return_value.apply_async.call_count == 3

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_returns_listed_identifiers(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """A completed crawl returns every identifier the collection still lists."""
        collection = overdrive_api_fixture.collection
        listed = overdrive_identifiers("id-one", "id-two")
        mock_crawl(mock_api_class, product_page(listed=listed, total_items=2))

        result = overdrive.reap_collection.delay(collection.id).wait()

        identifier_set = IdentifierSet(redis_fixture.client, result["key"])
        assert identifier_set.get() == set(listed)

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_pages_until_crawl_completes(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """The identifiers from every page end up in one set.

        Each page runs as a fresh task via task.replace(), and the set is keyed on the
        task id, which Celery preserves across that hand-off. If it ever stopped doing
        so, the crawl would return only its final page and the reaper would mark
        almost the whole collection as gone.
        """
        collection = overdrive_api_fixture.collection
        first_page = overdrive_identifiers("id-one", "id-two")
        second_page = overdrive_identifiers("id-three")
        api = mock_crawl(
            mock_api_class,
            product_page(listed=first_page, total_items=3, limit=2),
            product_page(listed=second_page, total_items=3, limit=2),
        )

        result = overdrive.reap_collection.delay(collection.id).wait()

        assert api.fetch_product_page.call_count == 2
        # Each page was requested at the offset the walk handed back, so a crawl
        # cannot be talked into re-requesting the page it just fetched.
        assert api.product_page_endpoint.call_args_list == [call(0), call(100)]
        identifier_set = IdentifierSet(redis_fixture.client, result["key"])
        assert identifier_set.get() == set(first_page) | set(second_page)

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_aborts_on_incomplete_crawl(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """A crawl that saw far fewer products than Overdrive reports marks nothing.

        Reaping acts on the absence of an identifier, so handing a partial crawl to the
        chord would mark live titles as gone. Returning None instead makes the chord
        body abort.
        """
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.error)
        mock_crawl(
            mock_api_class,
            product_page(listed=overdrive_identifiers("id-one"), total_items=5000),
        )

        async_result = overdrive.reap_collection.delay(collection.id)

        assert async_result.wait() is None
        assert "Refusing to reap on an incomplete crawl" in caplog.text
        # The partial set is not left behind in Redis for the chord to pick up.
        partial_set = IdentifierSet(
            redis_fixture.client, reap_key(collection.id, async_result.id)
        )
        assert not partial_set.exists()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_aborts_when_total_items_missing(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """Without totalItems there is no way to show the crawl was complete."""
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.error)
        mock_crawl(
            mock_api_class,
            product_page(listed=overdrive_identifiers("id-one"), total_items=None),
        )

        result = overdrive.reap_collection.delay(collection.id).wait()

        assert result is None
        assert "no usable totalItems or page size" in caplog.text

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_tolerates_small_shortfall(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """Titles added or removed mid-crawl move totalItems, which is not a failure.

        The allowance only applies to a crawl that spanned more than one response --
        that is the only kind with a window for the collection to change underneath
        it.
        """
        collection = overdrive_api_fixture.collection
        first_page = overdrive_identifiers("id-one", "id-two")
        second_page = overdrive_identifiers("id-three")
        listed = first_page + second_page
        total_items = len(listed) + overdrive.REAP_CRAWL_ALLOWANCE_FLOOR
        mock_crawl(
            mock_api_class,
            product_page(listed=first_page, total_items=total_items),
            product_page(listed=second_page, total_items=total_items),
        )

        result = overdrive.reap_collection.delay(collection.id).wait()

        assert result is not None
        assert IdentifierSet(redis_fixture.client, result["key"]).get() == set(listed)

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_requires_an_exact_count_for_a_single_page(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """A collection that arrives in one response had no window for churn.

        Its products and its totalItems were produced together, so a shortfall is
        Overdrive truncating the response rather than the collection moving, and the
        allowance for churn should not excuse it.
        """
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.error)
        mock_crawl(
            mock_api_class,
            product_page(listed=overdrive_identifiers("id-one"), total_items=2),
        )

        result = overdrive.reap_collection.delay(collection.id).wait()

        assert result is None
        assert "Refusing to reap on an incomplete crawl" in caplog.text

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_aborts_when_a_page_cannot_be_paged_from(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        """Giving up mid-walk is not the same as reaching the start of the list.

        A page that arrives without the fields the walk needs stops the crawl at an
        unknown point, so it aborts outright rather than letting the shortfall be
        weighed against an allowance meant for churn.
        """
        collection = overdrive_api_fixture.collection
        caplog.set_level(LogLevel.error)
        api = mock_crawl(
            mock_api_class,
            product_page(listed=overdrive_identifiers("id-one"), total_items=2005),
            product_page(listed=(), total_items=None, limit=0),
        )
        # The walk would otherwise read the second page as "collection covered".
        api.next_product_offset.side_effect = [5, None]

        async_result = overdrive.reap_collection.delay(collection.id)

        assert async_result.wait() is None
        assert "cannot be continued or verified" in caplog.text
        partial_set = IdentifierSet(
            redis_fixture.client, reap_key(collection.id, async_result.id)
        )
        assert not partial_set.exists()

    @pytest.mark.parametrize(
        "crawled,total_items,single_page,expected",
        [
            pytest.param(1_000, None, False, False, id="totalItems missing"),
            pytest.param(1_000, 1_000, False, True, id="exact match"),
            pytest.param(0, 5, False, True, id="floor covers a tiny collection"),
            pytest.param(0, 100, False, False, id="shortfall beyond the floor"),
            pytest.param(
                4_000_000 - 400, 4_000_000, False, True, id="churn within the cap"
            ),
            pytest.param(
                4_000_000 - 2_000,
                4_000_000,
                False,
                False,
                id="a lost page is not excused",
            ),
            pytest.param(1_000, 1_000, True, True, id="single page, counts agree"),
            pytest.param(999, 1_000, True, False, id="single page, one short"),
        ],
    )
    def test_crawl_is_complete(
        self,
        crawled: int,
        total_items: int | None,
        single_page: bool,
        expected: bool,
    ):
        """The allowance absorbs churn during a crawl, but never a whole lost page.

        It is capped because the churn it exists for does not scale with collection
        size, while a proportional allowance does: 0.1% of a 4M-title collection would
        be two full pages, so a crawl that silently dropped one would be accepted and
        those titles reaped. A collection that arrived in a single response had no
        window for churn at all, so its counts have to agree exactly.
        """
        assert (
            overdrive._crawl_is_complete(
                MagicMock(),
                "collection name",
                crawled,
                total_items,
                single_page=single_page,
            )
            is expected
        )

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_marks_titles_overdrive_no_longer_owns(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """Weeded titles are marked from Overdrive's flag, not from their absence.

        Overdrive keeps weeded and expired titles in the product list, flagged
        `isOwnedByCollections: false`. That is a statement that the title is gone, so
        it is acted on directly rather than inferred from a set difference.
        """
        collection = overdrive_api_fixture.collection
        overdrive_pool(db, collection, "id-weeded")
        overdrive_pool(db, collection, "id-available")
        already_gone = overdrive_pool(db, collection, "id-already-gone")
        already_gone.licenses_owned = already_gone.licenses_available = 0
        db.session.flush()

        listed = overdrive_identifiers("id-weeded", "id-available", "id-already-gone")
        mock_crawl(
            mock_api_class,
            product_page(
                listed=listed,
                unowned=overdrive_identifiers("id-weeded", "id-already-gone"),
                total_items=3,
            ),
        )

        with patch.object(identifiers, "circulation_apply") as mock_apply:
            result = overdrive.reap_collection.delay(collection.id).wait()

        # Only the weeded title we still record as available is marked. The one that is
        # already unavailable is left alone, so the titles Overdrive goes on listing as
        # unowned do not generate an apply task on every pass.
        mock_apply.delay.assert_called_once()
        circulation = mock_apply.delay.call_args.kwargs["circulation"]
        assert circulation.primary_identifier_data.identifier == "id-weeded"
        assert circulation.licenses_owned == 0
        assert circulation.licenses_available == 0
        assert circulation.status == LicensePoolStatus.EXHAUSTED

        # Unowned titles stay in the returned set, so the chord's set difference does
        # not mark them a second time.
        assert IdentifierSet(redis_fixture.client, result["key"]).get() == set(listed)

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_marks_unowned_titles_on_an_incomplete_crawl(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """A crawl that cannot be verified still marks what Overdrive stated outright.

        Only the half that acts on absence needs a complete crawl.
        """
        collection = overdrive_api_fixture.collection
        overdrive_pool(db, collection, "id-weeded")
        weeded = overdrive_identifiers("id-weeded")
        mock_crawl(
            mock_api_class,
            product_page(listed=weeded, unowned=weeded, total_items=5000),
        )

        with patch.object(identifiers, "circulation_apply") as mock_apply:
            result = overdrive.reap_collection.delay(collection.id).wait()

        assert result is None
        mock_apply.delay.assert_called_once()

    @patch("palace.manager.celery.tasks.overdrive.OverdriveAPI")
    def test_reap_collection_skips_collection_marked_for_deletion(
        self,
        mock_api_class: MagicMock,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        collection = overdrive_api_fixture.collection
        collection.marked_for_deletion = True
        db.session.flush()
        caplog.set_level(LogLevel.warning)

        result = overdrive.reap_collection.delay(collection.id).wait()

        assert result is None
        mock_api_class.return_value.fetch_product_page.assert_not_called()
        assert "marked for deletion" in caplog.text

    def test_reap_collection_lock_not_released_on_autoretry(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        overdrive_api_fixture: OverdriveAPIFixture,
        redis_fixture: RedisFixture,
    ):
        """A retryable failure holds the workflow lock and each retry re-runs the crawl.

        The workflow lock is keyed on ``task.request.id``, which Celery preserves across
        retries, so every retry re-acquires the same workflow lock and re-runs the page,
        rather than skipping as if another run were in progress. The lock stays held so no
        concurrent run can start.
        """
        collection = overdrive_api_fixture.collection
        mock_response = MockRequestsResponse(500, content="Internal Server Error")

        with patch(
            "palace.manager.celery.tasks.overdrive.OverdriveAPI"
        ) as mock_api_class:
            mock_api_class.return_value.fetch_product_page.side_effect = (
                BadResponseException("http://test.com", "Bad response", mock_response)
            )

            with celery_fixture.patch_retry_backoff():
                overdrive.reap_collection.delay(collection.id).get(propagate=False)

            # The crawl was re-run on every retry (1 initial attempt + max_retries=4),
            # not skipped as an "already in progress" run.
            assert mock_api_class.return_value.fetch_product_page.call_count == 5

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
        """If the workflow lock is already held, the task skips without crawling."""
        collection = overdrive_api_fixture.collection

        lock_value = str(uuid4())
        workflow_lock = reap_workflow_lock(
            redis_fixture.client, collection.id, lock_value
        )
        workflow_lock.acquire()

        caplog.set_level(LogLevel.warning)

        with patch(
            "palace.manager.celery.tasks.overdrive.OverdriveAPI"
        ) as mock_api_class:
            result = overdrive.reap_collection.delay(collection.id).wait()
            mock_api_class.return_value.fetch_product_page.assert_not_called()

        assert result is None
        assert "skipped" in caplog.text
        assert "already in progress" in caplog.text
        workflow_lock.release()
