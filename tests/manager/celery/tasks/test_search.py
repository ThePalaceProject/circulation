import json
import math
from collections import Counter
from unittest.mock import MagicMock, call, patch

import pytest
from celery.exceptions import MaxRetriesExceededError
from opensearchpy import OpenSearchException

from palace.manager.celery.tasks.search import (
    get_work_search_documents,
    index_works,
    search_indexing,
    search_reindex,
    update_read_pointer,
)
from palace.manager.scripts.initialization import InstanceInitializationScript
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.filter import Filter
from palace.manager.search.service import SearchPointer
from palace.manager.service.redis.models.lock import LockNotAcquired, TaskLock
from palace.manager.service.redis.models.search import WaitingForIndexing
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.search import EndToEndSearchFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.search import MockSearchSchemaRevisionLatest


class SearchReindexTaskLockFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture
        self.redis_client = redis_fixture.client

        self.task = MagicMock()
        self.task.request.root_id = "fake"
        self.task_lock = TaskLock(
            self.task, lock_name="search_reindex", redis_client=self.redis_client
        )


@pytest.fixture
def search_reindex_task_lock_fixture(redis_fixture: RedisFixture):
    return SearchReindexTaskLockFixture(redis_fixture)


@pytest.fixture
def mock_search_pointers(
    services_fixture: ServicesFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Point the mocked search service at a single index at the latest revision.

    A reindex reads both pointers to decide whether to advance the read pointer, and an
    unconfigured autospec hands back mocks that can neither be compared nor serialized
    into the task's requeue.
    """
    base_name = "test_index"
    revision = MockSearchSchemaRevisionLatest(42)
    index = revision.name_for_index(base_name)

    # The services fixture resets return values between tests, but not attributes we
    # assign directly, so base_revision_name is set through monkeypatch.
    monkeypatch.setattr(
        services_fixture.search_service, "base_revision_name", base_name
    )
    services_fixture.search_revision_directory.highest.return_value = revision
    services_fixture.search_service.read_pointer.return_value = SearchPointer(
        alias=f"{base_name}-search-read", index=index, version=revision.version
    )
    services_fixture.search_service.write_pointer.return_value = SearchPointer(
        alias=f"{base_name}-search-write", index=index, version=revision.version
    )


def test_get_work_search_documents(db: DatabaseTransactionFixture) -> None:
    work1 = db.work(with_open_access_download=True)
    work2 = db.work(with_open_access_download=True)
    # This work is not presentation ready, because it has no open access download.
    work3 = db.work(with_open_access_download=False)
    work4 = db.work(with_open_access_download=True)

    documents = get_work_search_documents(db.session, 2, 0)
    assert {doc["_id"] for doc in documents} == {work1.id, work2.id}

    documents = get_work_search_documents(db.session, 2, 2)
    assert {doc["_id"] for doc in documents} == {work4.id}

    documents = get_work_search_documents(db.session, 2, 4)
    assert documents == []


@pytest.mark.parametrize("batch_size", [2, 3, 500])
@patch("palace.manager.celery.tasks.search.random")
def test_search_reindex(
    mock_random: MagicMock,
    batch_size: int,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
) -> None:
    # When a batch fills up, search_reindex requeues itself with a random
    # `countdown` cooldown that the real Celery worker honours as an actual wait.
    # Mock it out so pagination is still exercised without the multi-second sleep.
    mock_random.uniform.return_value = 0.0

    client = end_to_end_search_fixture.external_search.write_client
    index = end_to_end_search_fixture.external_search_index

    work1 = db.work(with_open_access_download=True)
    work2 = db.work(with_open_access_download=True)
    work3 = db.work(with_open_access_download=False)
    work4 = db.work(with_open_access_download=True)

    # The works are not in the search index.
    client.indices.refresh()
    end_to_end_search_fixture.expect_results([], "")

    # Index the works, we use different batch sizes to test pagination.
    search_reindex.delay(batch_size=batch_size).wait()
    assert search_reindex_task_lock_fixture.task_lock.locked() is False
    client.indices.refresh()

    # Check that the works are in the search index.
    end_to_end_search_fixture.expect_results([work1, work2, work4], "", ordered=False)

    # Remove work1 from the search index.
    index.remove_work(work1)
    client.indices.refresh()
    end_to_end_search_fixture.expect_results([work2, work4], "", ordered=False)

    # Reindex the works.
    search_reindex.delay(batch_size=batch_size).wait()
    assert search_reindex_task_lock_fixture.task_lock.locked() is False
    client.indices.refresh()

    # Check that all the works are in the search index.
    end_to_end_search_fixture.expect_results([work1, work2, work4], "", ordered=False)


def test_search_reindex_lock(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    search_reindex_task_lock_fixture.task_lock.acquire()

    with pytest.raises(LockNotAcquired) as exc_info:
        search_reindex.delay().wait()

    assert "TaskLock::search_reindex could not be acquired" in str(exc_info.value)


def test_fiction_query_returns_results(
    db: DatabaseTransactionFixture, end_to_end_search_fixture: EndToEndSearchFixture
) -> None:
    work1 = db.work(with_open_access_download=True, fiction=True)
    work2 = db.work(with_open_access_download=True, fiction=False)
    documents = get_work_search_documents(db.session, 2, 0)
    assert {doc["_id"] for doc in documents} == {work2.id, work1.id}

    end_to_end_search_fixture.populate_search_index()
    end_to_end_search_fixture.expect_results(
        expect=[work1, work2], ordered=False, query_string=""
    )
    json_filter = Filter()
    json_filter.search_type = "json"
    qs = {"query": {"key": "fiction", "value": "fiction"}}
    end_to_end_search_fixture.expect_results(
        expect=[work1],
        ordered=False,
        filter=json_filter,
        query_string=json.dumps(qs),
    )
    qs["query"]["value"] = "nonfiction"
    end_to_end_search_fixture.expect_results(
        expect=[work2],
        ordered=False,
        filter=json_filter,
        query_string=json.dumps(qs),
    )


@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_search_reindex_failures(
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    services_fixture: ServicesFixture,
    mock_search_pointers: None,
):
    # Make sure our backoff function doesn't delay the test.
    mock_backoff.return_value = 0

    add_documents_mock = services_fixture.search_index.add_documents

    # If we fail to add documents, we should retry up to 4 times, then fail.
    add_documents_mock.return_value = [1, 2, 3]
    with pytest.raises(MaxRetriesExceededError):
        search_reindex.delay().wait()
    assert add_documents_mock.call_count == 5
    mock_backoff.assert_has_calls([call(0), call(1), call(2), call(3), call(4)])
    assert search_reindex_task_lock_fixture.task_lock.locked() is False

    add_documents_mock.reset_mock()
    add_documents_mock.side_effect = [[1, 2, 3], OpenSearchException(), None]
    search_reindex.delay().wait()
    assert add_documents_mock.call_count == 3
    assert search_reindex_task_lock_fixture.task_lock.locked() is False

    # Unknown exception, we don't retry, but do release the lock.
    add_documents_mock.reset_mock()
    add_documents_mock.side_effect = Exception()
    with pytest.raises(Exception):
        search_reindex.delay().wait()
    assert add_documents_mock.call_count == 1
    assert search_reindex_task_lock_fixture.task_lock.locked() is False


@patch("palace.manager.celery.tasks.search.random.uniform")
@patch("palace.manager.celery.tasks.search.exponential_backoff")
@patch("palace.manager.celery.tasks.search.get_work_search_documents")
def test_search_reindex_requeue_delay(
    mock_get_work_search_documents: MagicMock,
    mock_backoff: MagicMock,
    mock_random_uniform: MagicMock,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    services_fixture: ServicesFixture,
    mock_search_pointers: None,
) -> None:
    """Verify that the task requeues with a random delay to avoid hammering the search service."""
    mock_backoff.return_value = 0
    mock_random_uniform.return_value = 0

    # Return a full batch to trigger requeueing, then an empty batch to stop
    mock_get_work_search_documents.side_effect = [
        [{"_id": 1}, {"_id": 2}],
        [],
    ]

    # Ensure add_documents succeeds (returns no failed documents)
    services_fixture.search_index.add_documents.return_value = None

    search_reindex.delay(batch_size=2).wait()

    # Verify random.uniform was called with the expected range (5-15 seconds)
    mock_random_uniform.assert_called_once_with(5, 15)
    assert search_reindex_task_lock_fixture.task_lock.locked() is False


@patch("palace.manager.celery.tasks.search.random.uniform")
@patch("palace.manager.celery.tasks.search.exponential_backoff")
@patch("palace.manager.celery.tasks.search.get_work_search_documents")
def test_search_reindex_failures_multiple_batch(
    mock_get_work_search_documents: MagicMock,
    mock_backoff: MagicMock,
    mock_random_uniform: MagicMock,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    services_fixture: ServicesFixture,
    mock_search_pointers: None,
):
    # When a batch succeeds, the retry count is reset.
    mock_backoff.return_value = 0
    mock_random_uniform.return_value = 0
    search_documents = [
        {"_id": 1},
        {"_id": 2},
        {"_id": 3},
        {"_id": 4},
        {"_id": 5},
        {"_id": 6},
        {"_id": 7},
    ]
    mock_get_work_search_documents.side_effect = (
        lambda session, batch_size, offset: search_documents[
            offset : offset + batch_size
        ]
    )
    add_documents_mock = services_fixture.search_index.add_documents
    add_documents_mock.side_effect = [
        # First batch
        OpenSearchException(),
        OpenSearchException(),
        OpenSearchException(),
        OpenSearchException(),
        None,
        # Second batch
        OpenSearchException(),
        None,
        # Third batch
        None,
        # Fourth batch
        OpenSearchException(),
        OpenSearchException(),
        None,
    ]
    search_reindex.delay(batch_size=2).wait()
    assert add_documents_mock.call_count == 11
    assert search_reindex_task_lock_fixture.task_lock.locked() is False


def test_update_read_pointer(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    client = end_to_end_search_fixture.external_search.write_client
    service = end_to_end_search_fixture.external_search.service

    # Remove the read pointer
    alias_name = service.read_pointer_name()
    action = {
        "actions": [
            {"remove": {"index": "*", "alias": alias_name}},
        ]
    }
    client.indices.update_aliases(body=action)

    # Verify that the read pointer is gone
    assert service.read_pointer() is None

    # Update the read pointer
    update_read_pointer.delay().wait()

    # Verify that the read pointer is set
    assert service.read_pointer() is not None


@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_update_read_pointer_failures(
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
):
    # Make sure our backoff function doesn't delay the test.
    mock_backoff.return_value = 0

    read_pointer_set_mock = services_fixture.search_service.read_pointer_set
    read_pointer_set_mock.side_effect = OpenSearchException()
    with pytest.raises(MaxRetriesExceededError):
        update_read_pointer.delay().wait()
    assert read_pointer_set_mock.call_count == 5

    read_pointer_set_mock.reset_mock()
    read_pointer_set_mock.side_effect = [OpenSearchException(), None]
    update_read_pointer.delay().wait()
    assert read_pointer_set_mock.call_count == 2


class SearchMigrationFixture:
    """A search index mid-migration: a new revision exists and the write pointer has
    moved to it, but the read pointer is still serving the old one."""

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        end_to_end_search_fixture: EndToEndSearchFixture,
    ):
        container = end_to_end_search_fixture.external_search.search_container
        self.client = container.write_client()
        self.service = container.service()
        revision_directory = container.revision_directory()

        self.works = [
            db.work(title=f"Work {x}", with_open_access_download=True)
            for x in range(10)
        ]
        end_to_end_search_fixture.populate_search_index()

        self.old_index = revision_directory.highest().name_for_index(
            self.service.base_revision_name
        )

        self.new_revision = MockSearchSchemaRevisionLatest(1010101)
        self.new_index = self.new_revision.name_for_index(
            self.service.base_revision_name
        )
        available = dict(revision_directory.available)
        available[self.new_revision.version] = self.new_revision
        revision_directory._available = available

        InstanceInitializationScript.create_search_index(
            self.service, self.new_revision
        )

    def assert_pointers(self, read: str, write: str) -> None:
        read_pointer = self.service.read_pointer()
        write_pointer = self.service.write_pointer()
        assert read_pointer is not None and read_pointer.index == read
        assert write_pointer is not None and write_pointer.index == write


@pytest.fixture
def search_migration_fixture(
    db: DatabaseTransactionFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    return SearchMigrationFixture(db, end_to_end_search_fixture)


@patch("palace.manager.celery.tasks.search.random.uniform")
def test_search_reindex_advances_read_pointer(
    mock_random_uniform: MagicMock,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    fixture = search_migration_fixture
    mock_random_uniform.return_value = 0.0

    # Creating the new index moves the write pointer but leaves reads on the old index.
    fixture.assert_pointers(read=fixture.old_index, write=fixture.new_index)

    # A real migration always takes more than one batch, so use a batch size that makes
    # the run requeue itself: the index it is filling has to survive the requeues.
    search_reindex.delay(batch_size=3).wait()

    # Having filled the new index end to end, the reindex publishes it. Nothing had to
    # chain a second task on to do it.
    fixture.assert_pointers(read=fixture.new_index, write=fixture.new_index)

    fixture.client.indices.refresh()
    end_to_end_search_fixture.expect_results(fixture.works, "", ordered=False)


def test_search_reindex_does_not_advance_read_pointer_for_another_index(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
):
    fixture = search_migration_fixture

    # A run that started before the new revision existed has been filling the old index,
    # so the new one never received a complete pass and must not be published.
    search_reindex.delay(target_index=fixture.old_index).wait()

    fixture.assert_pointers(read=fixture.old_index, write=fixture.new_index)


@pytest.mark.parametrize("write_pointer_removed", [False, True])
def test_search_reindex_does_not_advance_read_pointer_when_the_write_pointer_moves(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
    write_pointer_removed: bool,
):
    fixture = search_migration_fixture
    started_on = fixture.service.write_pointer()

    # An instance running a newer revision deploys partway through and aims writes at an
    # index this run never touched, so the works are split across two indexes and neither
    # got a complete pass.
    later_revision = MockSearchSchemaRevisionLatest(1010102)
    moved_to = (
        None
        if write_pointer_removed
        else SearchPointer(
            alias=fixture.service.write_pointer_name(),
            index=later_revision.name_for_index(fixture.service.base_revision_name),
            version=later_revision.version,
        )
    )

    # The run reads the write pointer once on the way in and once at the end.
    with patch.object(
        type(fixture.service), "write_pointer", autospec=True
    ) as write_pointer:
        write_pointer.side_effect = [started_on, moved_to]
        search_reindex.delay().wait()

    assert write_pointer.call_count == 2
    fixture.assert_pointers(read=fixture.old_index, write=fixture.new_index)


@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_search_reindex_retries_when_the_write_pointer_cannot_be_read(
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
):
    fixture = search_migration_fixture
    mock_backoff.return_value = 0
    started_on = fixture.service.write_pointer()

    # The run can't tell which index it is filling until it reads the write pointer, so a
    # transient failure there is retried rather than failing the run.
    with patch.object(
        type(fixture.service), "write_pointer", autospec=True
    ) as write_pointer:
        write_pointer.side_effect = [OpenSearchException(), started_on, started_on]
        search_reindex.delay().wait()

    fixture.assert_pointers(read=fixture.new_index, write=fixture.new_index)


@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_search_reindex_retries_when_the_pointers_cannot_be_read(
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
):
    fixture = search_migration_fixture
    mock_backoff.return_value = 0
    current_read_pointer = fixture.service.read_pointer()

    # Reading the pointers is the last thing a pass does, after days of work on a large
    # collection, so a transient failure there is retried rather than losing the run.
    with patch.object(
        type(fixture.service), "read_pointer", autospec=True
    ) as read_pointer:
        read_pointer.side_effect = [OpenSearchException(), current_read_pointer]
        search_reindex.delay().wait()

    fixture.assert_pointers(read=fixture.new_index, write=fixture.new_index)


@patch("palace.manager.celery.tasks.search.random.uniform")
@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_search_reindex_does_not_advance_read_pointer_when_it_fails(
    mock_backoff: MagicMock,
    mock_random_uniform: MagicMock,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
):
    fixture = search_migration_fixture
    mock_backoff.return_value = 0
    mock_random_uniform.return_value = 0

    # The first batch lands, then the second fails through every retry, so the reindex
    # dies with the new index partly filled. Reads stay where they were rather than
    # moving to an index missing half its works.
    with patch.object(
        ExternalSearchIndex, "add_documents", autospec=True
    ) as add_documents:
        add_documents.side_effect = [None, *([OpenSearchException()] * 5)]
        with pytest.raises(MaxRetriesExceededError):
            search_reindex.delay(batch_size=5).wait()

    assert add_documents.call_count == 6
    fixture.assert_pointers(read=fixture.old_index, write=fixture.new_index)


def test_search_reindex_does_not_advance_read_pointer_from_a_resumed_run(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    search_migration_fixture: SearchMigrationFixture,
):
    fixture = search_migration_fixture

    # A run started partway through never indexes the works before its offset, so it has
    # not filled the new index and must not publish it.
    search_reindex.delay(offset=5).wait()

    fixture.assert_pointers(read=fixture.old_index, write=fixture.new_index)


def test_search_reindex_leaves_current_read_pointer_alone(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    # Outside a migration the read pointer is already at the latest revision, so a
    # routine reindex has nothing to advance.
    service = end_to_end_search_fixture.external_search.service
    db.work(with_open_access_download=True)
    before = service.read_pointer()

    with patch.object(
        type(service), "read_pointer_set", autospec=True
    ) as read_pointer_set:
        search_reindex.delay().wait()

    read_pointer_set.assert_not_called()
    assert service.read_pointer() == before


class SearchIndexingFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture
        self.redis_client = redis_fixture.client

        task = MagicMock()
        task.request.root_id = "fake"
        task.name = "palace.manager.celery.tasks.search.search_indexing"
        self.lock = TaskLock(task, redis_client=self.redis_client)

        self.waiting = WaitingForIndexing(self.redis_client)
        self.mock_works = {w_id for w_id in range(10)}

    def add_works(self):
        for work in self.mock_works:
            self.waiting.add(work)


@pytest.fixture
def search_indexing_fixture(redis_fixture: RedisFixture) -> SearchIndexingFixture:
    return SearchIndexingFixture(redis_fixture)


def test_search_indexing_lock(
    celery_fixture: CeleryFixture, search_indexing_fixture: SearchIndexingFixture
):
    search_indexing_fixture.lock.acquire()

    with pytest.raises(LockNotAcquired):
        search_indexing.delay().wait()


@pytest.mark.parametrize("batch_size", [3, 5, 500])
@patch("palace.manager.celery.tasks.search.random")
@patch("palace.manager.celery.tasks.search.index_works")
def test_search_indexing(
    mock_index_works: MagicMock,
    mock_random: MagicMock,
    batch_size: int,
    celery_fixture: CeleryFixture,
    search_indexing_fixture: SearchIndexingFixture,
):
    # The cooldown countdown is honoured by the real Celery workers, so mock it
    # out to avoid adding several seconds of real wait between requeued batches.
    mock_random.uniform.return_value = 0.0
    # No works to index, so we should not call index_works
    search_indexing.delay(batch_size=batch_size).wait()
    assert search_indexing_fixture.lock.locked() is False
    mock_index_works.delay.assert_not_called()

    # Add some works to the waiting list and run the task
    mock_index_works.reset_mock()
    search_indexing_fixture.add_works()
    search_indexing.delay(batch_size=batch_size).wait()

    # Lock is released
    assert search_indexing_fixture.lock.locked() is False

    # Index works was called the correct number of times
    assert mock_index_works.delay.call_count == math.ceil(
        len(search_indexing_fixture.mock_works) / batch_size
    )

    # All works were indexed
    indexed_works = []
    for idx, call_args in enumerate(mock_index_works.delay.call_args_list):
        indexed_works_for_call = call_args.kwargs["works"]
        if idx < len(mock_index_works.delay.call_args_list) - 1:
            assert len(indexed_works_for_call) == batch_size
        else:
            assert len(indexed_works_for_call) <= batch_size
        indexed_works.extend(indexed_works_for_call)

    # No work was indexed more than once
    assert all(count == 1 for count in Counter(indexed_works).values())
    assert set(indexed_works) == search_indexing_fixture.mock_works

    # No works are left in the waiting list
    assert search_indexing_fixture.waiting.get(10) == []


@patch("palace.manager.celery.tasks.search.random")
@patch("palace.manager.celery.tasks.search.index_works")
def test_search_indexing_cooldown_between_batches(
    mock_index_works: MagicMock,
    mock_random: MagicMock,
    celery_fixture: CeleryFixture,
    search_indexing_fixture: SearchIndexingFixture,
):
    # Each full batch requeues itself after a cooldown so a large backlog drains
    # at a bounded rate instead of flooding the index. With 10 works and
    # batch_size 3 the task pops 3, 3, 3, 1 -- the three full batches each
    # schedule a cooldown; the final partial batch does not.
    mock_random.uniform.return_value = 0.0
    search_indexing_fixture.add_works()

    search_indexing.delay(batch_size=3).wait()

    # A cooldown was taken after each full batch
    assert mock_random.uniform.call_count == 3

    # The backlog still fully drained and the lock was released at the end.
    assert search_indexing_fixture.lock.locked() is False
    assert search_indexing_fixture.waiting.get(10) == []
    indexed = [
        work
        for call_args in mock_index_works.delay.call_args_list
        for work in call_args.kwargs["works"]
    ]
    assert set(indexed) == search_indexing_fixture.mock_works


def test_index_works(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    client = end_to_end_search_fixture.external_search.write_client
    index = end_to_end_search_fixture.external_search_index

    work1_id = db.work(with_open_access_download=True).id
    work2_id = db.work(with_open_access_download=True).id

    # The works are not in the search index.
    client.indices.refresh()
    results = index.query_works("")
    assert len(results) == 0

    # Index both works
    index_works.delay([work1_id, work2_id]).wait()
    client.indices.refresh()

    # Check that both works are in the search index.
    results = index.query_works("")
    assert {result.work_id for result in results} == {work1_id, work2_id}


@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_index_works_failures(
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
    db: DatabaseTransactionFixture,
):
    # Make sure our backoff function doesn't delay the test.
    mock_backoff.return_value = 0

    # If we fail to add documents, we should retry up to 4 times, then fail.
    work = db.work(with_open_access_download=True)
    add_document_mocks = services_fixture.search_index.add_documents
    add_document_mocks.side_effect = OpenSearchException()
    with pytest.raises(MaxRetriesExceededError):
        index_works.delay([work.id]).wait()
    assert add_document_mocks.call_count == 5

    add_document_mocks.reset_mock()
    add_document_mocks.side_effect = [OpenSearchException(), [work.id], None]
    index_works.delay([work.id]).wait()
    assert add_document_mocks.call_count == 3
