import json
import math
from collections import Counter
from unittest.mock import MagicMock, call, patch

import pytest
from celery.exceptions import MaxRetriesExceededError
from opensearchpy import OpenSearchException

from palace.manager.celery.tasks.search import (
    get_migrate_search_chain,
    get_work_search_documents,
    index_works,
    search_indexing,
    search_reindex,
    update_read_pointer,
)
from palace.manager.core.exceptions import BasePalaceException
from palace.manager.scripts.initialization import InstanceInitializationScript
from palace.manager.search.filter import Filter
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
def test_search_reindex(
    batch_size: int,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
) -> None:
    client = end_to_end_search_fixture.external_search.client
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
    client = end_to_end_search_fixture.external_search.client
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


def test_get_migrate_search_chain(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    search_reindex_task_lock_fixture: SearchReindexTaskLockFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    container = end_to_end_search_fixture.external_search.search_container

    client = container.client()
    service = container.service()
    revision_directory = container.revision_directory()
    revision = revision_directory.highest()

    works = [
        db.work(title=f"Work {x}", with_open_access_download=True) for x in range(10)
    ]

    end_to_end_search_fixture.populate_search_index()
    new_revision = MockSearchSchemaRevisionLatest(1010101)
    new_revision_index_name = new_revision.name_for_index(service.base_revision_name)
    available = dict(revision_directory.available)
    available[new_revision.version] = new_revision
    revision_directory._available = available

    InstanceInitializationScript.create_search_index(service, new_revision)

    # The write pointer should point to the new revision
    write_pointer = service.write_pointer()
    assert write_pointer is not None
    assert write_pointer.index == new_revision_index_name
    assert write_pointer.version == new_revision.version

    # The read pointer should still point to the old revision
    read_pointer = service.read_pointer()
    assert read_pointer is not None
    assert read_pointer.index == revision.name_for_index(service.base_revision_name)
    assert read_pointer.version == revision.version

    # There is a lock on the search reindex task
    search_reindex_task_lock_fixture.task_lock.acquire()

    # Run the migration task
    with pytest.raises(BasePalaceException):
        get_migrate_search_chain().delay().wait()

    # The read pointer should still point to the old revision
    read_pointer = service.read_pointer()
    assert read_pointer is not None
    assert read_pointer.index == revision.name_for_index(service.base_revision_name)
    assert read_pointer.version == revision.version

    # Release the lock and try again
    search_reindex_task_lock_fixture.task_lock.release()
    get_migrate_search_chain().delay().wait()

    # The read pointer should now point to the new revision
    read_pointer = service.read_pointer()
    assert read_pointer is not None
    assert read_pointer.index == new_revision_index_name

    # And we should have all the works in the new index
    client.indices.refresh()
    end_to_end_search_fixture.expect_results(works, "", ordered=False)


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
@patch("palace.manager.celery.tasks.search.index_works")
def test_search_indexing(
    mock_index_works: MagicMock,
    batch_size: int,
    celery_fixture: CeleryFixture,
    search_indexing_fixture: SearchIndexingFixture,
):
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


def test_index_works(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    client = end_to_end_search_fixture.external_search.client
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
