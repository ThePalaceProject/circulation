import json
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
from palace.manager.search.external_search import Filter
from palace.manager.service.redis.models.lock import TaskLock
from palace.manager.service.redis.models.search import WaitingForIndexing
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.search import EndToEndSearchFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.search import MockSearchSchemaRevisionLatest


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


def test_search_reindex(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
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

    # Index the works, use a small batch size to test the pagination.
    search_reindex.delay(batch_size=2).wait()
    client.indices.refresh()

    # Check that the works are in the search index.
    end_to_end_search_fixture.expect_results([work1, work2, work4], "", ordered=False)

    # Remove work1 from the search index.
    index.remove_work(work1)
    client.indices.refresh()
    end_to_end_search_fixture.expect_results([work2, work4], "", ordered=False)

    # Reindex the works.
    search_reindex.delay().wait()
    client.indices.refresh()

    # Check that all the works are in the search index.
    end_to_end_search_fixture.expect_results([work1, work2, work4], "", ordered=False)


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
    services_fixture: ServicesFixture,
):
    # Make sure our backoff function doesn't delay the test.
    mock_backoff.return_value = 0

    add_documents_mock = services_fixture.search_fixture.index_mock.add_documents

    # If we fail to add documents, we should retry up to 4 times, then fail.
    add_documents_mock.return_value = [1, 2, 3]
    with pytest.raises(MaxRetriesExceededError):
        search_reindex.delay().wait()
    assert add_documents_mock.call_count == 5
    mock_backoff.assert_has_calls([call(0), call(1), call(2), call(3), call(4)])

    add_documents_mock.reset_mock()
    add_documents_mock.side_effect = [[1, 2, 3], OpenSearchException(), None]
    search_reindex.delay().wait()
    assert add_documents_mock.call_count == 3

    # Unknown exception, we don't retry
    add_documents_mock.reset_mock()
    add_documents_mock.side_effect = Exception()
    with pytest.raises(Exception):
        search_reindex.delay().wait()
    assert add_documents_mock.call_count == 1


@patch("palace.manager.celery.tasks.search.exponential_backoff")
@patch("palace.manager.celery.tasks.search.get_work_search_documents")
def test_search_reindex_failures_multiple_batch(
    mock_get_work_search_documents: MagicMock,
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
):
    # When a batch succeeds, the retry count is reset.
    mock_backoff.return_value = 0
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
    add_documents_mock = services_fixture.search_fixture.index_mock.add_documents
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

    read_pointer_set_mock = (
        services_fixture.search_fixture.service_mock.read_pointer_set
    )
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
    end_to_end_search_fixture: EndToEndSearchFixture,
):
    client = end_to_end_search_fixture.external_search.client
    service = end_to_end_search_fixture.external_search.service
    revision = end_to_end_search_fixture.external_search.revision
    services = end_to_end_search_fixture.external_search.services_container
    revision_directory = services.search.revision_directory()

    works = [
        db.work(title=f"Work {x}", with_open_access_download=True) for x in range(10)
    ]

    end_to_end_search_fixture.populate_search_index()
    new_revision = MockSearchSchemaRevisionLatest(1010101)
    new_revision_index_name = new_revision.name_for_index(service.base_revision_name)
    revision_directory.available[new_revision.version] = new_revision

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

    # Run the migration task
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
        self.lock = TaskLock(self.redis_client, task)

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

    with pytest.raises(BasePalaceException) as exc_info:
        search_indexing.delay().wait()

    assert "search_indexing is already running." in str(exc_info.value)


@patch("palace.manager.celery.tasks.search.index_works")
def test_search_indexing(
    mock_index_works: MagicMock,
    celery_fixture: CeleryFixture,
    search_indexing_fixture: SearchIndexingFixture,
):
    # No works to index, so we should not call index_works
    search_indexing.delay().wait()
    assert search_indexing_fixture.lock.locked() is False
    mock_index_works.delay.assert_not_called()

    # Add some works to the waiting list and run the task
    search_indexing_fixture.add_works()
    search_indexing.delay().wait()
    assert search_indexing_fixture.lock.locked() is False
    assert mock_index_works.delay.call_count == 1
    assert (
        set(mock_index_works.delay.call_args.kwargs["works"])
        == search_indexing_fixture.mock_works
    )
    assert search_indexing_fixture.waiting.get(10) == []

    # Add some works to the waiting list and run the task with a smaller batch size, to test that
    # we paginate through the works.
    mock_index_works.reset_mock()
    search_indexing_fixture.add_works()
    search_indexing.delay(batch_size=5).wait()
    assert search_indexing_fixture.lock.locked() is False
    assert mock_index_works.delay.call_count == 2
    for call_args in mock_index_works.delay.call_args_list:
        assert set(call_args.kwargs["works"]) <= search_indexing_fixture.mock_works
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
    add_document_mocks = services_fixture.search_fixture.index_mock.add_documents
    add_document_mocks.side_effect = OpenSearchException()
    with pytest.raises(MaxRetriesExceededError):
        index_works.delay([work.id]).wait()
    assert add_document_mocks.call_count == 5

    add_document_mocks.reset_mock()
    add_document_mocks.side_effect = [OpenSearchException(), [work.id], None]
    index_works.delay([work.id]).wait()
    assert add_document_mocks.call_count == 3
