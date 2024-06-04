import json
from unittest.mock import MagicMock, call, patch

import pytest
from celery.exceptions import MaxRetriesExceededError
from opensearchpy import OpenSearchException

from palace.manager.celery.tasks.search import (
    get_migrate_search_chain,
    get_work_search_documents,
    index_work,
    search_reindex,
    update_read_pointer,
)
from palace.manager.scripts.initialization import InstanceInitializationScript
from palace.manager.search.external_search import Filter
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
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


def test_PP_1332_fiction_returns_results(
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
    default_filter = Filter()
    default_filter.search_type = "json"
    qs = {"query": {"key": "fiction", "value": "fiction"}}
    end_to_end_search_fixture.expect_results(
        expect=[work1],
        ordered=False,
        filter=default_filter,
        query_string=json.dumps(qs),
    )
    qs["query"]["value"] = "nonfiction"
    end_to_end_search_fixture.expect_results(
        expect=[work2],
        ordered=False,
        filter=default_filter,
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


def test_index_work(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    end_to_end_search_fixture: EndToEndSearchFixture,
) -> None:
    client = end_to_end_search_fixture.external_search.client
    index = end_to_end_search_fixture.external_search_index

    work1_id = db.work(with_open_access_download=True).id
    work2_id = db.work(with_open_access_download=True).id

    # The works are not in the search index.
    client.indices.refresh()
    results = index.query_works("")
    assert len(results) == 0

    # Index work2
    index_work.delay(work2_id).wait()
    client.indices.refresh()

    # Check that it made it into the search index.
    [result] = index.query_works("")
    assert result.work_id == work2_id

    # Index work1
    index_work.delay(work1_id).wait()
    client.indices.refresh()

    # Check that both works are in the search index.
    results = index.query_works("")
    assert {result.work_id for result in results} == {work1_id, work2_id}


@patch("palace.manager.celery.tasks.search.exponential_backoff")
def test_index_work_failures(
    mock_backoff: MagicMock,
    celery_fixture: CeleryFixture,
    services_fixture: ServicesFixture,
    caplog: pytest.LogCaptureFixture,
    db: DatabaseTransactionFixture,
):
    # Make sure our backoff function doesn't delay the test.
    mock_backoff.return_value = 0

    # If we try to index a work that doesn't exist, we retry up to 4 times, then fail.
    with pytest.raises(MaxRetriesExceededError):
        index_work.delay(555).wait()
    assert "Work 555 not found" in caplog.text

    # If we fail to add documents, we should retry up to 4 times, then fail.
    work = db.work(with_open_access_download=True)
    add_document_mock = services_fixture.search_fixture.index_mock.add_document
    add_document_mock.side_effect = OpenSearchException()
    with pytest.raises(MaxRetriesExceededError):
        index_work.delay(work.id).wait()
    assert add_document_mock.call_count == 5

    add_document_mock.reset_mock()
    add_document_mock.side_effect = [OpenSearchException(), None]
    index_work.delay(work.id).wait()
    assert add_document_mock.call_count == 2


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
