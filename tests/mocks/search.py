from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from enum import Enum
from typing import Any
from unittest.mock import MagicMock

from opensearchpy import MultiSearch, OpenSearchException, Search
from opensearchpy.helpers.response.hit import Hit

from palace.manager.search.document import SearchMappingDocument
from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.service import (
    SearchPointer,
    SearchService,
    SearchServiceFailedDocument,
)
from palace.manager.search.v5 import SearchV5
from palace.manager.sqlalchemy.model.work import Work


class SearchServiceFailureMode(Enum):
    """The simulated failure modes for the search service."""

    NOT_FAILING = 0
    FAIL_INDEXING_DOCUMENTS = 1
    FAIL_INDEXING_DOCUMENTS_TIMEOUT = 3
    FAIL_ENTIRELY = 2


class SearchServiceFake(SearchService):
    """A search service that doesn't speak to a real service."""

    def __init__(self):
        self.base_name = "test_index"
        self._failing = SearchServiceFailureMode.NOT_FAILING
        self._documents_by_index = defaultdict(list)
        self._read_pointer: SearchPointer | None = None
        self._write_pointer: SearchPointer | None = None
        self._search_client = Search(using=MagicMock())
        self._multi_search_client = MultiSearch(using=MagicMock())
        self._document_submission_attempts = []

    @property
    def base_revision_name(self) -> str:
        return self.base_name

    @property
    def document_submission_attempts(self) -> list[dict]:
        return self._document_submission_attempts

    def _fail_if_necessary(self):
        if self._failing == SearchServiceFailureMode.FAIL_ENTIRELY:
            raise OpenSearchException("Search index is on fire.")

    def set_failing_mode(self, mode: SearchServiceFailureMode):
        self._failing = mode

    def documents_for_index(self, index_name: str) -> list[dict]:
        self._fail_if_necessary()

        return self._documents_by_index[index_name]

    def documents_all(self) -> list[dict]:
        self._fail_if_necessary()

        results: list[dict] = []
        for documents in self._documents_by_index.values():
            for document in documents:
                results.append(document)

        return results

    def refresh(self):
        self._fail_if_necessary()
        return

    def read_pointer_name(self) -> str:
        self._fail_if_necessary()
        return f"{self.base_name}-search-read"

    def write_pointer_name(self) -> str:
        self._fail_if_necessary()
        return f"{self.base_name}-search-write"

    def read_pointer(self) -> SearchPointer | None:
        self._fail_if_necessary()
        return self._read_pointer

    def write_pointer(self) -> SearchPointer | None:
        self._fail_if_necessary()
        return self._write_pointer

    def read_pointer_set(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._read_pointer = self._pointer_set(revision, self.read_pointer_name())

    def index_create(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        return None

    def index_set_mapping(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()

    def index_submit_document(
        self, document: dict[str, Any], refresh: bool = False
    ) -> None:
        self.index_submit_documents([document])

    def index_submit_documents(
        self, documents: Iterable[dict]
    ) -> list[SearchServiceFailedDocument]:
        self._fail_if_necessary()

        pointer = self.write_pointer_name()
        _should_fail = False
        _should_fail = (
            _should_fail
            or self._failing == SearchServiceFailureMode.FAIL_INDEXING_DOCUMENTS
        )
        _should_fail = (
            _should_fail
            or self._failing == SearchServiceFailureMode.FAIL_INDEXING_DOCUMENTS_TIMEOUT
        )

        if _should_fail:
            results: list[SearchServiceFailedDocument] = []
            for document in documents:
                self._document_submission_attempts.append(document)
                if self._failing == SearchServiceFailureMode.FAIL_INDEXING_DOCUMENTS:
                    _error = SearchServiceFailedDocument(
                        document["_id"],
                        error_message="There was an error!",
                        error_status=500,
                        error_exception="Exception",
                    )
                else:
                    _error = SearchServiceFailedDocument(
                        document["_id"],
                        error_message="Connection Timeout!",
                        error_status=0,
                        error_exception="ConnectionTimeout",
                    )
                results.append(_error)

            return results

        for document in documents:
            self._documents_by_index[pointer].append(document)

        return []

    def _pointer_set(self, revision: SearchSchemaRevision, alias: str) -> SearchPointer:
        return SearchPointer(
            alias=alias,
            index=revision.name_for_index(self.base_name),
            version=revision.version,
        )

    def write_pointer_set(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._write_pointer = self._pointer_set(revision, self.write_pointer_name())

    def index_clear_documents(self):
        self._fail_if_necessary()
        pointer = self.write_pointer_name()
        self._documents_by_index[pointer].clear()

    def read_search_client(self) -> Search:
        return self._search_client.index(self.read_pointer_name())

    def read_search_multi_client(self) -> MultiSearch:
        return self._multi_search_client.index(self.read_pointer_name())

    def index_remove_document(self, doc_id: int):
        self._fail_if_necessary()
        pointer = self.write_pointer_name()
        items = self._documents_by_index[pointer]
        to_remove = [item for item in items if item.get("_id") == doc_id]
        for item in to_remove:
            items.remove(item)


def fake_hits(works: list[Work]):
    return [
        Hit(
            {
                "_source": {"work_id": work.id},
                "_sort": [work.sort_title, work.sort_author, work.id],
            }
        )
        for work in works
    ]


class ExternalSearchIndexFake(ExternalSearchIndex):
    """A fake search index, to be used where we do not care what the search does, just that the results match what we expect
    Eg. Testing a Feed object doesn't need to test the search index, it just needs the search index to report correctly
    """

    def __init__(
        self,
    ):
        super().__init__(
            service=SearchServiceFake(),
        )

        self._mock_multi_works: list[dict] = []
        self._mock_count_works = 0
        self._queries: list[tuple] = []

    def mock_query_works(self, works: list[Work]):
        self.mock_query_works_multi(works)

    def mock_query_works_multi(self, works: list[Work], *args: list[Work]):
        self._mock_multi_works = [fake_hits(works)]
        self._mock_multi_works.extend([fake_hits(arg_works) for arg_works in args])

    def query_works_multi(self, queries, debug=False):
        result = []
        for ix, (query_string, filter, pagination) in enumerate(queries):
            self._queries.append((query_string, filter, pagination))
            this_result = []
            if not self._mock_multi_works:
                pagination.page_loaded([])
            # Mock Pagination
            elif len(self._mock_multi_works) > ix:
                this_result = self._mock_multi_works[ix]

                # sortkey pagination, if it exists
                # Sorting must be done by the test case
                if getattr(pagination, "last_item_on_previous_page", None):
                    for ix, hit in enumerate(this_result):
                        if hit.meta["sort"] == pagination.last_item_on_previous_page:
                            this_result = this_result[ix + 1 : ix + 1 + pagination.size]
                            break
                    else:
                        this_result = []
                else:
                    # Else just assume offset pagination
                    this_result = this_result[
                        pagination.offset : pagination.offset + pagination.size
                    ]

                pagination.page_loaded(this_result)
                result.append(this_result)
            else:
                # Catch all
                pagination.page_loaded([])

        return result

    def mock_count_works(self, count):
        self._mock_count_works = count

    def count_works(self, filter):
        """So far this is not required in the tests"""
        return self._mock_count_works

    def __repr__(self) -> str:
        return f"Expected Results({id(self)}): {self._mock_multi_works}"


class MockSearchSchemaRevision(SearchSchemaRevision):
    def __init__(self, version: int) -> None:
        super().__init__()
        self._version = version
        self._document = SearchMappingDocument()

    @property
    def version(self) -> int:
        return self._version

    def mapping_document(self) -> SearchMappingDocument:
        return self._document


class MockSearchSchemaRevisionLatest(MockSearchSchemaRevision):
    def __init__(self, version: int) -> None:
        super().__init__(version)
        self._document = SearchV5().mapping_document()
