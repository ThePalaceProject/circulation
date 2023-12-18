from __future__ import annotations

from collections.abc import Iterable
from enum import Enum
from unittest.mock import MagicMock

from opensearch_dsl import MultiSearch, Search
from opensearch_dsl.response.hit import Hit
from opensearchpy import OpenSearchException

from core.external_search import ExternalSearchIndex
from core.model import Work
from core.model.work import Work
from core.search.revision import SearchSchemaRevision
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import (
    SearchService,
    SearchServiceFailedDocument,
    SearchWritePointer,
)


class SearchServiceFailureMode(Enum):
    """The simulated failure modes for the search service."""

    NOT_FAILING = 0
    FAIL_INDEXING_DOCUMENTS = 1
    FAIL_INDEXING_DOCUMENTS_TIMEOUT = 3
    FAIL_ENTIRELY = 2


class SearchServiceFake(SearchService):
    """A search service that doesn't speak to a real service."""

    _documents_by_index: dict[str, list[dict]]
    _failing: SearchServiceFailureMode
    _search_client: Search
    _multi_search_client: MultiSearch
    _indexes_created: list[str]
    _document_submission_attempts: list[dict]

    def __init__(self):
        self.base_name = "test_index"
        self._failing = SearchServiceFailureMode.NOT_FAILING
        self._documents_by_index = {}
        self._read_pointer: str | None = None
        self._write_pointer: SearchWritePointer | None = None
        self._search_client = Search(using=MagicMock())
        self._multi_search_client = MultiSearch(using=MagicMock())
        self._indexes_created = []
        self._document_submission_attempts = []

    @property
    def document_submission_attempts(self) -> list[dict]:
        return self._document_submission_attempts

    def indexes_created(self) -> list[str]:
        return self._indexes_created

    def _fail_if_necessary(self):
        if self._failing == SearchServiceFailureMode.FAIL_ENTIRELY:
            raise OpenSearchException("Search index is on fire.")

    def set_failing_mode(self, mode: SearchServiceFailureMode):
        self._failing = mode

    def documents_for_index(self, index_name: str) -> list[dict]:
        self._fail_if_necessary()

        if not (index_name in self._documents_by_index):
            return []
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

    def read_pointer(self) -> str | None:
        self._fail_if_necessary()
        return self._read_pointer

    def write_pointer(self) -> SearchWritePointer | None:
        self._fail_if_necessary()
        return self._write_pointer

    def create_empty_index(self) -> None:
        self._fail_if_necessary()
        self._indexes_created.append(f"{self.base_name}-empty")
        return None

    def read_pointer_set(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._read_pointer = f"{revision.name_for_indexed_pointer(self.base_name)}"

    def index_set_populated(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()

    def read_pointer_set_empty(self) -> None:
        self._fail_if_necessary()
        self._read_pointer = f"{self.base_name}-empty"

    def index_create(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._indexes_created.append(revision.name_for_index(self.base_name))
        return None

    def index_is_populated(self, revision: SearchSchemaRevision) -> bool:
        self._fail_if_necessary()
        return True

    def index_set_mapping(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()

    def index_submit_documents(
        self, pointer: str, documents: Iterable[dict]
    ) -> list[SearchServiceFailedDocument]:
        self._fail_if_necessary()

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

        if not (pointer in self._documents_by_index):
            self._documents_by_index[pointer] = []

        for document in documents:
            self._documents_by_index[pointer].append(document)

        return []

    def write_pointer_set(self, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._write_pointer = SearchWritePointer(self.base_name, revision.version)

    def index_clear_documents(self, pointer: str):
        self._fail_if_necessary()
        if pointer in self._documents_by_index:
            self._documents_by_index[pointer] = []

    def search_client(self, write=False) -> Search:
        return self._search_client.index(
            self.read_pointer_name() if not write else self.write_pointer_name()
        )

    def search_multi_client(self, write=False) -> MultiSearch:
        return self._multi_search_client.index(
            self.read_pointer_name() if not write else self.write_pointer_name()
        )

    def index_remove_document(self, pointer: str, id: int):
        self._fail_if_necessary()
        if pointer in self._documents_by_index:
            items = self._documents_by_index[pointer]
            to_remove = []
            for item in items:
                if item.get("_id") == id:
                    to_remove.append(item)
            for item in to_remove:
                items.remove(item)

    def is_pointer_empty(*args):
        return False


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
        _db,
        url: str | None = None,
        test_search_term: str | None = None,
        revision_directory: SearchRevisionDirectory | None = None,
        version: int | None = None,
    ):
        super().__init__(
            _db, url, test_search_term, revision_directory, version, SearchServiceFake()
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
