from enum import Enum
from typing import Dict, Iterable, List, Optional
from unittest.mock import MagicMock

from opensearch_dsl import MultiSearch, Search
from opensearchpy import OpenSearchException

from core.search.revision import SearchSchemaRevision
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

    _documents_by_index: Dict[str, List[dict]]
    _failing: SearchServiceFailureMode
    _search_client: Search
    _multi_search_client: MultiSearch
    _indexes_created: List[str]
    _document_submission_attempts: List[dict]

    def __init__(self):
        self._failing = SearchServiceFailureMode.NOT_FAILING
        self._documents_by_index = {}
        self._read_pointer: Optional[str] = None
        self._write_pointer: Optional[SearchWritePointer] = None
        self._search_client = Search(using=MagicMock())
        self._multi_search_client = MultiSearch(using=MagicMock())
        self._indexes_created = []
        self._document_submission_attempts = []

    @property
    def document_submission_attempts(self) -> List[dict]:
        return self._document_submission_attempts

    def indexes_created(self) -> List[str]:
        return self._indexes_created

    def _fail_if_necessary(self):
        if self._failing == SearchServiceFailureMode.FAIL_ENTIRELY:
            raise OpenSearchException("Search index is on fire.")

    def set_failing_mode(self, mode: SearchServiceFailureMode):
        self._failing = mode

    def documents_for_index(self, index_name: str) -> List[dict]:
        self._fail_if_necessary()

        if not (index_name in self._documents_by_index):
            return []
        return self._documents_by_index[index_name]

    def documents_all(self) -> List[dict]:
        self._fail_if_necessary()

        results: List[dict] = []
        for documents in self._documents_by_index.values():
            for document in documents:
                results.append(document)

        return results

    def refresh(self):
        self._fail_if_necessary()
        return

    def read_pointer_name(self, base_name: str) -> str:
        self._fail_if_necessary()
        return f"{base_name}-search-read"

    def write_pointer_name(self, base_name: str) -> str:
        self._fail_if_necessary()
        return f"{base_name}-search-write"

    def read_pointer(self, base_name: str) -> Optional[str]:
        self._fail_if_necessary()
        return self._read_pointer

    def write_pointer(self, base_name: str) -> Optional[SearchWritePointer]:
        self._fail_if_necessary()
        return self._write_pointer

    def create_empty_index(self, base_name: str) -> None:
        self._fail_if_necessary()
        self._indexes_created.append(f"{base_name}-empty")
        return None

    def read_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._read_pointer = f"{revision.name_for_indexed_pointer(base_name)}"

    def index_set_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> None:
        self._fail_if_necessary()

    def read_pointer_set_empty(self, base_name: str) -> None:
        self._fail_if_necessary()
        self._read_pointer = f"{base_name}-empty"

    def index_create(self, base_name: str, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._indexes_created.append(revision.name_for_index(base_name))
        return None

    def index_is_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> bool:
        self._fail_if_necessary()
        return True

    def index_set_mapping(self, base_name: str, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()

    def index_submit_documents(
        self, pointer: str, documents: Iterable[dict]
    ) -> List[SearchServiceFailedDocument]:
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
            results: List[SearchServiceFailedDocument] = []
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

    def write_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        self._fail_if_necessary()
        self._write_pointer = SearchWritePointer(base_name, revision.version)

    def index_clear_documents(self, pointer: str):
        self._fail_if_necessary()
        if pointer in self._documents_by_index:
            self._documents_by_index[pointer] = []

    def search_client(self) -> Search:
        return self._search_client

    def search_multi_client(self) -> MultiSearch:
        return self._multi_search_client

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
