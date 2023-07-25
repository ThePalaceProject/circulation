import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Optional

import opensearchpy.helpers
from opensearch_dsl import MultiSearch, Search
from opensearchpy import NotFoundError, OpenSearch, RequestError

from core.search.revision import SearchSchemaRevision


@dataclass
class SearchWritePointer:
    """The 'write' pointer; the pointer that will be used to populate an index with search documents."""

    base_name: str
    version: int

    @property
    def name(self) -> str:
        return f"{self.base_name}-search-write"

    @property
    def target_name(self) -> str:
        return f"{self.base_name}-v{self.version}"


class SearchServiceException(Exception):
    """The type of exceptions raised by the search service."""

    def __init__(self, message: str):
        super().__init__(message)


@dataclass
class SearchServiceFailedDocument:
    """An error indicating that a document failed to index."""

    id: int
    error_message: str
    error_status: int
    error_exception: str

    @classmethod
    def from_bulk_error(cls, error: dict):
        """Transform an error dictionary returned from opensearchpy's bulk API to a typed error"""
        if error.get("index"):
            error_indexed = error["index"]
            error_id = int(error_indexed["_id"])
            error_status = error_indexed["status"]
            error_reason = error_indexed["error"]["reason"]
            return SearchServiceFailedDocument(
                id=error_id,
                error_message=error_reason,
                error_status=error_status,
                error_exception="<unavailable>",
            )
        else:
            # Not exactly ideal, but we really have no idea what the bulk API can return.
            return SearchServiceFailedDocument(
                id=-1,
                error_message="Unrecognized error returned from Opensearch bulk API.",
                error_status=-1,
                error_exception=f"{error}",
            )


class SearchService(ABC):
    """The interface we need from services like Opensearch. Essentially, it provides the operations we want with
    sensible types, rather than the untyped pile of JSON the actual search client provides."""

    @abstractmethod
    def read_pointer_name(self, base_name: str) -> str:
        """Get the name used for the read pointer."""

    @abstractmethod
    def write_pointer_name(self, base_name: str) -> str:
        """Get the name used for the write pointer."""

    @abstractmethod
    def read_pointer(self, base_name: str) -> Optional[str]:
        """Get the read pointer, if it exists."""

    @abstractmethod
    def write_pointer(self, base_name: str) -> Optional[SearchWritePointer]:
        """Get the writer pointer, if it exists."""

    @abstractmethod
    def create_empty_index(self, base_name: str) -> None:
        """Atomically create the empty index for the given base name."""

    @abstractmethod
    def read_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        """Atomically set the read pointer to the index for the given revision and base name."""

    @abstractmethod
    def read_pointer_set_empty(self, base_name: str) -> None:
        """Atomically set the read pointer to the empty index for the base name."""

    @abstractmethod
    def index_create(self, base_name: str, revision: SearchSchemaRevision) -> None:
        """Atomically create an index for the given base name and revision."""

    @abstractmethod
    def indexes_created(self) -> List[str]:
        """A log of all the indexes that have been created by this client service."""

    @abstractmethod
    def index_is_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> bool:
        """Return True if the index for the given base name and revision has been populated."""

    @abstractmethod
    def index_set_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> None:
        """Set an index as populated."""

    @abstractmethod
    def index_set_mapping(self, base_name: str, revision: SearchSchemaRevision) -> None:
        """Set the schema mappings for the given index."""

    @abstractmethod
    def index_submit_documents(
        self,
        pointer: str,
        documents: Iterable[dict],
    ) -> List[SearchServiceFailedDocument]:
        """Submit search documents to the given index."""

    @abstractmethod
    def write_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        """Atomically set the write pointer to the index for the given revision and base name."""

    @abstractmethod
    def refresh(self):
        """Synchronously refresh the service and wait for changes to be completed."""

    @abstractmethod
    def index_clear_documents(self, pointer: str):
        """Clear all search documents in the given index."""

    @abstractmethod
    def search_client(self) -> Search:
        """Return the underlying search client."""

    @abstractmethod
    def search_multi_client(self) -> MultiSearch:
        """Return the underlying search client."""

    @abstractmethod
    def index_remove_document(self, pointer: str, id: int):
        """Remove a specific document from the given index."""


class SearchServiceOpensearch1(SearchService):
    """The real Opensearch 1.x service."""

    def __init__(self, client: OpenSearch):
        self._logger = logging.getLogger(SearchServiceOpensearch1.__name__)
        self._client = client
        self._search = Search(using=self._client)
        self._multi_search = MultiSearch(using=self._client)
        self._indexes_created: List[str] = []

        # Documents are not allowed to automatically create indexes.
        self._client.cluster.put_settings(
            body={"persistent": {"action": {"auto_create_index": "false"}}}
        )

    def indexes_created(self) -> List[str]:
        return self._indexes_created

    def write_pointer(self, base_name: str) -> Optional[SearchWritePointer]:
        try:
            result: dict = self._client.indices.get_alias(
                name=self.write_pointer_name(base_name)
            )
            for name in result.keys():
                match = re.search(f"{base_name}-v([0-9]+)", string=name)
                if match:
                    return SearchWritePointer(base_name, int(match.group(1)))
            return None
        except NotFoundError:
            return None

    def create_empty_index(self, base_name: str) -> None:
        try:
            index_name = self._empty(base_name)
            self._logger.debug(f"creating empty index {index_name}")
            self._client.indices.create(index=index_name)
            self._indexes_created.append(index_name)
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                return
            raise e

    def read_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        alias_name = self.read_pointer_name(base_name)
        target_index = revision.name_for_index(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._logger.debug(f"setting read pointer {alias_name} to index {target_index}")
        self._client.indices.update_aliases(body=action)

    def index_set_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> None:
        alias_name = revision.name_for_indexed_pointer(base_name)
        target_index = revision.name_for_index(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._logger.debug(
            f"creating 'indexed' flag alias {alias_name} for index {target_index}"
        )
        self._client.indices.update_aliases(body=action)

    def read_pointer_set_empty(self, base_name: str) -> None:
        alias_name = self.read_pointer_name(base_name)
        target_index = self._empty(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._logger.debug(
            f"setting read pointer {alias_name} to empty index {target_index}"
        )
        self._client.indices.update_aliases(body=action)

    def index_create(self, base_name: str, revision: SearchSchemaRevision) -> None:
        try:
            index_name = revision.name_for_index(base_name)
            self._logger.debug(f"creating index {index_name}")
            self._client.indices.create(
                index=index_name,
                body=revision.mapping_document().serialize(),
            )
            self._indexes_created.append(index_name)
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                return
            raise e

    def index_is_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> bool:
        return self._client.indices.exists_alias(
            name=revision.name_for_indexed_pointer(base_name)
        )

    def index_set_mapping(self, base_name: str, revision: SearchSchemaRevision) -> None:
        data = {"properties": revision.mapping_document().serialize_properties()}
        index_name = revision.name_for_index(base_name)
        self._logger.debug(f"setting mappings for index {index_name}")
        self._client.indices.put_mapping(index=index_name, body=data)

    def index_submit_documents(
        self, pointer: str, documents: Iterable[dict]
    ) -> List[SearchServiceFailedDocument]:
        self._logger.info(f"submitting documents to index {pointer}")

        # Specifically override the target in all documents to the target pointer
        # Add a hard requirement that the target be an alias (this prevents documents from implicitly creating
        # indexes).
        for document in documents:
            document["_index"] = pointer
            document["_require_alias"] = True

        # See: Sources for "streaming_bulk":
        # https://github.com/opensearch-project/opensearch-py/blob/db972e615b9156b4e364091d6a893d64fb3ef4f3/opensearchpy/helpers/actions.py#L267
        # The documentation is incredibly vague about what the function actually returns, but these
        # parameters _should_ cause it to return a tuple containing the number of successfully indexed documents
        # and a list of documents that failed. Unfortunately, the type checker disagrees and the documentation
        # gives no hint as to what an "int" might mean for errors.
        (success_count, errors) = opensearchpy.helpers.bulk(
            client=self._client,
            actions=documents,
            raise_on_error=False,
            max_retries=3,
            max_backoff=30,
            yield_ok=False,
        )

        error_results: List[SearchServiceFailedDocument] = []
        if isinstance(errors, list):
            for error in errors:
                error_results.append(SearchServiceFailedDocument.from_bulk_error(error))
        else:
            raise SearchServiceException(
                f"Opensearch returned {errors} instead of a list of errors."
            )

        return error_results

    def index_clear_documents(self, pointer: str):
        self._client.delete_by_query(
            index=pointer, body={"query": {"match_all": {}}}, wait_for_completion=True
        )

    def refresh(self):
        self._logger.debug(f"waiting for indexes to become ready")
        self._client.indices.refresh()

    def write_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        alias_name = self.write_pointer_name(base_name)
        target_index = revision.name_for_index(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._logger.debug(f"setting write pointer {alias_name} to {target_index}")
        self._client.indices.update_aliases(body=action)

    def read_pointer(self, base_name: str) -> Optional[str]:
        try:
            result: dict = self._client.indices.get_alias(
                name=self.read_pointer_name(base_name)
            )
            for name in result.keys():
                if name.startswith(f"{base_name}-"):
                    return name
            return None
        except NotFoundError:
            return None

    def search_client(self, pointer_name: str) -> Search:
        return self._search.index(pointer_name)

    def search_multi_client(self, pointer_name: str) -> MultiSearch:
        return self._multi_search.index(pointer_name)

    def read_pointer_name(self, base_name: str) -> str:
        return f"{base_name}-search-read"

    def write_pointer_name(self, base_name: str) -> str:
        return f"{base_name}-search-write"

    @staticmethod
    def _empty(base_name):
        return f"{base_name}-empty"

    def index_remove_document(self, pointer: str, id: int):
        self._client.delete(index=pointer, id=id, doc_type="_doc")
