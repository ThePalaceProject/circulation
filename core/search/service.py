import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterable, List, Optional

import opensearchpy.helpers
from opensearch_dsl import Search
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
    def index_is_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> bool:
        """Return True if the index for the given base name and revision has been populated."""

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


class SearchServiceOpensearch1(SearchService):
    """The real Opensearch 1.x service."""

    def __init__(self, client: OpenSearch):
        self._logger = logging.getLogger(SearchServiceOpensearch1.__name__)
        self._client = client
        self._search = Search(using=self._client)

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
        # See: Sources for "streaming_bulk":
        # https://github.com/opensearch-project/opensearch-py/blob/db972e615b9156b4e364091d6a893d64fb3ef4f3/opensearchpy/helpers/actions.py#L267
        # The documentation is incredibly vague about what the function actually returns, but these
        # parameters _should_ cause it to return a tuple containing the number of successfully documents
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
                error_results.append(
                    SearchServiceFailedDocument(
                        id=int(error.id),
                        error_status=error.status,
                        error_message=error.error,
                        error_exception=error.exception,
                    )
                )
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

    def search_client(self) -> Search:
        return self._search

    def read_pointer_name(self, base_name: str) -> str:
        return f"{base_name}-search-read"

    def write_pointer_name(self, base_name: str) -> str:
        return f"{base_name}-search-write"

    @staticmethod
    def _empty(base_name):
        return f"{base_name}-empty"
