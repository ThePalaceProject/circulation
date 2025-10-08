from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import opensearchpy.helpers
from opensearchpy import MultiSearch, NotFoundError, OpenSearch, RequestError, Search
from typing_extensions import Self

from palace.manager.core.exceptions import BasePalaceException
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.util.log import LoggerMixin


@dataclass(frozen=True)
class SearchPointer:
    """A search pointer, which is an alias that points to a specific index."""

    alias: str
    index: str
    version: int

    @classmethod
    def from_index(cls, base_name: str, alias: str, index: str) -> Self | None:
        version = cls._parse_version(base_name, index)
        if version is None:
            return None

        return cls(
            alias=alias,
            index=index,
            version=version,
        )

    @classmethod
    def _parse_version(cls, base_name: str, index: str) -> int | None:
        match = re.search(f"^{base_name}-v([0-9]+)$", string=index)
        if match is None:
            return None
        return int(match.group(1))


class SearchServiceException(BasePalaceException):
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
    def from_bulk_error(cls, error: dict[str, Any]) -> SearchServiceFailedDocument:
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


SearchDocument = dict[str, Any]


class SearchService(ABC):
    """The interface we need from services like Opensearch. Essentially, it provides the operations we want with
    sensible types, rather than the untyped pile of JSON the actual search client provides.
    """

    @property
    @abstractmethod
    def base_revision_name(self) -> str:
        """The base name used for all indexes."""

    @abstractmethod
    def read_pointer_name(self) -> str:
        """Get the name used for the read pointer."""

    @abstractmethod
    def write_pointer_name(self) -> str:
        """Get the name used for the write pointer."""

    @abstractmethod
    def read_pointer(self) -> SearchPointer | None:
        """Get the read pointer, if it exists."""

    @abstractmethod
    def write_pointer(self) -> SearchPointer | None:
        """Get the writer pointer, if it exists."""

    @abstractmethod
    def read_pointer_set(self, revision: SearchSchemaRevision) -> None:
        """Atomically set the read pointer to the index for the given revision and base name."""

    @abstractmethod
    def index_create(self, revision: SearchSchemaRevision) -> None:
        """Atomically create an index for the given base name and revision."""

    @abstractmethod
    def index_set_mapping(self, revision: SearchSchemaRevision) -> None:
        """Set the schema mappings for the given index."""

    @abstractmethod
    def index_submit_document(
        self, document: dict[str, Any], refresh: bool = False
    ) -> None:
        """Submit a search document to the given index."""

    @abstractmethod
    def index_submit_documents(
        self,
        documents: Sequence[SearchDocument],
    ) -> list[SearchServiceFailedDocument]:
        """Submit search documents to the given index."""

    @abstractmethod
    def write_pointer_set(self, revision: SearchSchemaRevision) -> None:
        """Atomically set the write pointer to the index for the given revision and base name."""

    @abstractmethod
    def refresh(self) -> None:
        """Synchronously refresh the service and wait for changes to be completed."""

    @abstractmethod
    def index_clear_documents(self) -> None:
        """Clear all search documents in the given index."""

    @abstractmethod
    def read_search_client(self) -> Search:
        """Return the underlying search client."""

    @abstractmethod
    def read_search_multi_client(self) -> MultiSearch:
        """Return the underlying search client."""

    @abstractmethod
    def index_remove_document(self, doc_id: int) -> None:
        """Remove a specific document from the given index."""


class SearchServiceOpensearch1(SearchService, LoggerMixin):
    """The real Opensearch 1.x service."""

    def __init__(self, client: OpenSearch, base_revision_name: str):
        self._client = client
        self._search = Search(using=self._client)
        self._base_revision_name = base_revision_name
        self._multi_search = MultiSearch(using=self._client)

        # Documents are not allowed to automatically create indexes.
        # AWS OpenSearch only accepts the "flat" format
        self._client.cluster.put_settings(
            body={"persistent": {"action.auto_create_index": "false"}}
        )

    @property
    def base_revision_name(self) -> str:
        return self._base_revision_name

    def _get_pointer(self, name: str) -> SearchPointer | None:
        try:
            result = self._client.indices.get_alias(name=name)
            if len(result) != 1:
                # This should never happen, based on my understanding of the API.
                self.log.error(
                    f"unexpected number of indexes for alias {name}: {result}"
                )
                return None
            index_name = next(iter(result.keys()))
            return SearchPointer.from_index(
                base_name=self.base_revision_name,
                alias=name,
                index=index_name,
            )
        except NotFoundError:
            return None

    def write_pointer(self) -> SearchPointer | None:
        return self._get_pointer(self.write_pointer_name())

    def read_pointer_set(self, revision: SearchSchemaRevision) -> None:
        alias_name = self.read_pointer_name()
        target_index = revision.name_for_index(self.base_revision_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self.log.debug(f"setting read pointer {alias_name} to index {target_index}")
        self._client.indices.update_aliases(body=action)

    def index_create(self, revision: SearchSchemaRevision) -> None:
        try:
            index_name = revision.name_for_index(self.base_revision_name)
            self.log.info(f"creating index {index_name}")
            self._client.indices.create(
                index=index_name,
                body=revision.mapping_document().serialize(),
            )
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                return
            raise e

    def index_set_mapping(self, revision: SearchSchemaRevision) -> None:
        data = {"properties": revision.mapping_document().serialize_properties()}
        index_name = revision.name_for_index(self.base_revision_name)
        self.log.debug(f"setting mappings for index {index_name}")
        self._client.indices.put_mapping(index=index_name, body=data)
        self._ensure_scripts(revision)

    def _ensure_scripts(self, revision: SearchSchemaRevision) -> None:
        for name, body in revision.mapping_document().scripts.items():
            script = dict(script=dict(lang="painless", source=body))
            if not name.startswith("simplified"):
                name = revision.script_name(name)
            self._client.put_script(id=name, body=script)

    def index_submit_document(
        self, document: dict[str, Any], refresh: bool = False
    ) -> None:
        _id = document.pop("_id")
        self._client.index(
            id=_id,
            index=self.write_pointer_name(),
            body=document,
            require_alias=True,
            refresh=refresh,
        )

    def index_submit_documents(
        self, documents: Sequence[SearchDocument]
    ) -> list[SearchServiceFailedDocument]:
        pointer = self.write_pointer_name()
        self.log.info(f"submitting documents to index {pointer}")

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

        error_results: list[SearchServiceFailedDocument] = []
        if isinstance(errors, list):
            for error in errors:
                error_results.append(SearchServiceFailedDocument.from_bulk_error(error))
        else:
            raise SearchServiceException(
                f"Opensearch returned {errors} instead of a list of errors."
            )

        return error_results

    def index_clear_documents(self) -> None:
        self._client.delete_by_query(
            index=self.write_pointer_name(),
            body={"query": {"match_all": {}}},
            wait_for_completion=True,
        )

    def refresh(self) -> None:
        self.log.debug(f"waiting for indexes to become ready")
        self._client.indices.refresh()

    def write_pointer_set(self, revision: SearchSchemaRevision) -> None:
        alias_name = self.write_pointer_name()
        target_index = revision.name_for_index(self.base_revision_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self.log.debug(f"setting write pointer {alias_name} to {target_index}")
        self._client.indices.update_aliases(body=action)

    def read_pointer(self) -> SearchPointer | None:
        return self._get_pointer(self.read_pointer_name())

    def read_search_client(self) -> Search:
        return self._search.index(self.read_pointer_name())  # type: ignore[no-any-return]
        # opensearchpy Search.index() is not properly typed

    def read_search_multi_client(self) -> MultiSearch:
        return self._multi_search.index(self.read_pointer_name())  # type: ignore[no-any-return]
        # opensearchpy MultiSearch.index() is not properly typed

    def read_pointer_name(self) -> str:
        return f"{self.base_revision_name}-search-read"

    def write_pointer_name(self) -> str:
        return f"{self.base_revision_name}-search-write"

    def index_remove_document(self, doc_id: int) -> None:
        self._client.delete(index=self.write_pointer_name(), id=doc_id)
