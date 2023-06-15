import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

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


class SearchService(ABC):
    """The interface we need from services like Opensearch when dealing with migrations."""

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
    def create_index(self, base_name: str, revision: SearchSchemaRevision) -> None:
        """Atomically create an index for the given base name and revision."""

    @abstractmethod
    def index_is_populated(
        self, base_name: str, revision: SearchSchemaRevision
    ) -> bool:
        """Return True if the index for the given base name and revision has been populated."""

    @abstractmethod
    def populate_index(self, base_name: str, revision: SearchSchemaRevision) -> None:
        pass

    @abstractmethod
    def write_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        """Atomically set the write pointer to the index for the given revision and base name."""


class SearchServiceOpensearch1(SearchService):
    """The real Opensearch 1.x service."""

    @staticmethod
    def _read_pointer(base_name):
        return f"{base_name}-search-read"

    @staticmethod
    def _empty(base_name):
        return f"{base_name}-empty"

    @staticmethod
    def _write_pointer(base_name: str) -> str:
        return f"{base_name}-search-write"

    def __init__(self, client: OpenSearch):
        self._client = client

    def write_pointer(self, base_name: str) -> Optional[SearchWritePointer]:
        try:
            result: dict = self._client.indices.get_alias(
                name=self._write_pointer(base_name)
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
            self._client.indices.create(index=self._empty(base_name))
        except RequestError as e:
            if e.error == "resource_already_exists_exception":
                return
            raise e

    def read_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        alias_name = self._read_pointer(base_name)
        target_index = revision.name_for_index(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._client.indices.update_aliases(body=action)

    def read_pointer_set_empty(self, base_name: str) -> None:
        alias_name = self._read_pointer(base_name)
        target_index = self._empty(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._client.indices.update_aliases(body=action)

    def create_index(self, base_name: str, revision: SearchSchemaRevision) -> None:
        try:
            self._client.indices.create(
                index=revision.name_for_index(base_name),
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

    def populate_index(self, base_name: str, revision: SearchSchemaRevision) -> None:
        data = {"properties": revision.mapping_document().serialize_properties()}
        self._client.indices.put_mapping(
            index=revision.name_for_index(base_name), body=data
        )

    def write_pointer_set(self, base_name: str, revision: SearchSchemaRevision) -> None:
        alias_name = self._write_pointer(base_name)
        target_index = revision.name_for_index(base_name)
        action = {
            "actions": [
                {"remove": {"index": "*", "alias": alias_name}},
                {"add": {"index": target_index, "alias": alias_name}},
            ]
        }
        self._client.indices.update_aliases(body=action)

    def read_pointer(self, base_name: str) -> Optional[str]:
        try:
            result: dict = self._client.indices.get_alias(
                name=self._read_pointer(base_name)
            )
            for name in result.keys():
                if name.startswith(f"{base_name}-"):
                    return name
            return None
        except NotFoundError:
            return None
