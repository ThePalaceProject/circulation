from abc import ABC, abstractmethod

from core.search.document import SearchMappingDocument


class SearchSchemaRevision(ABC):
    """
    A versioned schema revision. A revision has an associated version number and can produce
    a top-level Opensearch mapping document on demand. Revision version numbers are unique,
    and revisions are treated as immutable once created.
    """

    _version: int

    def __init__(self, version: int):
        self._version = version

    @abstractmethod
    def mapping_document(self) -> SearchMappingDocument:
        """Produce a mapping document for this schema revision."""

    @property
    def version(self) -> int:
        return self._version

    def name_for_index(self, base_name: str) -> str:
        """Produce the name of the index as it will appear in Opensearch,
        such as 'circulation-works-v5'."""
        return f"{base_name}-v{self.version}"

    def name_for_indexed_pointer(self, base_name: str) -> str:
        """Produce the name of the "indexed pointer" as it will appear in Opensearch,
        such as 'circulation-works-v5-indexed'."""
        return f"{base_name}-v{self.version}-indexed"

    def script_name(self, script_name):
        return f"simplified.{script_name}.v{self.version}"
