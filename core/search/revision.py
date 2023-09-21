from abc import ABC, abstractmethod

from core.search.document import SearchMappingDocument


class SearchSchemaRevision(ABC):
    """
    A versioned schema revision. A revision has an associated version number and can produce
    a top-level Opensearch mapping document on demand. Revision version numbers are unique,
    and revisions are treated as immutable once created.
    """

    _version: int
    # The SEARCH_VERSION variable MUST be populated in the implemented child classes
    SEARCH_VERSION: int

    def __init__(self):
        if self.SEARCH_VERSION is None:
            raise ValueError("The SEARCH_VERSION must be defined with an integer value")
        self._version = self.SEARCH_VERSION

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
