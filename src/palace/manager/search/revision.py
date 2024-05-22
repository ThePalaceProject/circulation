from abc import ABC, abstractmethod

from palace.manager.search.document import SearchMappingDocument


class SearchSchemaRevision(ABC):
    """
    A versioned schema revision. A revision has an associated version number and can produce
    a top-level Opensearch mapping document on demand. Revision version numbers are unique,
    and revisions are treated as immutable once created.
    """

    @abstractmethod
    def mapping_document(self) -> SearchMappingDocument:
        """Produce a mapping document for this schema revision."""

    @property
    @abstractmethod
    def version(self) -> int:
        """The version number of this schema revision."""

    def name_for_index(self, base_name: str) -> str:
        """Produce the name of the index as it will appear in Opensearch,
        such as 'circulation-works-v5'."""
        return f"{base_name}-v{self.version}"

    def script_name(self, script_name: str) -> str:
        return f"simplified.{script_name}.v{self.version}"
