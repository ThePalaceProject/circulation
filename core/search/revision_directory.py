from typing import Mapping

from core.config import CannotLoadConfiguration
from core.search.revision import SearchSchemaRevision
from core.search.v5 import SearchV5

REVISIONS = [SearchV5()]


class SearchRevisionDirectory:
    """A directory of the supported search index schemas."""

    @staticmethod
    def _create_revisions() -> Mapping[int, SearchSchemaRevision]:
        numbers = set()
        revisions = {}
        for r in REVISIONS:
            if r.version in numbers:
                raise ValueError(
                    f"Revision version {r.version} is defined multiple times"
                )
            numbers.add(r.version)
            revisions[r.version] = r
        return revisions

    def __init__(self, available: Mapping[int, SearchSchemaRevision]):
        self._available = available

    @staticmethod
    def create() -> "SearchRevisionDirectory":
        return SearchRevisionDirectory(SearchRevisionDirectory._create_revisions())

    @staticmethod
    def empty() -> "SearchRevisionDirectory":
        return SearchRevisionDirectory({})

    @property
    def available(self) -> Mapping[int, SearchSchemaRevision]:
        return self._available

    def find(self, version: int) -> SearchSchemaRevision:
        """Find the revision with the given version number."""
        try:
            return self._available[version]
        except KeyError:
            raise CannotLoadConfiguration(
                f"No revision available with version {version}"
            )

    def highest(self) -> SearchSchemaRevision:
        """Find the revision with the highest version."""
        return self.find(max(self._available.keys()))
