from typing import Mapping

from core.search.revision import SearchSchemaRevision
from core.search.v5 import SearchV5


class SearchRevisionDirectory:
    """A directory of the supported search index schemas."""

    @staticmethod
    def _create_revisions() -> Mapping[int, SearchSchemaRevision]:
        return dict(map(lambda r: (r.version, r), [SearchV5()]))

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
