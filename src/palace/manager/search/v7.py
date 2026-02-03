from typing import cast

from palace.manager.search.document import LONG, SearchMappingFieldTypeObject
from palace.manager.search.v6 import SearchV6


class SearchV7(SearchV6):
    @property
    def version(self) -> int:
        return 7

    def __init__(self) -> None:
        super().__init__()
        licensepools = cast(SearchMappingFieldTypeObject, self._fields["licensepools"])
        licensepools.add_property("last_updated", LONG)
