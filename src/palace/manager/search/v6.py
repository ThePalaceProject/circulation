from palace.manager.search.document import (
    INTEGER,
)
from palace.manager.search.v5 import SearchV5


class SearchV6(SearchV5):
    @property
    def version(self) -> int:
        return 6

    def __init__(self) -> None:
        super().__init__()
        licensepools = self._fields["licensepools"]
        licensepools.add_property("lane_priority_level", INTEGER)
