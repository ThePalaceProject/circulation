from palace.manager.search.document import LONG
from palace.manager.search.v7 import SearchV7


class TestSearchV7:
    def test(self):
        v7 = SearchV7()
        licensepools = v7._fields["licensepools"]
        assert licensepools.properties["last_updated"] == LONG
