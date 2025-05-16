from palace.manager.search.document import INTEGER
from palace.manager.search.v6 import SearchV6


class TestSearchV6:
    def test(self):
        v6 = SearchV6()
        assert v6._fields["lane_priority_level"] == INTEGER
