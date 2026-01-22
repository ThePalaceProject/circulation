from unittest.mock import MagicMock

from palace.manager.feed.worklist.top_level import TopLevelWorkList
from tests.fixtures.database import DatabaseTransactionFixture


class TestHierarchyWorkList:
    """Test HierarchyWorkList in terms of its two subclasses, Lane and TopLevelWorkList."""

    def test_accessible_to(self, db: DatabaseTransactionFixture):
        # In addition to the general tests imposed by WorkList, a Lane
        # is only accessible to a patron if it is a descendant of
        # their root lane.
        lane = db.lane()
        patron = db.patron()
        lane.root_for_patron_type = ["1"]
        patron.external_type = "1"

        # Descendant -> it's accessible
        m = lane.accessible_to
        lane.is_self_or_descendant = MagicMock(return_value=True)
        assert True == m(patron)

        # Not a descendant -> it's not accessible
        lane.is_self_or_descendant = MagicMock(return_value=False)
        assert False == m(patron)

        # If the patron has no root lane, is_self_or_descendant
        # isn't consulted -- everything is accessible.
        patron.external_type = "2"
        assert True == m(patron)

        # Similarly if there is no authenticated patron.
        assert True == m(None)

        # TopLevelWorkList works the same way -- it's visible unless the
        # patron has a top-level lane set.
        wl = TopLevelWorkList()
        wl.initialize(db.default_library())

        assert True == wl.accessible_to(None)
        assert True == wl.accessible_to(patron)
        patron.external_type = "1"
        assert False == wl.accessible_to(patron)

        # However, a TopLevelWorkList associated with library A is not
        # visible to a patron from library B.
        library2 = db.library()
        wl.initialize(library2)
        patron.external_type = None
        assert False == wl.accessible_to(patron)
