from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.facets.qa import JackpotFacets
from tests.fixtures.database import DatabaseTransactionFixture


class TestJackpotFacets:
    def test_default_facet(self, db: DatabaseTransactionFixture):
        # A JackpotFacets object defaults to showing only books that
        # are currently available. Normal facet configuration is
        # ignored.
        m = JackpotFacets.default_facet

        default = m(None, JackpotFacets.AVAILABILITY_FACET_GROUP_NAME)
        assert Facets.AVAILABLE_NOW == default

        # For other facet groups, the class defers to the Facets
        # superclass. (But this doesn't matter because it's not relevant
        # to the creation of jackpot feeds.)
        assert m(
            db.default_library(), Facets.ORDER_FACET_GROUP_NAME
        ) == Facets.default_facet(db.default_library(), Facets.ORDER_FACET_GROUP_NAME)

    def test_available_facets(self, db: DatabaseTransactionFixture):
        # A JackpotFacets object always has the same availability
        # facets. Normal facet configuration is ignored.

        m = JackpotFacets.available_facets
        available = m(None, JackpotFacets.AVAILABILITY_FACET_GROUP_NAME)
        assert [
            Facets.AVAILABLE_NOW,
            Facets.AVAILABLE_NOT_NOW,
            Facets.AVAILABLE_ALL,
            Facets.AVAILABLE_OPEN_ACCESS,
        ] == available

        # For other facet groups, the class defers to the Facets
        # superclass. (But this doesn't matter because it's not relevant
        # to the creation of jackpot feeds.)
        for group in (Facets.ORDER_FACET_GROUP_NAME,):
            assert m(db.default_library(), group) == Facets.available_facets(
                db.default_library(), group
            )
