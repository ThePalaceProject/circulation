from palace.manager.feed.facets.contributor import ContributorFacets
from palace.manager.feed.facets.feed import DefaultSortOrderFacets, Facets
from tests.fixtures.database import DatabaseTransactionFixture


class TestContributorFacets:
    def test_default_sort_order(self, db: DatabaseTransactionFixture):
        assert Facets.ORDER_TITLE == ContributorFacets.DEFAULT_SORT_ORDER
        facets = ContributorFacets.default(db.default_library())
        assert isinstance(facets, DefaultSortOrderFacets)
        assert Facets.ORDER_TITLE == facets.order
