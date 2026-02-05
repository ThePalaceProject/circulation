from palace.manager.feed.facets.feed import DefaultSortOrderFacets, Facets
from palace.manager.feed.facets.series import SeriesFacets
from tests.fixtures.database import DatabaseTransactionFixture


class TestSeriesFacets:
    def test_default_sort_order(self, db: DatabaseTransactionFixture):
        assert Facets.ORDER_SERIES_POSITION == SeriesFacets.DEFAULT_SORT_ORDER
        facets = SeriesFacets.default(db.default_library())
        assert isinstance(facets, DefaultSortOrderFacets)
        assert Facets.ORDER_SERIES_POSITION == facets.order
