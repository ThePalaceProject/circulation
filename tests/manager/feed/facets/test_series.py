from palace.manager.feed.facets.feed import DefaultSortOrderFacets, Facets
from palace.manager.feed.facets.series import HasSeriesFacets, SeriesFacets
from palace.manager.search.filter import Filter
from tests.fixtures.database import DatabaseTransactionFixture


class TestHasSeriesFacets:
    def test_modify_search_filter(self, db: DatabaseTransactionFixture):
        facets = HasSeriesFacets.default(db.default_library())
        filter = Filter()
        assert None == filter.series
        facets.modify_search_filter(filter)
        assert True == filter.series


class TestSeriesFacets:
    def test_default_sort_order(self, db: DatabaseTransactionFixture):
        assert Facets.ORDER_SERIES_POSITION == SeriesFacets.DEFAULT_SORT_ORDER
        facets = SeriesFacets.default(db.default_library())
        assert isinstance(facets, DefaultSortOrderFacets)
        assert Facets.ORDER_SERIES_POSITION == facets.order
