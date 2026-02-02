from __future__ import annotations

from typing import TYPE_CHECKING

from palace.manager.feed.facets.feed import DefaultSortOrderFacets, Facets

if TYPE_CHECKING:
    from palace.manager.search.filter import Filter


class SeriesFacets(DefaultSortOrderFacets):
    """A list with a series restriction is ordered by series position by
    default.
    """

    DEFAULT_SORT_ORDER = Facets.ORDER_SERIES_POSITION


class HasSeriesFacets(Facets):
    """A faceting object for a feed containg books guaranteed
    to belong to _some_ series.
    """

    def modify_search_filter(self, filter: Filter) -> Filter:
        filter.series = True
        return filter
