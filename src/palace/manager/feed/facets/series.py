from __future__ import annotations

from palace.manager.feed.facets.feed import DefaultSortOrderFacets, Facets


class SeriesFacets(DefaultSortOrderFacets):
    """A list with a series restriction is ordered by series position by
    default.
    """

    DEFAULT_SORT_ORDER = Facets.ORDER_SERIES_POSITION
