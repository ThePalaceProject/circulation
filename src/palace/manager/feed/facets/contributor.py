from palace.manager.feed.facets.feed import DefaultSortOrderFacets, Facets


class ContributorFacets(DefaultSortOrderFacets):
    """A list with a contributor restriction is, by default, sorted by
    title.
    """

    DEFAULT_SORT_ORDER = Facets.ORDER_TITLE
