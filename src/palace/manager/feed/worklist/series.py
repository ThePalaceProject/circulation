from palace.manager.feed.facets.series import SeriesFacets
from palace.manager.feed.worklist.dynamic import DynamicLane, WorkBasedLane


class SeriesLane(DynamicLane):
    """A lane of Works in a particular series."""

    ROUTE = "series"
    # Cache for 24 hours -- would ideally be longer but availability
    # information goes stale.
    MAX_CACHE_AGE = 24 * 60 * 60

    def __init__(self, library, series_name, parent=None, **kwargs):
        if not series_name:
            raise ValueError("SeriesLane can't be created without series")
        super().initialize(library, display_name=series_name, **kwargs)
        self.series = series_name
        if parent:
            parent.append_child(self)
            if isinstance(parent, WorkBasedLane) and parent.source_audience:
                # WorkBasedLane forces self.audiences to values
                # compatible with the work in the WorkBasedLane, but
                # that's not enough for us. We want to force
                # self.audiences to *the specific audience* of the
                # work in the WorkBasedLane. If we're looking at a YA
                # series, we don't want to see books in a children's
                # series with the same name, even if it would be
                # appropriate to show those books.
                self.audiences = [parent.source_audience]

    @property
    def url_arguments(self):
        kwargs = dict(series_name=self.series)
        if self.language_key:
            kwargs["languages"] = self.language_key
        if self.audience_key:
            kwargs["audiences"] = self.audience_key
        return self.ROUTE, kwargs

    def overview_facets(self, _db, facets):
        """Convert a FeaturedFacets to a SeriesFacets suitable for
        use in a grouped feed. Our contribution to a grouped feed will
        be ordered by series position.
        """
        return SeriesFacets.default(
            self.get_library(_db),
            availability=facets.AVAILABLE_ALL,
            entrypoint=facets.entrypoint,
        )

    def modify_search_filter_hook(self, filter):
        filter.series = self.series
        return filter
