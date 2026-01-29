from palace.manager.feed.facets.contributor import ContributorFacets
from palace.manager.feed.worklist.dynamic import DynamicLane


class ContributorLane(DynamicLane):
    """A lane of Works written by a particular contributor"""

    ROUTE = "contributor"
    # Cache for 24 hours -- would ideally be longer but availability
    # information goes stale.
    MAX_CACHE_AGE = 24 * 60 * 60

    def __init__(
        self, library, contributor, parent=None, languages=None, audiences=None
    ):
        """Constructor.

        :param library: A Library.
        :param contributor: A Contributor or ContributorData object.
        :param parent: A WorkList.
        :param languages: An extra restriction on the languages of Works.
        :param audiences: An extra restriction on the audience for Works.
        """
        if not contributor:
            raise ValueError("ContributorLane can't be created without contributor")

        self.contributor = contributor
        self.contributor_key = (
            self.contributor.display_name or self.contributor.sort_name
        )
        super().initialize(
            library,
            display_name=self.contributor_key,
            audiences=audiences,
            languages=languages,
        )
        if parent:
            parent.append_child(self)

    @property
    def url_arguments(self):
        kwargs = dict(
            contributor_name=self.contributor_key,
            languages=self.language_key,
            audiences=self.audience_key,
        )
        return self.ROUTE, kwargs

    def overview_facets(self, _db, facets):
        """Convert a FeaturedFacets to a ContributorFacets suitable for
        use in a grouped feed.
        """
        return ContributorFacets.default(
            self.get_library(_db),
            availability=facets.AVAILABLE_ALL,
            entrypoint=facets.entrypoint,
        )

    def modify_search_filter_hook(self, filter):
        filter.author = self.contributor
        return filter
