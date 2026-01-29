from collections import defaultdict

from sqlalchemy.orm import Session

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.worklist.contributor import ContributorLane
from palace.manager.feed.worklist.dynamic import WorkBasedLane
from palace.manager.feed.worklist.series import SeriesLane
from palace.manager.integration.metadata.novelist import NoveListAPI
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.identifier import Identifier


class RecommendationLane(WorkBasedLane):
    """A lane of recommended Works based on a particular Work"""

    DISPLAY_NAME = "Titles recommended by NoveList"
    ROUTE = "recommendations"

    # Cache for 24 hours -- would ideally be much longer but availability
    # information goes stale.
    MAX_CACHE_AGE = 24 * 60 * 60

    def __init__(
        self, library, work, display_name=None, novelist_api=None, parent=None
    ):
        """Constructor.

        :raises: CannotLoadConfiguration if `novelist_api` is not provided
        and no Novelist integration is configured for this library.
        """
        super().__init__(
            library,
            work,
            display_name=display_name,
        )
        self.novelist_api = novelist_api or NoveListAPI.from_config(library)
        if parent:
            parent.append_child(self)
        _db = Session.object_session(library)
        self.recommendations = self.fetch_recommendations(_db)

    def fetch_recommendations(self, _db: Session) -> list[Identifier]:
        """Get identifiers of recommendations for this LicensePool"""
        recommendation_data = self.novelist_api.lookup_recommendations(
            self.edition.primary_identifier
        )
        recommendations = []
        by_type: defaultdict[str, list[str]] = defaultdict(list)
        for identifier in recommendation_data:
            by_type[identifier.type].append(identifier.identifier)

        for identifier_type, identifiers in by_type.items():
            existing_identifiers = (
                _db.query(Identifier)
                .filter(Identifier.type == identifier_type)
                .filter(Identifier.identifier.in_(identifiers))
            )
            recommendations.extend(existing_identifiers.all())

        return recommendations

    def overview_facets(self, _db, facets):
        """Convert a generic FeaturedFacets to some other faceting object,
        suitable for showing an overview of this WorkList in a grouped
        feed.
        """
        # TODO: Since the purpose of the recommendation feed is to
        # suggest books that can be borrowed immediately, it would be
        # better to set availability=AVAILABLE_NOW. However, this feed
        # is cached for so long that we can't rely on the availability
        # information staying accurate. It would be especially bad if
        # people borrowed all of the recommendations that were
        # available at the time this feed was generated, and then
        # recommendations that were unavailable when the feed was
        # generated became available.
        #
        # For now, it's better to show all books and let people put
        # the unavailable ones on hold if they want.
        #
        # TODO: It would be better to order works in the same order
        # they come from the recommendation engine, since presumably
        # the best recommendations are in the front.
        return Facets.default(
            self.get_library(_db),
            availability=facets.AVAILABLE_ALL,
            entrypoint=facets.entrypoint,
        )

    def modify_search_filter_hook(self, filter):
        """Find Works whose Identifiers include the ISBNs returned
        by an external recommendation engine.

        :param filter: A Filter object.
        """
        if not self.recommendations:
            # There are no recommendations. The search should not even
            # be executed.
            filter.match_nothing = True
        else:
            filter.identifiers = self.recommendations
        return filter


class RelatedBooksLane(WorkBasedLane):
    """A lane of Works all related to a given Work by various criteria.

    Each criterion is represented by another WorkBaseLane class:

    * ContributorLane: Works by one of the contributors to this work.
    * SeriesLane: Works in the same series.
    * RecommendationLane: Works provided by a third-party recommendation
      service.
    """

    DISPLAY_NAME = "Related Books"
    ROUTE = "related_books"

    # Cache this lane for the shortest amount of time any of its
    # component lane should be cached.
    MAX_CACHE_AGE = min(
        ContributorLane.MAX_CACHE_AGE,
        SeriesLane.MAX_CACHE_AGE,
        RecommendationLane.MAX_CACHE_AGE,
    )

    def __init__(self, library, work, display_name=None, novelist_api=None):
        super().__init__(
            library,
            work,
            display_name=display_name,
        )
        _db = Session.object_session(library)
        sublanes = self._get_sublanes(_db, novelist_api)
        if not sublanes:
            raise ValueError(
                "No related books for {} by {}".format(
                    self.work.title, self.work.author
                )
            )
        self.children = sublanes

    def works(self, _db, *args, **kwargs):
        """This lane never has works of its own.

        Only its sublanes have works.
        """
        return []

    def _get_sublanes(self, _db, novelist_api):
        sublanes = list()

        for contributor_lane in self._contributor_sublanes(_db):
            sublanes.append(contributor_lane)

        for recommendation_lane in self._recommendation_sublane(_db, novelist_api):
            sublanes.append(recommendation_lane)

        # Create a series sublane.
        series_name = self.edition.series
        if series_name:
            sublanes.append(
                SeriesLane(
                    self.get_library(_db),
                    series_name,
                    parent=self,
                    languages=self.languages,
                )
            )

        return sublanes

    def _contributor_sublanes(self, _db):
        """Create contributor sublanes"""
        viable_contributors = list()
        roles_by_priority = list(Contributor.author_contributor_tiers())[1:]

        while roles_by_priority and not viable_contributors:
            author_roles = roles_by_priority.pop(0)
            viable_contributors = [
                c.contributor
                for c in self.edition.contributions
                if c.role in author_roles
            ]

        library = self.get_library(_db)
        for contributor in viable_contributors:
            contributor_lane = ContributorLane(
                library,
                contributor,
                parent=self,
                languages=self.languages,
                audiences=self.audiences,
            )
            yield contributor_lane

    def _recommendation_sublane(self, _db, novelist_api):
        """Create a recommendations sublane."""
        lane_name = "Similar titles recommended by NoveList"
        try:
            recommendation_lane = RecommendationLane(
                library=self.get_library(_db),
                work=self.work,
                display_name=lane_name,
                novelist_api=novelist_api,
                parent=self,
            )
            if recommendation_lane.recommendations:
                yield recommendation_lane
        except CannotLoadConfiguration as e:
            # NoveList isn't configured. This isn't fatal -- we just won't
            # use this sublane.
            pass
