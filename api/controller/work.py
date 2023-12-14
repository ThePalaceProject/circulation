from __future__ import annotations

import urllib.parse

import flask
from flask_babel import lazy_gettext as _

from api.controller.circulation_manager import CirculationManagerController
from api.lanes import (
    ContributorFacets,
    ContributorLane,
    RecommendationLane,
    RelatedBooksLane,
    SeriesFacets,
    SeriesLane,
)
from api.problem_details import NO_SUCH_LANE, NOT_FOUND_ON_REMOTE
from core.app_server import load_pagination_from_request
from core.config import CannotLoadConfiguration
from core.external_search import SortKeyPagination
from core.feed.acquisition import OPDSAcquisitionFeed
from core.lane import FeaturedFacets, Pagination
from core.metadata_layer import ContributorData
from core.util.opds_writer import OPDSFeed
from core.util.problem_detail import ProblemDetail


class WorkController(CirculationManagerController):
    def _lane_details(self, languages, audiences):
        if languages:
            languages = languages.split(",")
        if audiences:
            audiences = [urllib.parse.unquote_plus(a) for a in audiences.split(",")]

        return languages, audiences

    def contributor(
        self, contributor_name, languages, audiences, feed_class=OPDSAcquisitionFeed
    ):
        """Serve a feed of books written by a particular author"""
        library = flask.request.library
        if not contributor_name:
            return NO_SUCH_LANE.detailed(_("No contributor provided"))

        # contributor_name is probably a display_name, but it could be a
        # sort_name. Pass it in for both fields and
        # ContributorData.lookup() will do its best to figure it out.
        contributor = ContributorData.lookup(
            self._db, sort_name=contributor_name, display_name=contributor_name
        )
        if not contributor:
            return NO_SUCH_LANE.detailed(
                _("Unknown contributor: %s") % contributor_name
            )

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        languages, audiences = self._lane_details(languages, audiences)

        lane = ContributorLane(
            library, contributor, languages=languages, audiences=audiences
        )
        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=ContributorFacets
        )
        if isinstance(facets, ProblemDetail):
            return facets

        pagination = load_pagination_from_request(SortKeyPagination)
        if isinstance(pagination, ProblemDetail):
            return pagination

        annotator = self.manager.annotator(lane, facets)

        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        return feed_class.page(
            _db=self._db,
            title=lane.display_name,
            url=url,
            worklist=lane,
            facets=facets,
            pagination=pagination,
            annotator=annotator,
            search_engine=search_engine,
        ).as_response(
            max_age=lane.max_cache_age(), mime_types=flask.request.accept_mimetypes
        )

    def permalink(self, identifier_type, identifier):
        """Serve an entry for a single book.

        This does not include any loan or hold-specific information for
        the authenticated patron.

        This is different from the /works lookup protocol, in that it
        returns a single entry while the /works lookup protocol returns a
        feed containing any number of entries.
        """
        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        patron = flask.request.patron

        if patron:
            pools = self.load_licensepools(library, identifier_type, identifier)
            if isinstance(pools, ProblemDetail):
                return pools

            loan, pool = self.get_patron_loan(patron, pools)
            hold = None

            if not loan:
                hold, pool = self.get_patron_hold(patron, pools)

            item = loan or hold
            pool = pool or pools[0]

            return OPDSAcquisitionFeed.single_entry_loans_feed(
                self.circulation, item or pool
            )
        else:
            annotator = self.manager.annotator(lane=None)

            return OPDSAcquisitionFeed.entry_as_response(
                OPDSAcquisitionFeed.single_entry(work, annotator),
                max_age=OPDSFeed.DEFAULT_MAX_AGE,
            )

    def related(
        self,
        identifier_type,
        identifier,
        novelist_api=None,
        feed_class=OPDSAcquisitionFeed,
    ):
        """Serve a groups feed of books related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if work is None:
            return NOT_FOUND_ON_REMOTE

        if isinstance(work, ProblemDetail):
            return work

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        try:
            lane_name = f"Books Related to {work.title} by {work.author}"
            lane = RelatedBooksLane(library, work, lane_name, novelist_api=novelist_api)
        except ValueError as e:
            # No related books were found.
            return NO_SUCH_LANE.detailed(str(e))

        facets = self.manager.load_facets_from_request(
            worklist=lane,
            base_class=FeaturedFacets,
            base_class_constructor_kwargs=dict(
                minimum_featured_quality=library.settings.minimum_featured_quality
            ),
        )
        if isinstance(facets, ProblemDetail):
            return facets

        annotator = self.manager.annotator(lane)
        url = annotator.feed_url(
            lane,
            facets=facets,
        )

        return feed_class.groups(
            _db=self._db,
            title=lane.DISPLAY_NAME,
            url=url,
            worklist=lane,
            annotator=annotator,
            pagination=None,
            facets=facets,
            search_engine=search_engine,
        ).as_response(
            max_age=lane.max_cache_age(), mime_types=flask.request.accept_mimetypes
        )

    def recommendations(
        self,
        identifier_type,
        identifier,
        novelist_api=None,
        feed_class=OPDSAcquisitionFeed,
    ):
        """Serve a feed of recommendations related to a given book."""

        library = flask.request.library
        work = self.load_work(library, identifier_type, identifier)
        if isinstance(work, ProblemDetail):
            return work

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        lane_name = f"Recommendations for {work.title} by {work.author}"
        try:
            lane = RecommendationLane(
                library=library,
                work=work,
                display_name=lane_name,
                novelist_api=novelist_api,
            )
        except CannotLoadConfiguration as e:
            # NoveList isn't configured.
            return NO_SUCH_LANE.detailed(_("Recommendations not available"))

        facets = self.manager.load_facets_from_request(worklist=lane)
        if isinstance(facets, ProblemDetail):
            return facets

        # We use a normal Pagination object because recommendations
        # are looked up in a third-party API and paginated through the
        # database lookup.
        pagination = load_pagination_from_request(Pagination)
        if isinstance(pagination, ProblemDetail):
            return pagination

        annotator = self.manager.annotator(lane)
        url = annotator.feed_url(
            lane,
            facets=facets,
            pagination=pagination,
        )

        return feed_class.page(
            _db=self._db,
            title=lane.DISPLAY_NAME,
            url=url,
            worklist=lane,
            facets=facets,
            pagination=pagination,
            annotator=annotator,
            search_engine=search_engine,
        ).as_response(max_age=lane.max_cache_age())

    def series(self, series_name, languages, audiences, feed_class=OPDSAcquisitionFeed):
        """Serve a feed of books in a given series."""
        library = flask.request.library
        if not series_name:
            return NO_SUCH_LANE.detailed(_("No series provided"))

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        languages, audiences = self._lane_details(languages, audiences)
        lane = SeriesLane(
            library, series_name=series_name, languages=languages, audiences=audiences
        )

        facets = self.manager.load_facets_from_request(
            worklist=lane, base_class=SeriesFacets
        )
        if isinstance(facets, ProblemDetail):
            return facets

        pagination = load_pagination_from_request(SortKeyPagination)
        if isinstance(pagination, ProblemDetail):
            return pagination

        annotator = self.manager.annotator(lane)

        url = annotator.feed_url(lane, facets=facets, pagination=pagination)
        return feed_class.page(
            _db=self._db,
            title=lane.display_name,
            url=url,
            worklist=lane,
            facets=facets,
            pagination=pagination,
            annotator=annotator,
            search_engine=search_engine,
        ).as_response(
            max_age=lane.max_cache_age(), mime_types=flask.request.accept_mimetypes
        )
