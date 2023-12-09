from __future__ import annotations

import flask
from flask import Response, redirect, url_for

from api.controller.circulation_manager import CirculationManagerController
from api.lanes import (
    CrawlableCollectionBasedLane,
    CrawlableCustomListBasedLane,
    CrawlableFacets,
    HasSeriesFacets,
    JackpotFacets,
    JackpotWorkList,
)
from api.problem_details import NO_SUCH_COLLECTION, NO_SUCH_LIST
from core.app_server import load_facets_from_request, load_pagination_from_request
from core.entrypoint import EverythingEntryPoint
from core.external_search import SortKeyPagination
from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.navigation import NavigationFeed
from core.feed.opds import NavigationFacets
from core.lane import FeaturedFacets, Pagination, SearchFacets, WorkList
from core.model import Collection, CustomList
from core.opensearch import OpenSearchDocument
from core.util.problem_detail import ProblemDetail


class OPDSFeedController(CirculationManagerController):
    def groups(self, lane_identifier, feed_class=OPDSAcquisitionFeed):
        """Build or retrieve a grouped acquisition feed.

        :param lane_identifier: An identifier that uniquely identifiers
            the WorkList whose feed we want.
        :param feed_class: A replacement for AcquisitionFeed, for use in
            tests.
        """
        library = flask.request.library

        # Special case: a patron with a root lane who attempts to access
        # the library's top-level WorkList is redirected to their root
        # lane (as though they had accessed the index controller)
        # rather than being denied access.
        if lane_identifier is None:
            patron = self.request_patron
            if patron is not None and patron.root_lane:
                return redirect(
                    url_for(
                        "acquisition_groups",
                        library_short_name=library.short_name,
                        lane_identifier=patron.root_lane.id,
                        _external=True,
                    )
                )

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane

        if not lane.children:
            # This lane has no children. Although we can technically
            # create a grouped feed, it would be an unsatisfying
            # gateway to a paginated feed. We should just serve the
            # paginated feed.
            return self.feed(lane_identifier, feed_class)

        facet_class_kwargs = dict(
            minimum_featured_quality=library.settings.minimum_featured_quality,
        )
        facets = self.manager.load_facets_from_request(
            worklist=lane,
            base_class=FeaturedFacets,
            base_class_constructor_kwargs=facet_class_kwargs,
        )
        if isinstance(facets, ProblemDetail):
            return facets

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        url = url_for(
            "acquisition_groups",
            lane_identifier=lane_identifier,
            library_short_name=library.short_name,
            _external=True,
        )

        annotator = self.manager.annotator(lane, facets)
        return feed_class.groups(
            _db=self._db,
            title=lane.display_name,
            url=url,
            worklist=lane,
            annotator=annotator,
            facets=facets,
            search_engine=search_engine,
        ).as_response(mime_types=flask.request.accept_mimetypes)

    def feed(self, lane_identifier, feed_class=OPDSAcquisitionFeed):
        """Build or retrieve a paginated acquisition feed.

        :param lane_identifier: An identifier that uniquely identifiers
            the WorkList whose feed we want.
        :param feed_class: A replacement for AcquisitionFeed, for use in
            tests.
        """
        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        facets = self.manager.load_facets_from_request(worklist=lane)
        if isinstance(facets, ProblemDetail):
            return facets
        pagination = load_pagination_from_request(SortKeyPagination)
        if isinstance(pagination, ProblemDetail):
            return pagination
        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        library_short_name = flask.request.library.short_name
        url = url_for(
            "feed",
            lane_identifier=lane_identifier,
            library_short_name=library_short_name,
            _external=True,
        )

        annotator = self.manager.annotator(lane, facets=facets)
        max_age = flask.request.args.get("max_age")
        feed = feed_class.page(
            _db=self._db,
            title=lane.display_name,
            url=url,
            worklist=lane,
            annotator=annotator,
            facets=facets,
            pagination=pagination,
            search_engine=search_engine,
        )
        return feed.as_response(
            max_age=int(max_age) if max_age else lane.max_cache_age(),
            mime_types=flask.request.accept_mimetypes,
        )

    def navigation(self, lane_identifier):
        """Build or retrieve a navigation feed, for clients that do not support groups."""

        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane
        library = flask.request.library
        library_short_name = library.short_name
        url = url_for(
            "navigation_feed",
            lane_identifier=lane_identifier,
            library_short_name=library_short_name,
            _external=True,
        )

        title = lane.display_name
        facet_class_kwargs = dict(
            minimum_featured_quality=library.settings.minimum_featured_quality,
        )
        facets = self.manager.load_facets_from_request(
            worklist=lane,
            base_class=NavigationFacets,
            base_class_constructor_kwargs=facet_class_kwargs,
        )
        annotator = self.manager.annotator(lane, facets)
        return NavigationFeed.navigation(
            _db=self._db,
            title=title,
            url=url,
            worklist=lane,
            annotator=annotator,
            facets=facets,
        ).as_response(max_age=lane.max_cache_age())

    def crawlable_library_feed(self):
        """Build or retrieve a crawlable acquisition feed for the
        request library.
        """
        library = flask.request.library
        url = url_for(
            "crawlable_library_feed",
            library_short_name=library.short_name,
            _external=True,
        )
        title = library.name
        lane = CrawlableCollectionBasedLane()
        lane.initialize(library)
        return self._crawlable_feed(title=title, url=url, worklist=lane)

    def crawlable_collection_feed(self, collection_name):
        """Build or retrieve a crawlable acquisition feed for the
        requested collection.
        """
        collection = Collection.by_name(self._db, collection_name)
        if not collection:
            return NO_SUCH_COLLECTION
        title = collection.name
        url = url_for(
            "crawlable_collection_feed", collection_name=collection.name, _external=True
        )
        lane = CrawlableCollectionBasedLane()
        lane.initialize([collection])
        return self._crawlable_feed(title=title, url=url, worklist=lane)

    def crawlable_list_feed(self, list_name):
        """Build or retrieve a crawlable, paginated acquisition feed for the
        named CustomList, sorted by update date.
        """
        # TODO: A library is not strictly required here, since some
        # CustomLists aren't associated with a library, but this isn't
        # a use case we need to support now.
        library = flask.request.library
        list = CustomList.find(self._db, list_name, library=library)
        if not list:
            return NO_SUCH_LIST
        library_short_name = library.short_name
        title = list.name
        url = url_for(
            "crawlable_list_feed",
            list_name=list.name,
            library_short_name=library_short_name,
            _external=True,
        )
        lane = CrawlableCustomListBasedLane()
        lane.initialize(library, list)
        return self._crawlable_feed(title=title, url=url, worklist=lane)

    def _crawlable_feed(
        self, title, url, worklist, annotator=None, feed_class=OPDSAcquisitionFeed
    ):
        """Helper method to create a crawlable feed.

        :param title: The title to use for the feed.
        :param url: The URL from which the feed will be served.
        :param worklist: A crawlable Lane which controls which works show up
            in the feed.
        :param annotator: A custom Annotator to use when generating the feed.
        :param feed_class: A drop-in replacement for OPDSAcquisitionFeed
            for use in tests.
        """
        pagination = load_pagination_from_request(
            SortKeyPagination, default_size=Pagination.DEFAULT_CRAWLABLE_SIZE
        )
        if isinstance(pagination, ProblemDetail):
            return pagination

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        # A crawlable feed has only one possible set of Facets,
        # so library settings are irrelevant.
        facets = self.manager.load_facets_from_request(
            worklist=worklist,
            base_class=CrawlableFacets,
        )
        annotator = annotator or self.manager.annotator(worklist, facets=facets)

        return feed_class.page(
            _db=self._db,
            title=title,
            url=url,
            worklist=worklist,
            annotator=annotator,
            facets=facets,
            pagination=pagination,
            search_engine=search_engine,
        ).as_response(
            mime_types=flask.request.accept_mimetypes, max_age=worklist.max_cache_age()
        )

    def _load_search_facets(self, lane):
        entrypoints = list(flask.request.library.entrypoints)
        if len(entrypoints) > 1:
            # There is more than one enabled EntryPoint.
            # By default, search them all.
            default_entrypoint = EverythingEntryPoint
        else:
            # There is only one enabled EntryPoint,
            # and no need for a special default.
            default_entrypoint = None
        return self.manager.load_facets_from_request(
            worklist=lane,
            base_class=SearchFacets,
            default_entrypoint=default_entrypoint,
        )

    def search(self, lane_identifier, feed_class=OPDSAcquisitionFeed):
        """Search for books."""
        lane = self.load_lane(lane_identifier)
        if isinstance(lane, ProblemDetail):
            return lane

        # Although the search query goes against Opensearch, we must
        # use normal pagination because the results are sorted by
        # match quality, not bibliographic information.
        pagination = load_pagination_from_request(
            Pagination, default_size=Pagination.DEFAULT_SEARCH_SIZE
        )
        if isinstance(pagination, ProblemDetail):
            return pagination

        facets = self._load_search_facets(lane)
        if isinstance(facets, ProblemDetail):
            return facets

        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        # Check whether there is a query string -- if not, we want to
        # send an OpenSearch document explaining how to search.
        query = flask.request.args.get("q")
        library_short_name = flask.request.library.short_name

        # Create a function that, when called, generates a URL to the
        # search controller.
        #
        # We'll call this one way if there is no query string in the
        # request arguments, and another way if there is a query
        # string.
        make_url_kwargs = dict(list(facets.items()))
        make_url = lambda: url_for(
            "lane_search",
            lane_identifier=lane_identifier,
            library_short_name=library_short_name,
            _external=True,
            **make_url_kwargs,
        )
        if not query:
            # Send the search form
            open_search_doc = OpenSearchDocument.for_lane(lane, make_url())
            headers = {"Content-Type": "application/opensearchdescription+xml"}
            return Response(open_search_doc, 200, headers)

        # We have a query -- add it to the keyword arguments used when
        # generating a URL.
        make_url_kwargs["q"] = query.encode("utf8")

        # Run a search.
        annotator = self.manager.annotator(lane, facets)
        info = OpenSearchDocument.search_info(lane)
        response = feed_class.search(
            _db=self._db,
            title=info["name"],
            url=make_url(),
            lane=lane,
            search_engine=search_engine,
            query=query,
            annotator=annotator,
            pagination=pagination,
            facets=facets,
        )
        if isinstance(response, ProblemDetail):
            return response
        return response.as_response(
            mime_types=flask.request.accept_mimetypes, max_age=lane.max_cache_age()
        )

    def _qa_feed(
        self, feed_factory, feed_title, controller_name, facet_class, worklist_factory
    ):
        """Create some kind of OPDS feed designed for consumption by an
        automated QA process.

        :param feed_factory: This function will be called to create the feed.
           It must either be AcquisitionFeed.groups or Acquisition.page,
           or it must take the same arguments as those methods.
        :param feed_title: String title of the feed.
        :param controller_name: Controller name to use when generating
           the URL to the feed.
        :param facet_class: Faceting class to load (through
            load_facets_from_request).
        :param worklist_factory: Function that takes (Library, Facets)
            and returns a Worklist configured to generate the feed.
        :return: A ProblemDetail if there's a problem loading the faceting
            object; otherwise the return value of `feed_factory`.
        """
        library = flask.request.library
        search_engine = self.search_engine
        if isinstance(search_engine, ProblemDetail):
            return search_engine

        url = url_for(
            controller_name, library_short_name=library.short_name, _external=True
        )

        facets = load_facets_from_request(
            base_class=facet_class, default_entrypoint=EverythingEntryPoint
        )
        if isinstance(facets, ProblemDetail):
            return facets

        worklist = worklist_factory(library, facets)
        annotator = self.manager.annotator(worklist)

        # Since this feed will be consumed by an automated client, and
        # we're choosing titles for specific purposes, there's no
        # reason to put more than a single item in each group.
        pagination = Pagination(size=1)
        return feed_factory(
            _db=self._db,
            title=feed_title,
            url=url,
            pagination=pagination,
            worklist=worklist,
            annotator=annotator,
            search_engine=search_engine,
            facets=facets,
            max_age=0,
        )

    def qa_feed(self, feed_class=OPDSAcquisitionFeed):
        """Create an OPDS feed containing the information necessary to
        run a full set of integration tests against this server and
        the vendors it relies on.

        :param feed_class: Class to substitute for AcquisitionFeed during
            tests.
        """

        def factory(library, facets):
            return JackpotWorkList(library, facets)

        return self._qa_feed(
            feed_factory=feed_class.groups,
            feed_title="QA test feed",
            controller_name="qa_feed",
            facet_class=JackpotFacets,
            worklist_factory=factory,
        )

    def qa_series_feed(self, feed_class=OPDSAcquisitionFeed):
        """Create an OPDS feed containing books that belong to _some_
        series, without regard to _which_ series.

        :param feed_class: Class to substitute for AcquisitionFeed during
            tests.
        """

        def factory(library, facets):
            wl = WorkList()
            wl.initialize(library)
            return wl

        return self._qa_feed(
            feed_factory=feed_class.page,
            feed_title="QA series test feed",
            controller_name="qa_series_feed",
            facet_class=HasSeriesFacets,
            worklist_factory=factory,
        )
