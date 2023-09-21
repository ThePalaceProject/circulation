import json
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import feedparser
from flask import url_for
from opensearch_dsl.response.hit import Hit

from api.lanes import (
    CrawlableCollectionBasedLane,
    CrawlableCustomListBasedLane,
    CrawlableFacets,
    DynamicLane,
)
from api.problem_details import NO_SUCH_COLLECTION, NO_SUCH_LIST
from core.external_search import SortKeyPagination
from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.annotator.circulation import CirculationManagerAnnotator
from core.problem_details import INVALID_INPUT
from core.util.flask_util import Response
from core.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import CirculationControllerFixture


class TestCrawlableFeed:
    @contextmanager
    def mock_crawlable_feed(self, circulation_fixture: CirculationControllerFixture):
        """Temporarily mock _crawlable_feed with something
        that records the arguments used to call it.
        """
        controller = circulation_fixture.manager.opds_feeds
        original = controller._crawlable_feed

        def mock(title, url, worklist, annotator=None, feed_class=OPDSAcquisitionFeed):
            self._crawlable_feed_called_with = dict(
                title=title,
                url=url,
                worklist=worklist,
                annotator=annotator,
                feed_class=feed_class,
            )
            return "An OPDS feed."

        controller._crawlable_feed = mock
        yield
        controller._crawlable_feed = original

    def test_crawlable_library_feed(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Test the creation of a crawlable feed for everything in
        # a library.
        controller = circulation_fixture.manager.opds_feeds
        library = circulation_fixture.db.default_library()
        with circulation_fixture.request_context_with_library("/"):
            with self.mock_crawlable_feed(circulation_fixture):
                response = controller.crawlable_library_feed()
                expect_url = url_for(
                    "crawlable_library_feed",
                    library_short_name=library.short_name,
                    _external=True,
                )

        # The response of the mock _crawlable_feed was returned as-is;
        # creating a proper Response object is the job of the real
        # _crawlable_feed.
        assert "An OPDS feed." == response

        # Verify that _crawlable_feed was called with the right arguments.
        kwargs = self._crawlable_feed_called_with
        assert expect_url == kwargs.pop("url")
        assert library.name == kwargs.pop("title")
        assert None == kwargs.pop("annotator")
        assert OPDSAcquisitionFeed == kwargs.pop("feed_class")

        # A CrawlableCollectionBasedLane has been set up to show
        # everything in any of the requested library's collections.
        lane = kwargs.pop("worklist")
        assert isinstance(lane, CrawlableCollectionBasedLane)
        assert library.id == lane.library_id
        assert [x.id for x in library.collections] == lane.collection_ids
        assert {} == kwargs

    def test_crawlable_collection_feed(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Test the creation of a crawlable feed for everything in
        # a collection.
        controller = circulation_fixture.manager.opds_feeds
        library = circulation_fixture.db.default_library()

        collection = circulation_fixture.db.collection()

        # Bad collection name -> Problem detail.
        with circulation_fixture.app.test_request_context("/"):
            response = controller.crawlable_collection_feed(
                collection_name="No such collection"
            )
            assert NO_SUCH_COLLECTION == response

        # Unlike most of these controller methods, this one does not
        # require a library context.
        with circulation_fixture.app.test_request_context("/"):
            with self.mock_crawlable_feed(circulation_fixture):
                response = controller.crawlable_collection_feed(
                    collection_name=collection.name
                )
                expect_url = url_for(
                    "crawlable_collection_feed",
                    collection_name=collection.name,
                    _external=True,
                )

        # The response of the mock _crawlable_feed was returned as-is;
        # creating a proper Response object is the job of the real
        # _crawlable_feed.
        assert "An OPDS feed." == response

        # Verify that _crawlable_feed was called with the right arguments.
        kwargs = self._crawlable_feed_called_with
        assert expect_url == kwargs.pop("url")
        assert collection.name == kwargs.pop("title")

        # A CrawlableCollectionBasedLane has been set up to show
        # everything in the requested collection.
        lane = kwargs.pop("worklist")
        assert isinstance(lane, CrawlableCollectionBasedLane)
        assert None == lane.library_id
        assert [collection.id] == lane.collection_ids

        # No specific Annotator as created to build the OPDS
        # feed. We'll be using the default for a request with no
        # library context--a CirculationManagerAnnotator.
        assert None == kwargs.pop("annotator")

    def test_crawlable_list_feed(
        self, circulation_fixture: CirculationControllerFixture
    ):
        # Test the creation of a crawlable feed for everything in
        # a custom list.
        controller = circulation_fixture.manager.opds_feeds
        library = circulation_fixture.db.default_library()

        customlist, ignore = circulation_fixture.db.customlist(num_entries=0)
        customlist.library = library

        other_list, ignore = circulation_fixture.db.customlist(num_entries=0)

        # List does not exist, or not associated with library ->
        # ProblemDetail
        for bad_name in ("Nonexistent list", other_list.name):
            with circulation_fixture.request_context_with_library("/"):
                with self.mock_crawlable_feed(circulation_fixture):
                    response = controller.crawlable_list_feed(bad_name)
                    assert NO_SUCH_LIST == response

        with circulation_fixture.request_context_with_library("/"):
            with self.mock_crawlable_feed(circulation_fixture):
                response = controller.crawlable_list_feed(customlist.name)
                expect_url = url_for(
                    "crawlable_list_feed",
                    list_name=customlist.name,
                    library_short_name=library.short_name,
                    _external=True,
                )

        # The response of the mock _crawlable_feed was returned as-is;
        # creating a proper Response object is the job of the real
        # _crawlable_feed.
        assert "An OPDS feed." == response

        # Verify that _crawlable_feed was called with the right arguments.
        kwargs = self._crawlable_feed_called_with
        assert expect_url == kwargs.pop("url")
        assert customlist.name == kwargs.pop("title")
        assert None == kwargs.pop("annotator")
        assert OPDSAcquisitionFeed == kwargs.pop("feed_class")

        # A CrawlableCustomListBasedLane was created to fetch only
        # the works in the custom list.
        lane = kwargs.pop("worklist")
        assert isinstance(lane, CrawlableCustomListBasedLane)
        assert [customlist.id] == lane.customlist_ids
        assert {} == kwargs

    def test__crawlable_feed(self, circulation_fixture: CirculationControllerFixture):
        # Test the helper method called by all other feed methods.
        self.page_called_with: Any = None

        class MockFeed:
            @classmethod
            def page(cls, **kwargs):
                self.page_called_with = kwargs
                feed = MagicMock()
                feed.as_response.return_value = Response("An OPDS feed")
                return feed

        work = circulation_fixture.db.work(with_open_access_download=True)

        class MockLane(DynamicLane):
            def works(self, _db, facets, pagination, *args, **kwargs):
                # We need to call page_loaded() (normally called by
                # the search engine after obtaining real search
                # results), because OPDSFeed.page will call it if it
                # wasn't already called.
                #
                # It's not necessary for this test to call it with a
                # realistic value, but we might as well.
                results = [
                    Hit(
                        {
                            "_source": {
                                "work_id": work.id,
                            },
                            "_sort": [work.sort_title, work.sort_author, work.id],
                        }
                    )
                ]
                pagination.page_loaded(results)
                return [work]

        mock_lane = MockLane()
        mock_lane.initialize(None)
        in_kwargs = dict(
            title="Lane title", url="Lane URL", worklist=mock_lane, feed_class=MockFeed
        )

        # Bad pagination data -> problem detail
        with circulation_fixture.app.test_request_context("/?size=a"):
            response = circulation_fixture.manager.opds_feeds._crawlable_feed(
                **in_kwargs
            )
            assert isinstance(response, ProblemDetail)
            assert INVALID_INPUT.uri == response.uri
            assert None == self.page_called_with

        # Bad search engine -> problem detail
        circulation_fixture.assert_bad_search_index_gives_problem_detail(
            lambda: circulation_fixture.manager.opds_feeds._crawlable_feed(**in_kwargs)
        )

        # Good pagination data -> feed_class.page() is called.
        sort_key = ["sort", "pagination", "key"]
        with circulation_fixture.app.test_request_context(
            "/?size=23&key=%s" % json.dumps(sort_key)
        ):
            response = circulation_fixture.manager.opds_feeds._crawlable_feed(
                **in_kwargs
            )

        # The result of page() was served as an OPDS feed.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Verify the arguments passed in to page().
        out_kwargs = self.page_called_with
        assert circulation_fixture.db.session == out_kwargs.pop("_db")
        assert circulation_fixture.manager.opds_feeds.search_engine == out_kwargs.pop(
            "search_engine"
        )
        assert in_kwargs["worklist"] == out_kwargs.pop("worklist")
        assert in_kwargs["title"] == out_kwargs.pop("title")
        assert in_kwargs["url"] == out_kwargs.pop("url")

        # Since no annotator was provided and the request did not
        # happen in a library context, a generic
        # CirculationManagerAnnotator was created.
        annotator = out_kwargs.pop("annotator")
        assert isinstance(annotator, CirculationManagerAnnotator)
        assert mock_lane == annotator.lane

        # There's only one way to configure CrawlableFacets, so it's
        # sufficient to check that our faceting object is in fact a
        # CrawlableFacets.
        facets = out_kwargs.pop("facets")
        assert isinstance(facets, CrawlableFacets)

        # Verify that pagination was picked up from the request.
        pagination = out_kwargs.pop("pagination")
        assert isinstance(pagination, SortKeyPagination)
        assert sort_key == pagination.last_item_on_previous_page
        assert 23 == pagination.size

        # We're done looking at the arguments.
        assert {} == out_kwargs

        # If a custom Annotator is passed in to _crawlable_feed, it's
        # propagated to the page() call.
        mock_annotator = object()
        with circulation_fixture.app.test_request_context("/"):
            response = circulation_fixture.manager.opds_feeds._crawlable_feed(
                annotator=mock_annotator, **in_kwargs
            )
            assert mock_annotator == self.page_called_with["annotator"]

        # Finally, remove the mock feed class and verify that a real OPDS
        # feed is generated from the result of MockLane.works()
        del in_kwargs["feed_class"]
        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.opds_feeds._crawlable_feed(
                **in_kwargs
            )
        feed = feedparser.parse(response.data)

        # There is one entry with the expected title.
        [entry] = feed["entries"]
        assert entry["title"] == work.title
