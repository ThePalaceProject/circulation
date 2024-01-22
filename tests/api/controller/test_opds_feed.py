import json
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import quote_plus

import feedparser
from flask import url_for

from api.lanes import HasSeriesFacets, JackpotFacets, JackpotWorkList
from core.app_server import load_facets_from_request
from core.entrypoint import AudiobooksEntryPoint, EverythingEntryPoint
from core.external_search import SortKeyPagination
from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.annotator.circulation import LibraryAnnotator
from core.feed.navigation import NavigationFeed
from core.feed.opds import NavigationFacets
from core.lane import Facets, FeaturedFacets, Pagination, SearchFacets, WorkList
from core.model import Edition
from core.util.flask_util import Response
from tests.fixtures.api_controller import CirculationControllerFixture, WorkSpec
from tests.fixtures.library import LibraryFixture


class TestOPDSFeedController:
    """Test most of the methods of OPDSFeedController.

    Methods relating to crawlable feeds are tested in
    TestCrawlableFeed.
    """

    _EXTRA_BOOKS = [
        WorkSpec("english_2", "Totally American", "Uncle Sam", "eng", False),
        WorkSpec("french_1", "Très Français", "Marianne", "fre", False),
    ]

    groups_called_with: Any
    page_called_with: Any
    called_with: Any

    def test_feed(
        self,
        circulation_fixture: CirculationControllerFixture,
        library_fixture: LibraryFixture,
    ):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # Test the feed() method.

        # First, test some common error conditions.

        # Bad lane -> Problem detail
        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.opds_feeds.feed(-1)
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane"
                == response.uri
            )

        # Bad faceting information -> Problem detail
        lane_id = circulation_fixture.english_adult_fiction.id
        with circulation_fixture.request_context_with_library("/?order=nosuchorder"):
            response = circulation_fixture.manager.opds_feeds.feed(lane_id)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input"
                == response.uri
            )

        # Bad pagination -> Problem detail
        with circulation_fixture.request_context_with_library("/?size=abc"):
            response = circulation_fixture.manager.opds_feeds.feed(lane_id)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input"
                == response.uri
            )

        # Now let's make a real feed.

        # Set up configuration settings for links and entry points
        library = circulation_fixture.db.default_library()
        settings = library_fixture.settings(library)
        settings.terms_of_service = "a"  # type: ignore[assignment]
        settings.privacy_policy = "b"  # type: ignore[assignment]
        settings.copyright = "c"  # type: ignore[assignment]
        settings.about = "d"  # type: ignore[assignment]

        # Make a real OPDS feed and poke at it.
        with (
            circulation_fixture.request_context_with_library(
                "/?entrypoint=Book&size=10"
            ),
            circulation_fixture.wired_container(),
        ):
            response = circulation_fixture.manager.opds_feeds.feed(
                circulation_fixture.english_adult_fiction.id
            )

            # The mock search index returned every book it has, without
            # respect to which books _ought_ to show up on this page.
            #
            # So we'll need to do a more detailed test to make sure
            # the right arguments are being passed _into_ the search
            # index.

            assert 200 == response.status_code
            feed = feedparser.parse(response.data)
            assert {x.title for x in circulation_fixture.works} == {
                x["title"] for x in feed["entries"]
            }

            # But the rest of the feed looks good.
            links = feed["feed"]["links"]
            by_rel: dict[str, Any] = dict()

            # Put the links into a data structure based on their rel values.
            for i in links:
                rel = i["rel"]
                href = i["href"]
                if isinstance(by_rel.get(rel), (bytes, str)):
                    by_rel[rel] = [by_rel[rel]]
                if isinstance(by_rel.get(rel), list):
                    by_rel[rel].append(href)
                else:
                    by_rel[i["rel"]] = i["href"]

            assert "a" == by_rel["terms-of-service"]
            assert "b" == by_rel["privacy-policy"]
            assert "c" == by_rel["copyright"]
            assert "d" == by_rel["about"]

            next_link = by_rel["next"]
            lane_str = str(lane_id)
            assert lane_str in next_link
            assert "entrypoint=Book" in next_link
            assert "size=10" in next_link
            last_item = circulation_fixture.works[-1]

            # The pagination key for the next page is derived from the
            # sort fields of the last work in the current page.
            expected_pagination_key = [
                last_item.sort_title,
                last_item.sort_author,
                last_item.id,
            ]
            expect = "key=%s" % quote_plus(
                json.dumps(expected_pagination_key), safe=","
            )
            assert expect in next_link

            search_link = by_rel["search"]
            assert lane_str in search_link
            assert "entrypoint=Book" in search_link

            shelf_link = by_rel["http://opds-spec.org/shelf"]
            assert shelf_link.endswith("/loans/")

            facet_links = by_rel["http://opds-spec.org/facet"]
            assert all(lane_str in x for x in facet_links)
            assert all("entrypoint=Book" in x for x in facet_links)
            assert any("order=title" in x for x in facet_links)
            assert any("order=author" in x for x in facet_links)

        # Now let's take a closer look at what this controller method
        # passes into AcquisitionFeed.page(), by mocking page().
        class Mock:
            @classmethod
            def page(cls, **kwargs):
                self.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = Response("An OPDS feed")
                return resp

        sort_key = ["sort", "pagination", "key"]
        with circulation_fixture.request_context_with_library(
            "/?entrypoint=Audio&size=36&key=%s&order=added&max_age=10"
            % (json.dumps(sort_key))
        ):
            response = circulation_fixture.manager.opds_feeds.feed(
                circulation_fixture.english_adult_fiction.id, feed_class=Mock
            )

            # While we're in request context, generate the URL we
            # expect to be used for this feed.
            expect_url = url_for(
                "feed",
                lane_identifier=lane_id,
                library_short_name=circulation_fixture.db.default_library().short_name,
                _external=True,
            )

        assert isinstance(response, Response)
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Now check all the keyword arguments that were passed into
        # page().
        kwargs = self.called_with
        assert kwargs.pop("url") == expect_url
        assert circulation_fixture.db.session == kwargs.pop("_db")
        assert circulation_fixture.english_adult_fiction.display_name == kwargs.pop(
            "title"
        )
        assert circulation_fixture.english_adult_fiction == kwargs.pop("worklist")

        # Query string arguments were taken into account when
        # creating the Facets and Pagination objects.
        facets = kwargs.pop("facets")
        assert AudiobooksEntryPoint == facets.entrypoint
        assert "added" == facets.order

        pagination = kwargs.pop("pagination")
        assert isinstance(pagination, SortKeyPagination)
        assert 36 == pagination.size
        assert sort_key == pagination.last_item_on_previous_page

        # The Annotator object was instantiated with the proper lane
        # and the newly created Facets object.
        annotator = kwargs.pop("annotator")
        assert circulation_fixture.english_adult_fiction == annotator.lane
        assert facets == annotator.facets

        # The ExternalSearchIndex associated with the
        # CirculationManager was passed in; that way we don't have to
        # connect to the search engine again.
        assert circulation_fixture.manager.external_search == kwargs.pop(
            "search_engine"
        )

        # No other arguments were passed into page().
        assert {} == kwargs

    def test_groups(
        self,
        circulation_fixture: CirculationControllerFixture,
        library_fixture: LibraryFixture,
    ):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # AcquisitionFeed.groups is tested in core/test_opds.py, and a
        # full end-to-end test would require setting up a real search
        # index, so we're just going to test that groups() (or, in one
        # case, page()) is called properly.
        library = circulation_fixture.db.default_library()
        settings = library_fixture.settings(library)
        settings.minimum_featured_quality = 0.15  # type: ignore[assignment]
        settings.featured_lane_size = 2

        # Patron with root lane -> redirect to root lane
        lane = circulation_fixture.db.lane()
        lane.root_for_patron_type = ["1"]
        circulation_fixture.default_patron.external_type = "1"
        auth = dict(Authorization=circulation_fixture.valid_auth)
        with circulation_fixture.request_context_with_library("/", headers=auth):
            controller = circulation_fixture.manager.opds_feeds
            response = controller.groups(None)
            assert 302 == response.status_code
            expect_url = url_for(
                "acquisition_groups",
                library_short_name=circulation_fixture.db.default_library().short_name,
                lane_identifier=lane.id,
                _external=True,
            )
            assert response.headers["Location"] == expect_url

        # Bad lane -> Problem detail
        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.opds_feeds.groups(-1)
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane"
                == response.uri
            )

        # A grouped feed has no pagination, and the FeaturedFacets
        # constructor never raises an exception. So we don't need to
        # test for those error conditions.

        # Now let's see what goes into groups()
        class Mock:
            @classmethod
            def groups(cls, **kwargs):
                # This method ends up being called most of the time
                # the grouped feed controller is activated.
                self.groups_called_with = kwargs
                self.page_called_with = None
                resp = MagicMock()
                resp.as_response.return_value = Response("A grouped feed")
                return resp

            @classmethod
            def page(cls, **kwargs):
                # But for lanes that have no children, this method
                # ends up being called instead.
                self.groups_called_with = None
                self.page_called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = Response("A paginated feed")
                return resp

        # Earlier we tested an authenticated request for a patron with an
        # external type. Now try an authenticated request for a patron with
        # no external type, just to verify that nothing unusual happens
        # for that kind of patron.
        circulation_fixture.default_patron.external_type = None
        with circulation_fixture.request_context_with_library(
            "/?entrypoint=Audio", headers=auth
        ):
            # In default_config, there are no LARGE_COLLECTION_LANGUAGES,
            # so the sole top-level lane is "World Languages", which covers the
            # SMALL and TINY_COLLECTION_LANGUAGES.
            #
            # Thus, when we pass lane=None into groups(), we're asking for a
            # feed for the sole top-level lane, "World Languages".
            expect_lane = circulation_fixture.manager.opds_feeds.load_lane(None)
            assert "World Languages" == expect_lane.display_name

            # Ask for that feed.
            response = circulation_fixture.manager.opds_feeds.groups(
                None, feed_class=Mock
            )

            # The Response returned by Mock.groups() has been converted
            # into a Flask response.
            assert "A grouped feed" == response.get_data(as_text=True)

            # While we're in request context, generate the URL we
            # expect to be used for this feed.
            expect_url = url_for(
                "acquisition_groups",
                lane_identifier=None,
                library_short_name=library.short_name,
                _external=True,
            )

        kwargs = self.groups_called_with
        assert circulation_fixture.db.session == kwargs.pop("_db")
        lane = kwargs.pop("worklist")
        assert expect_lane == lane
        assert lane.display_name == kwargs.pop("title")
        assert expect_url == kwargs.pop("url")

        # A FeaturedFacets object was loaded from library, lane and
        # request configuration.
        facets = kwargs.pop("facets")
        assert isinstance(facets, FeaturedFacets)
        assert AudiobooksEntryPoint == facets.entrypoint
        assert 0.15 == facets.minimum_featured_quality

        # A LibraryAnnotator object was created from the Lane and
        # Facets objects.
        annotator = kwargs.pop("annotator")
        assert lane == annotator.lane
        assert facets == annotator.facets

        # Finally, let's try again with a specific lane rather than
        # None.

        # This lane has no sublanes, so our call to groups()
        # is going to become a call to page().
        with circulation_fixture.request_context_with_library("/?entrypoint=Audio"):
            response = circulation_fixture.manager.opds_feeds.groups(
                circulation_fixture.english_adult_fiction.id, feed_class=Mock
            )

            # While we're in request context, generate the URL we
            # expect to be used for this feed.
            expect_url = url_for(
                "feed",
                lane_identifier=circulation_fixture.english_adult_fiction.id,
                library_short_name=library.short_name,
                _external=True,
            )

        assert circulation_fixture.english_adult_fiction == self.page_called_with.pop(
            "worklist"
        )

        # The canonical URL for this feed is a page-type URL, not a
        # groups-type URL.
        assert expect_url == self.page_called_with.pop("url")

        # The faceting and pagination objects are typical for the
        # first page of a paginated feed.
        pagination = self.page_called_with.pop("pagination")
        assert isinstance(pagination, SortKeyPagination)
        facets = self.page_called_with.pop("facets")
        assert isinstance(facets, Facets)

        # groups() was never called.
        assert None == self.groups_called_with

        # Give this lane a sublane, and the call to groups() goes
        # through as normal.
        sublane = circulation_fixture.db.lane(
            parent=circulation_fixture.english_adult_fiction
        )
        with circulation_fixture.request_context_with_library("/?entrypoint=Audio"):
            response = circulation_fixture.manager.opds_feeds.groups(
                circulation_fixture.english_adult_fiction.id, feed_class=Mock
            )
        assert None == self.page_called_with
        assert circulation_fixture.english_adult_fiction == self.groups_called_with.pop(
            "worklist"
        )
        assert isinstance(self.groups_called_with.pop("facets"), FeaturedFacets)
        assert "pagination" not in self.groups_called_with

    def test_navigation(self, circulation_fixture: CirculationControllerFixture):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        library = circulation_fixture.db.default_library()
        lane = circulation_fixture.manager.top_level_lanes[library.id]
        lane = circulation_fixture.db.session.merge(lane)

        # Mock NavigationFeed.navigation so we can see the arguments going
        # into it.
        old_navigation = NavigationFeed.navigation

        def mock_navigation(*args, **kwargs):
            self.called_with = (args, kwargs)
            return old_navigation(*args, **kwargs)

        NavigationFeed.navigation = mock_navigation  # type: ignore

        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.opds_feeds.navigation(lane.id)

            feed = feedparser.parse(response.data)
            entries = feed["entries"]
            # The default top-level lane is "World Languages", which contains
            # sublanes for English, Spanish, Chinese, and French.
            assert len(lane.sublanes) == len(entries)

        # A NavigationFacets object was created and passed in to
        # NavigationFeed.navigation().
        args, kwargs = self.called_with
        facets = kwargs["facets"]
        assert isinstance(facets, NavigationFacets)
        NavigationFeed.navigation = old_navigation  # type: ignore

    def mock_search(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_search_document(self, circulation_fixture: CirculationControllerFixture):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # When you invoke the search controller but don't specify a search
        # term, you get an OpenSearch document.
        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.opds_feeds.search(None)
            assert (
                response.headers["Content-Type"]
                == "application/opensearchdescription+xml"
            )
            assert "OpenSearchDescription" in response.get_data(as_text=True)

    def test_search(
        self,
        circulation_fixture: CirculationControllerFixture,
        library_fixture: LibraryFixture,
    ):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # Test the search() controller method.

        # Bad lane -> problem detail
        with circulation_fixture.request_context_with_library("/"):
            response = circulation_fixture.manager.opds_feeds.search(-1)
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane"
                == response.uri
            )

        # Bad pagination -> problem detail
        with circulation_fixture.request_context_with_library("/?size=abc"):
            response = circulation_fixture.manager.opds_feeds.search(None)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input"
                == response.uri
            )

        # Loading the SearchFacets object from a request can't return
        # a problem detail, so we can't test that case.

        # The AcquisitionFeed.search method is tested in core, so we're
        # just going to test that appropriate values are passed into that
        # method:

        class Mock:
            @classmethod
            def search(cls, **kwargs):
                self.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = "An OPDS feed"
                return resp

        with circulation_fixture.request_context_with_library(
            "/?q=t&size=99&after=22&media=Music"
        ):
            # Try the top-level lane, "World Languages"
            expect_lane = circulation_fixture.manager.opds_feeds.load_lane(None)
            response = circulation_fixture.manager.opds_feeds.search(
                None, feed_class=Mock
            )

        kwargs = self.called_with
        assert circulation_fixture.db.session == kwargs.pop("_db")

        # Unlike other types of feeds, here the argument is called
        # 'lane' instead of 'worklist', because a Lane is the _only_
        # kind of WorkList that is currently searchable.
        lane = kwargs.pop("lane")
        assert expect_lane == lane
        query = kwargs.pop("query")
        assert "t" == query
        assert "Search" == kwargs.pop("title")
        assert circulation_fixture.manager.external_search == kwargs.pop(
            "search_engine"
        )

        # A SearchFacets object was loaded from library, lane and
        # request configuration.
        facets = kwargs.pop("facets")
        assert isinstance(facets, SearchFacets)

        # There are multiple possible entry points, and the request
        # didn't specify, so the SearchFacets object is configured to
        # search all of them.
        assert EverythingEntryPoint == facets.entrypoint

        # The "media" query string parameter -- used only by
        # SearchFacets -- was picked up.
        assert [Edition.MUSIC_MEDIUM] == facets.media

        # Information from the query string was used to make a
        # Pagination object.
        pagination = kwargs.pop("pagination")
        assert 22 == pagination.offset
        assert 99 == pagination.size

        # A LibraryAnnotator object was created from the Lane and
        # Facets objects.
        annotator = kwargs.pop("annotator")
        assert lane == annotator.lane
        assert facets == annotator.facets

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the SearchFacets object created during the
        # original request.
        library = circulation_fixture.db.default_library()
        with circulation_fixture.request_context_with_library(""):
            expect_url = url_for(
                "lane_search",
                lane_identifier=None,
                library_short_name=library.short_name,
                **dict(list(facets.items())),
                q=query,
                _external=True,
            )
        assert expect_url == kwargs.pop("url")

        # No other arguments were passed into search().
        assert {} == kwargs

        # When a specific entry point is selected, the SearchFacets
        # object is configured with that entry point alone.
        with circulation_fixture.request_context_with_library("/?entrypoint=Audio&q=t"):
            # Search a specific lane rather than the top-level.
            response = circulation_fixture.manager.opds_feeds.search(
                circulation_fixture.english_adult_fiction.id, feed_class=Mock
            )
            kwargs = self.called_with

            # We're searching that lane.
            assert circulation_fixture.english_adult_fiction == kwargs["lane"]

            # And we get the entry point we asked for.
            assert AudiobooksEntryPoint == kwargs["facets"].entrypoint

        # When only a single entry point is enabled, it's used as the
        # default.
        library_fixture.settings(library).enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME
        ]
        with circulation_fixture.request_context_with_library("/?q=t"):
            response = circulation_fixture.manager.opds_feeds.search(
                None, feed_class=Mock
            )
            assert AudiobooksEntryPoint == self.called_with["facets"].entrypoint

        with circulation_fixture.request_context_with_library("/?q=t&search_type=json"):
            response = circulation_fixture.manager.opds_feeds.search(
                None, feed_class=Mock
            )
            assert self.called_with["facets"].search_type == "json"

    def test_lane_search_params(
        self,
        circulation_fixture: CirculationControllerFixture,
    ):
        # Tests some of the lane search parameters.
        # TODO: Add test for valid `distributor`.

        valid_lane_id = circulation_fixture.english_adult_fiction.id
        valid_collection_name = circulation_fixture.collection.name
        invalid_collection_name = "__non-existent-collection__"
        invalid_distributor = "__non-existent-distributor__"

        with circulation_fixture.request_context_with_library("/?collectionName=All"):
            response = circulation_fixture.manager.opds_feeds.search(valid_lane_id)
            assert 200 == response.status_code
            assert "application/opensearchdescription+xml" == response.headers.get(
                "content-type"
            )

        with circulation_fixture.request_context_with_library(
            f"/?collectionName={valid_collection_name}"
        ):
            response = circulation_fixture.manager.opds_feeds.search(valid_lane_id)
            assert 200 == response.status_code
            assert "application/opensearchdescription+xml" == response.headers.get(
                "content-type"
            )

        with circulation_fixture.request_context_with_library(
            f"/?collectionName={invalid_collection_name}"
        ):
            response = circulation_fixture.manager.opds_feeds.search(valid_lane_id)
            assert 400 == response.status_code
            assert (
                f"I don't understand which collection '{invalid_collection_name}' refers to."
                == response.detail
            )

        with circulation_fixture.request_context_with_library("/?distributor=All"):
            response = circulation_fixture.manager.opds_feeds.search(valid_lane_id)
            assert 200 == response.status_code
            assert "application/opensearchdescription+xml" == response.headers.get(
                "content-type"
            )

        with circulation_fixture.request_context_with_library(
            f"/?distributor={invalid_distributor}"
        ):
            response = circulation_fixture.manager.opds_feeds.search(valid_lane_id)
            assert 400 == response.status_code
            assert (
                f"I don't understand which distributor '{invalid_distributor}' refers to."
                == response.detail
            )

    def test__qa_feed(self, circulation_fixture: CirculationControllerFixture):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # Test the _qa_feed() controller method.

        # First, mock the hook functions that do the actual work.
        wl = WorkList()
        wl.initialize(circulation_fixture.library)
        worklist_factory = MagicMock(return_value=wl)
        feed_method = MagicMock(return_value="an OPDS feed")

        m = circulation_fixture.manager.opds_feeds._qa_feed
        args = (feed_method, "QA test feed", "qa_feed", Facets, worklist_factory)

        # Bad faceting information -> Problem detail
        with circulation_fixture.request_context_with_library("/?order=nosuchorder"):
            response = m(*args)
            assert 400 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/invalid-input"
                == response.uri
            )

        # Now test success.
        with circulation_fixture.request_context_with_library("/"):
            expect_url = url_for(
                "qa_feed",
                library_short_name=circulation_fixture.db.default_library().short_name,
                _external=True,
            )

            response = m(*args)

        # The response is the return value of feed_method().
        assert "an OPDS feed" == response

        # The worklist factory was called once, with the Library
        # associated with the request and a freshly created Facets
        # object.
        [factory_call] = worklist_factory.mock_calls
        (library, facets) = factory_call.args
        assert circulation_fixture.db.default_library() == library
        assert isinstance(facets, Facets)
        assert EverythingEntryPoint == facets.entrypoint

        # feed_method was called once, with a variety of arguments.
        [call] = feed_method.mock_calls
        kwargs = call.kwargs

        assert circulation_fixture.db.session == kwargs.pop("_db")  # type: ignore
        assert "QA test feed" == kwargs.pop("title")  # type: ignore
        assert circulation_fixture.manager.external_search == kwargs.pop("search_engine")  # type: ignore
        assert expect_url == kwargs.pop("url")  # type: ignore

        # These feeds are never to be cached.
        assert 0 == kwargs.pop("max_age")  # type: ignore

        # To improve performance, a Pagination object was created that
        # limits each lane in the test feed to a single Work.
        pagination = kwargs.pop("pagination")  # type: ignore
        assert isinstance(pagination, Pagination)
        assert 1 == pagination.size

        # The WorkList returned by worklist_factory was passed into
        # feed_method.
        assert wl == kwargs.pop("worklist")  # type: ignore

        # So was a LibraryAnnotator object created from that WorkList.
        annotator = kwargs.pop("annotator")  # type: ignore
        assert isinstance(annotator, LibraryAnnotator)
        assert wl == annotator.lane
        assert None == annotator.facets

        # The Facets object used to initialize the feed is the same
        # one passed into worklist_factory.
        assert facets == kwargs.pop("facets")  # type: ignore

        # No other arguments were passed into feed_method().
        assert {} == kwargs

    def test_qa_feed(self, circulation_fixture: CirculationControllerFixture):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # Verify that the qa_feed controller creates a factory for a
        # JackpotWorkList and passes it into _qa_feed.

        mock = MagicMock(return_value="an OPDS feed")
        circulation_fixture.manager.opds_feeds._qa_feed = mock

        response = circulation_fixture.manager.opds_feeds.qa_feed()
        [call] = mock.mock_calls
        kwargs = call.kwargs

        # For the most part, we're verifying that the expected values
        # are passed in to _qa_feed.
        assert OPDSAcquisitionFeed.groups == kwargs.pop("feed_factory")  # type: ignore
        assert JackpotFacets == kwargs.pop("facet_class")  # type: ignore
        assert "qa_feed" == kwargs.pop("controller_name")  # type: ignore
        assert "QA test feed" == kwargs.pop("feed_title")  # type: ignore
        factory = kwargs.pop("worklist_factory")  # type: ignore
        assert {} == kwargs

        # However, one of those expected values is a function. We need
        # to call that function to verify that it builds the
        # JackpotWorkList that distinguishes this _qa_feed call from
        # other calls.
        with circulation_fixture.request_context_with_library("/"):
            facets = load_facets_from_request(
                base_class=JackpotFacets, default_entrypoint=EverythingEntryPoint
            )

        worklist = factory(circulation_fixture.db.default_library(), facets)
        assert isinstance(worklist, JackpotWorkList)

        # Each child of the JackpotWorkList is based on the
        # JackpotFacets object we passed in to the factory method.
        for child in worklist.children:
            assert facets == child.facets

    def test_qa_feed2(self, circulation_fixture: CirculationControllerFixture):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # Verify that the qa_feed controller creates a factory for a
        # JackpotWorkList and passes it into _qa_feed.

        mock = MagicMock(return_value="an OPDS feed")
        circulation_fixture.manager.opds_feeds._qa_feed = mock

        response = circulation_fixture.manager.opds_feeds.qa_feed()
        [call] = mock.mock_calls
        kwargs = call.kwargs

        # For the most part, we're verifying that the expected values
        # are passed in to _qa_feed.
        assert OPDSAcquisitionFeed.groups == kwargs.pop("feed_factory")  # type: ignore
        assert JackpotFacets == kwargs.pop("facet_class")  # type: ignore
        assert "qa_feed" == kwargs.pop("controller_name")  # type: ignore
        assert "QA test feed" == kwargs.pop("feed_title")  # type: ignore
        factory = kwargs.pop("worklist_factory")  # type: ignore
        assert {} == kwargs

        # However, one of those expected values is a function. We need
        # to call that function to verify that it builds the
        # JackpotWorkList that distinguishes this _qa_feed call from
        # other calls.
        with circulation_fixture.request_context_with_library("/"):
            facets = load_facets_from_request(
                base_class=JackpotFacets, default_entrypoint=EverythingEntryPoint
            )

        worklist = factory(circulation_fixture.db.default_library(), facets)
        assert isinstance(worklist, JackpotWorkList)

        # Each child of the JackpotWorkList is based on the
        # JackpotFacets object we passed in to the factory method.
        for child in worklist.children:
            assert facets == child.facets

    def test_qa_series_feed(self, circulation_fixture: CirculationControllerFixture):
        circulation_fixture.add_works(self._EXTRA_BOOKS)

        # Verify that the qa_series_feed controller creates a factory
        # for a generic WorkList and passes it into _qa_feed with
        # instructions to use HasSeriesFacets.

        mock = MagicMock(return_value="an OPDS feed")
        circulation_fixture.manager.opds_feeds._qa_feed = mock

        response = circulation_fixture.manager.opds_feeds.qa_series_feed()
        [call] = mock.mock_calls
        kwargs = call.kwargs

        # For the most part, we're verifying that the expected values
        # are passed in to _qa_feed.

        # Note that the feed_method is different from the one in qa_feed.
        # We want to generate an ungrouped feed rather than a grouped one.
        assert OPDSAcquisitionFeed.page == kwargs.pop("feed_factory")  # type: ignore
        assert HasSeriesFacets == kwargs.pop("facet_class")  # type: ignore
        assert "qa_series_feed" == kwargs.pop("controller_name")  # type: ignore
        assert "QA series test feed" == kwargs.pop("feed_title")  # type: ignore
        factory = kwargs.pop("worklist_factory")  # type: ignore
        assert {} == kwargs

        # One of those expected values is a function. We need to call
        # that function to verify that it builds a generic WorkList
        # with no special features. Unlike with qa_feed, the
        # HasSeriesFacets object is not used to build the WorkList;
        # instead it directly modifies the Filter object used to
        # generate the query.
        worklist = factory(circulation_fixture.db.default_library(), object())
        assert isinstance(worklist, WorkList)
        assert circulation_fixture.db.default_library().id == worklist.library_id
