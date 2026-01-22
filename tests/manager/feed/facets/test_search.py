import logging

from palace.manager.core.entrypoint import (
    AudiobooksEntryPoint,
    EbooksEntryPoint,
    EverythingEntryPoint,
)
from palace.manager.feed.facets.feed import Facets
from palace.manager.feed.facets.search import SearchFacets
from palace.manager.search.filter import Filter
from palace.manager.sqlalchemy.model.edition import Edition
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.library import LibraryFixture
from tests.mocks.mock import LogCaptureHandler


class TestSearchFacets:
    def test_constructor(self):
        # The SearchFacets constructor allows you to specify
        # a medium and language (or a list of them) as well
        # as an entrypoint.

        m = SearchFacets

        # If you don't pass any information in, you get a SearchFacets
        # that does nothing.
        defaults = m()
        assert None == defaults.entrypoint
        assert None == defaults.languages
        assert None == defaults.media
        assert m.ORDER_BY_RELEVANCE == defaults.order
        assert None == defaults.min_score
        assert False == defaults._language_from_query

        mock_entrypoint = object()

        # If you pass in a single value for medium or language
        # they are turned into a list.
        with_single_value = m(
            entrypoint=mock_entrypoint,
            media=Edition.BOOK_MEDIUM,
            languages="eng",
            language_from_query=True,
        )
        assert mock_entrypoint == with_single_value.entrypoint
        assert [Edition.BOOK_MEDIUM] == with_single_value.media
        assert ["eng"] == with_single_value.languages
        assert True == with_single_value._language_from_query

        # If you pass in a list of values, it's left alone.
        media = [Edition.BOOK_MEDIUM, Edition.AUDIO_MEDIUM]
        languages = ["eng", "spa"]
        with_multiple_values = m(media=media, languages=languages)
        assert media == with_multiple_values.media
        assert languages == with_multiple_values.languages

        # The only exception is if you pass in Edition.ALL_MEDIUM
        # as 'medium' -- that's passed through as is.
        every_medium = m(media=Edition.ALL_MEDIUM)
        assert Edition.ALL_MEDIUM == every_medium.media

        # Pass in a value for min_score, and it's stored for later.
        mock_min_score = object()
        with_min_score = m(min_score=mock_min_score)
        assert mock_min_score == with_min_score.min_score

        # Pass in a value for order, and you automatically get a
        # reasonably tight value for min_score.
        order = object()
        with_order = m(order=order)
        assert order == with_order.order
        assert SearchFacets.DEFAULT_MIN_SCORE == with_order.min_score

    def test_from_request(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # An HTTP client can customize which SearchFacets object
        # is created by sending different HTTP requests.

        # These variables mock the query string arguments and
        # HTTP headers of an HTTP request.
        arguments = dict(
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
            media=Edition.AUDIO_MEDIUM,
            min_score="123",
        )
        headers = {"Accept-Language": "da, en-gb;q=0.8"}
        get_argument = arguments.get
        get_header = headers.get

        unused = object()

        library_settings = library_fixture.mock_settings()
        library_settings.enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        library = db.library(settings=library_settings)

        def from_request(**extra):
            return SearchFacets.from_request(
                library,
                library,
                get_argument,
                get_header,
                unused,
                **extra,
            )

        facets = from_request(extra="value")
        assert (
            dict(extra="value", language_from_query=False) == facets.constructor_kwargs
        )

        # The superclass's from_request implementation pulled the
        # requested EntryPoint out of the request.
        assert EbooksEntryPoint == facets.entrypoint

        # The SearchFacets implementation pulled the 'media' query
        # string argument.
        #
        # The medium from the 'media' argument contradicts the medium
        # implied by the entry point, but that's not our problem.
        assert [Edition.AUDIO_MEDIUM] == facets.media

        # The SearchFacets implementation turned the 'min_score'
        # argument into a numeric minimum score.
        assert 123 == facets.min_score

        # The SearchFacets implementation turned the 'Accept-Language'
        # header into a set of language codes.
        assert ["dan", "eng"] == facets.languages
        assert False == facets._language_from_query

        # Try again with bogus media, languages, and minimum score.
        arguments["media"] = "Unknown Media"
        arguments["min_score"] = "not a number"
        headers["Accept-Language"] = "xx, ql"

        # None of the bogus information was used.
        facets = from_request()
        assert None == facets.media
        assert None == facets.languages
        assert None == facets.min_score

        # Reading the language from the query, with a search type
        arguments["language"] = "all"
        arguments["search_type"] = "json"
        headers["Accept-Language"] = "da, en-gb;q=0.8"

        facets = from_request()
        assert ["all"] == facets.languages
        assert True == facets._language_from_query
        assert "json" == facets.search_type

        # Try again with no information.
        del arguments["media"]
        del arguments["language"]
        del headers["Accept-Language"]

        facets = from_request()
        assert None == facets.media
        assert None == facets.languages

    def test_from_request_from_admin_search(
        self, db: DatabaseTransactionFixture, library_fixture: LibraryFixture
    ):
        # If the SearchFacets object is being created by a search run from the admin interface,
        # there might be order and language arguments which should be used to filter search results.

        arguments = dict(
            order="author",
            language="fre",
            entrypoint=EbooksEntryPoint.INTERNAL_NAME,
            media=Edition.AUDIO_MEDIUM,
            min_score="123",
        )
        headers = {"Accept-Language": "da, en-gb;q=0.8"}
        get_argument = arguments.get
        get_header = headers.get

        unused = object()

        library_settings = library_fixture.mock_settings()
        library_settings.enabled_entry_points = [
            AudiobooksEntryPoint.INTERNAL_NAME,
            EbooksEntryPoint.INTERNAL_NAME,
        ]
        library = library_fixture.library(settings=library_settings)

        def from_request(**extra):
            return SearchFacets.from_request(
                library,
                library,
                get_argument,
                get_header,
                unused,
                **extra,
            )

        facets = from_request(extra="value")
        # The SearchFacets implementation uses the order and language values submitted by the admin.
        assert "author" == facets.order
        assert ["fre"] == facets.languages

    def test_selectable_entrypoints(self):
        """If the WorkList has more than one facet, an 'everything' facet
        is added for search purposes.
        """

        class MockWorkList:
            def __init__(self):
                self.entrypoints = None

        ep1 = object()
        ep2 = object()
        worklist = MockWorkList()

        # No WorkList, no EntryPoints.
        m = SearchFacets.selectable_entrypoints
        assert [] == m(None)

        # If there is one EntryPoint, it is returned as-is.
        worklist.entrypoints = [ep1]
        assert [ep1] == m(worklist)

        # If there are multiple EntryPoints, EverythingEntryPoint
        # shows up at the beginning.
        worklist.entrypoints = [ep1, ep2]
        assert [EverythingEntryPoint, ep1, ep2] == m(worklist)

        # If EverythingEntryPoint is already in the list, it's not
        # added twice.
        worklist.entrypoints = [ep1, EverythingEntryPoint, ep2]
        assert worklist.entrypoints == m(worklist)

    def test_items(self):
        facets = SearchFacets(
            entrypoint=EverythingEntryPoint,
            media=Edition.BOOK_MEDIUM,
            languages=["eng"],
            min_score=123,
        )

        # When we call items(), e.g. to create a query string that
        # propagates the facet settings, both entrypoint and
        # media are propagated if present.
        #
        # language is not propagated, because it's set through
        # the Accept-Language header rather than through a query
        # string.
        assert [
            ("entrypoint", EverythingEntryPoint.INTERNAL_NAME),
            (Facets.ORDER_FACET_GROUP_NAME, SearchFacets.ORDER_BY_RELEVANCE),
            (Facets.AVAILABILITY_FACET_GROUP_NAME, Facets.AVAILABLE_ALL),
            ("media", Edition.BOOK_MEDIUM),
            ("min_score", "123"),
            ("search_type", "default"),
        ] == list(facets.items())

        # In case the language came from a query argument
        facets = SearchFacets(
            languages=["eng"],
            language_from_query=True,
        )

        assert dict(facets.items())["language"] == ["eng"]

    def test_navigation(self):
        """Navigating from one SearchFacets to another gives a new
        SearchFacets object. A number of fields can be changed,
        including min_score, which is SearchFacets-specific.
        """
        facets = SearchFacets(entrypoint=object(), order="field1", min_score=100)
        new_ep = object()
        new_facets = facets.navigate(entrypoint=new_ep, order="field2", min_score=120)
        assert isinstance(new_facets, SearchFacets)
        assert new_ep == new_facets.entrypoint
        assert "field2" == new_facets.order
        assert 120 == new_facets.min_score

    def test_modify_search_filter(self):
        # Test superclass behavior -- filter is modified by entrypoint.
        facets = SearchFacets(entrypoint=AudiobooksEntryPoint)
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [Edition.AUDIO_MEDIUM] == filter.media

        # The medium specified in the constructor overrides anything
        # already present in the filter.
        facets = SearchFacets(entrypoint=None, media=Edition.BOOK_MEDIUM)
        filter = Filter(media=Edition.AUDIO_MEDIUM)
        facets.modify_search_filter(filter)
        assert [Edition.BOOK_MEDIUM] == filter.media

        # It also overrides anything specified by the EntryPoint.
        facets = SearchFacets(
            entrypoint=AudiobooksEntryPoint, media=Edition.BOOK_MEDIUM
        )
        filter = Filter()
        facets.modify_search_filter(filter)
        assert [Edition.BOOK_MEDIUM] == filter.media

        # The language specified in the constructor _adds_ to any
        # languages already present in the filter.
        facets = SearchFacets(languages=["eng", "spa"])
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        assert ["eng", "spa"] == filter.languages

        # It doesn't override those values.
        facets = SearchFacets(languages="eng")
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        assert ["eng", "spa"] == filter.languages

        # This may result in modify_search_filter being a no-op.
        facets = SearchFacets(languages="eng")
        filter = Filter(languages="eng")
        facets.modify_search_filter(filter)
        assert ["eng"] == filter.languages

        # If no languages are specified in the SearchFacets, the value
        # set by the filter is used by itself.
        facets = SearchFacets(languages=None)
        filter = Filter(languages="spa")
        facets.modify_search_filter(filter)
        assert ["spa"] == filter.languages

        # If neither facets nor filter includes any languages, there
        # is no language filter.
        facets = SearchFacets(languages=None)
        filter = Filter(languages=None)
        facets.modify_search_filter(filter)
        assert None == filter.languages

        # We don't interfere with the languages filter when languages is ["all"]
        facets = SearchFacets(languages="all")
        filter = Filter(languages=["spa"])
        facets.modify_search_filter(filter)
        assert ["spa"] == filter.languages

    def test_modify_search_filter_accepts_relevance_order(self):
        # By default, Opensearch orders by relevance, so if order
        # is specified as "relevance", filter should not have an
        # `order` property.
        with LogCaptureHandler(logging.root) as logs:
            facets = SearchFacets()
            filter = Filter()
            facets.modify_search_filter(filter)
            assert None == filter.order
            assert 0 == len(logs.error)

        with LogCaptureHandler(logging.root) as logs:
            facets = SearchFacets(order="relevance")
            filter = Filter()
            facets.modify_search_filter(filter)
            assert None == filter.order
            assert 0 == len(logs.error)

        with LogCaptureHandler(logging.root) as logs:
            supported_order = "author"
            facets = SearchFacets(order=supported_order)
            filter = Filter()
            facets.modify_search_filter(filter)
            assert filter.order is not None
            assert len(filter.order) > 0
            assert 0 == len(logs.error)

        with LogCaptureHandler(logging.root) as logs:
            unsupported_order = "some_order_we_do_not_support"
            facets = SearchFacets(order=unsupported_order)
            filter = Filter()
            facets.modify_search_filter(filter)
            assert None == filter.order
            assert "Unrecognized sort order: %s" % unsupported_order in logs.error
