import datetime
import json
import urllib.parse
from typing import Any, Dict
from unittest.mock import MagicMock

import feedparser
import flask
import pytest
from flask import url_for

from api.circulation import FulfillmentInfo, LoanInfo
from api.lanes import (
    ContributorFacets,
    ContributorLane,
    RecommendationLane,
    RelatedBooksLane,
    SeriesFacets,
    SeriesLane,
)
from api.novelist import MockNoveListAPI
from api.problem_details import NO_SUCH_LANE, NOT_FOUND_ON_REMOTE
from core.classifier import Classifier
from core.entrypoint import AudiobooksEntryPoint
from core.external_search import SortKeyPagination, mock_search_index
from core.feed.acquisition import OPDSAcquisitionFeed
from core.feed.annotator.circulation import LibraryAnnotator
from core.feed.types import WorkEntry
from core.lane import Facets, FeaturedFacets
from core.metadata_layer import ContributorData, Metadata
from core.model import (
    DataSource,
    Edition,
    Identifier,
    LicensePool,
    MediaTypes,
    Resource,
    get_one,
    tuple_to_numericrange,
)
from core.model.work import Work
from core.problem_details import INVALID_INPUT
from core.util.datetime_helpers import utc_now
from core.util.flask_util import Response
from core.util.opds_writer import OPDSFeed
from core.util.problem_detail import ProblemDetail
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.mocks.search import fake_hits


class WorkFixture(CirculationControllerFixture):
    lp: LicensePool
    identifier: Identifier
    datasource: DataSource
    edition: Edition

    def __init__(self, db: DatabaseTransactionFixture):
        super().__init__(db)
        [self.lp] = self.english_1.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name  # type: ignore
        self.identifier = self.lp.identifier


@pytest.fixture(scope="function")
def work_fixture(db: DatabaseTransactionFixture):
    return WorkFixture(db)


class TestWorkController:
    def test_contributor(self, work_fixture: WorkFixture):
        m = work_fixture.manager.work_controller.contributor
        work_fixture.collection.data_source = None

        # Find a real Contributor put in the system through the setup
        # process.
        [contribution] = work_fixture.english_1.presentation_edition.contributions
        contributor = contribution.contributor

        # The contributor is created with both .sort_name and
        # .display_name, but we want to test what happens when both
        # pieces of data aren't avaiable, so unset .sort_name.
        contributor.sort_name = None

        # No contributor name -> ProblemDetail
        with work_fixture.request_context_with_library("/"):
            response = m("", None, None)
        assert 404 == response.status_code
        assert NO_SUCH_LANE.uri == response.uri
        assert "No contributor provided" == response.detail

        # Unable to load ContributorData from contributor name ->
        # ProblemDetail
        with work_fixture.request_context_with_library("/"):
            response = m("Unknown Author", None, None)
        assert 404 == response.status_code
        assert NO_SUCH_LANE.uri == response.uri
        assert "Unknown contributor: Unknown Author" == response.detail

        contributor = contributor.display_name

        # Search index misconfiguration -> Problem detail
        work_fixture.assert_bad_search_index_gives_problem_detail(
            lambda: work_fixture.manager.work_controller.series(contributor, None, None)
        )

        # Bad facet data -> ProblemDetail
        with work_fixture.request_context_with_library("/?order=nosuchorder"):
            response = m(contributor, None, None)
            assert 400 == response.status_code
            assert INVALID_INPUT.uri == response.uri

        # Bad pagination data -> ProblemDetail
        with work_fixture.request_context_with_library("/?size=abc"):
            response = m(contributor, None, None)
            assert 400 == response.status_code
            assert INVALID_INPUT.uri == response.uri

        # Test an end-to-end success (not including a test that the
        # search engine can actually find books by a given person --
        # that's tested in core/tests/test_external_search.py).
        with work_fixture.request_context_with_library("/"):
            response = m(contributor, "eng,spa", "Children,Young Adult")
        assert 200 == response.status_code
        assert OPDSFeed.ACQUISITION_FEED_TYPE == response.headers["Content-Type"]
        feed = feedparser.parse(response.data)

        # The feed is named after the person we looked up.
        assert contributor == feed["feed"]["title"]

        # It's got one entry -- the book added to the search engine
        # during test setup.
        [entry] = feed["entries"]
        assert work_fixture.english_1.title == entry["title"]

        # The feed has facet links.
        links = feed["feed"]["links"]
        facet_links = [
            link for link in links if link["rel"] == "http://opds-spec.org/facet"
        ]
        assert 10 == len(facet_links)

        # At this point we don't want to generate real feeds anymore.
        # We can't do a real end-to-end test without setting up a real
        # search index, which is obnoxiously slow.
        #
        # Instead, we will mock AcquisitionFeed.page, and examine the objects
        # passed into it under different mock requests.
        #
        # Those objects, such as ContributorLane and
        # ContributorFacets, are tested elsewhere, in terms of their
        # effects on search objects such as Filter. Those search
        # objects are the things that are tested against a real search
        # index (in core).
        #
        # We know from the previous test that any results returned
        # from the search engine are converted into an OPDS feed. Now
        # we verify that an incoming request results in the objects
        # we'd expect to use to generate the feed for that request.
        class Mock:
            @classmethod
            def page(cls, **kwargs):
                self.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = Response("An OPDS feed")
                return resp

        # Test a basic request with custom faceting, pagination, and a
        # language and audience restriction. This will exercise nearly
        # all the functionality we need to check.
        languages = "some languages"
        audiences = "some audiences"
        sort_key = ["sort", "pagination", "key"]
        with work_fixture.request_context_with_library(
            "/?order=title&size=100&key=%s&entrypoint=Audio" % (json.dumps(sort_key))
        ):
            response = m(contributor, languages, audiences, feed_class=Mock)

        # The Response served by Mock.page becomes the response to the
        # incoming request.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Now check all the keyword arguments that were passed into
        # page().
        kwargs = self.called_with  # type: ignore

        assert work_fixture.db.session == kwargs.pop("_db")
        assert work_fixture.manager._external_search == kwargs.pop("search_engine")

        # The feed is named after the contributor the request asked
        # about.
        assert contributor == kwargs.pop("title")

        # Query string arguments were taken into account when
        # creating the Facets and Pagination objects.
        facets = kwargs.pop("facets")
        assert isinstance(facets, ContributorFacets)
        assert AudiobooksEntryPoint == facets.entrypoint
        assert "title" == facets.order

        pagination = kwargs.pop("pagination")
        assert isinstance(pagination, SortKeyPagination)
        assert sort_key == pagination.last_item_on_previous_page
        assert 100 == pagination.size

        lane = kwargs.pop("worklist")
        assert isinstance(lane, ContributorLane)
        assert isinstance(lane.contributor, ContributorData)

        # We don't know whether the incoming name is a sort name
        # or a display name, so we ask ContributorData.lookup to
        # try it both ways.
        assert contributor == lane.contributor.sort_name
        assert contributor == lane.contributor.display_name
        assert [languages] == lane.languages
        assert [audiences] == lane.audiences

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the ContributorFacets, Pagination and Lane
        # created during the original request.
        library = work_fixture.db.default_library()
        route, url_kwargs = lane.url_arguments
        url_kwargs.update(dict(list(facets.items())))
        url_kwargs.update(dict(list(pagination.items())))
        with work_fixture.request_context_with_library(""):
            expect_url = url_for(
                route,
                lane_identifier=None,
                library_short_name=library.short_name,
                _external=True,
                **url_kwargs,
            )
        assert kwargs.pop("url") == expect_url

        # The Annotator object was instantiated with the proper lane
        # and the newly created Facets object.
        annotator = kwargs.pop("annotator")
        assert lane == annotator.lane
        assert facets == annotator.facets

        # No other arguments were passed into page().
        assert {} == kwargs

    def test_age_appropriateness_end_to_end(self, work_fixture: WorkFixture):
        # An end-to-end test of the idea that a patron can't access
        # feeds configured to include titles that would not be
        # age-appropriate for that patron.
        #
        # A similar test could be run for any of the other subclasses
        # of DynamicLane.
        m = work_fixture.manager.work_controller.contributor

        contributor, ignore = work_fixture.db.contributor()

        patron = work_fixture.default_patron
        patron.external_type = "child"
        children_lane = work_fixture.db.lane()
        children_lane.audiences = [Classifier.AUDIENCE_CHILDREN]
        children_lane.target_age = tuple_to_numericrange((4, 5))
        children_lane.root_for_patron_type = ["child"]

        with work_fixture.request_context_with_library(
            "/", headers=dict(Authorization=work_fixture.valid_auth)
        ):
            # If we ask for books for adults _or_ children by a given
            # author, we're denied access -- the authenticated
            # patron's root lane would make any adult books
            # age-inappropriate.
            audiences = ",".join(
                [Classifier.AUDIENCE_ADULT, Classifier.AUDIENCE_CHILDREN]
            )
            response = m(contributor.sort_name, "eng", audiences)
            assert isinstance(response, ProblemDetail)
            assert NO_SUCH_LANE.uri == response.uri

            # If we only ask for children's books by the same author,
            # we're fine.
            response = m(contributor.sort_name, "eng", Classifier.AUDIENCE_CHILDREN)
            assert 200 == response.status_code

        # We're also fine if we don't authenticate the request at all.
        with work_fixture.request_context_with_library("/"):
            response = m(contributor.sort_name, "eng", audiences)
            assert 200 == response.status_code

    def test_permalink(self, work_fixture: WorkFixture):
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.permalink(
                work_fixture.identifier.type, work_fixture.identifier.identifier
            )
            annotator = LibraryAnnotator(None, None, work_fixture.db.default_library())
            feed = OPDSAcquisitionFeed.single_entry(work_fixture.english_1, annotator)
            assert isinstance(feed, WorkEntry)
            expect = OPDSAcquisitionFeed.entry_as_response(feed)

        assert 200 == response.status_code
        assert expect.data == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers["Content-Type"]

    def test_permalink_does_not_return_fulfillment_links_for_authenticated_patrons_without_loans(
        self, work_fixture: WorkFixture
    ):
        with work_fixture.request_context_with_library("/"):
            # We have two patrons.
            patron_1 = work_fixture.db.patron()
            patron_2 = work_fixture.db.patron()

            # But the request was initiated by the first patron.
            flask.request.patron = patron_1  # type: ignore

            identifier_type = Identifier.GUTENBERG_ID
            identifier = "1234567890"
            edition, _ = work_fixture.db.edition(
                title="Test Book",
                identifier_type=identifier_type,
                identifier_id=identifier,
                with_license_pool=True,
            )
            work = work_fixture.db.work(
                "Test Book", presentation_edition=edition, with_license_pool=True
            )
            pool = work.license_pools[0]

            # Only the second patron has a loan.
            patron2_loan, _ = pool.loan_to(patron_2)

            # We want to make sure that the feed doesn't contain any fulfillment links.
            active_loans_by_work: Dict[Any, Any] = {}
            annotator = LibraryAnnotator(
                None,
                None,
                work_fixture.db.default_library(),
                active_loans_by_work=active_loans_by_work,
            )
            feed = OPDSAcquisitionFeed.single_entry(work, annotator)
            assert isinstance(feed, WorkEntry)
            expect = OPDSAcquisitionFeed.entry_as_response(feed).data

            response = work_fixture.manager.work_controller.permalink(
                identifier_type, identifier
            )

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers["Content-Type"]

    def test_permalink_returns_fulfillment_links_for_authenticated_patrons_with_loans(
        self, work_fixture: WorkFixture
    ):
        with work_fixture.request_context_with_library("/"):
            # We have two patrons.
            patron_1 = work_fixture.db.patron()
            patron_2 = work_fixture.db.patron()

            # But the request was initiated by the first patron.
            flask.request.patron = patron_1  # type: ignore

            identifier_type = Identifier.GUTENBERG_ID
            identifier = "1234567890"
            edition, _ = work_fixture.db.edition(
                title="Test Book",
                identifier_type=identifier_type,
                identifier_id=identifier,
                with_license_pool=True,
            )
            work = work_fixture.db.work(
                "Test Book", presentation_edition=edition, with_license_pool=True
            )
            pool = work.license_pools[0]

            # Both patrons have loans.
            patron1_loan, _ = pool.loan_to(patron_1)
            patron2_loan, _ = pool.loan_to(patron_2)

            # We want to make sure that only the first patron's loan will be in the feed.
            active_loans_by_work = {work: patron1_loan}
            annotator = LibraryAnnotator(
                None,
                None,
                work_fixture.db.default_library(),
                active_loans_by_work=active_loans_by_work,
            )
            feed = OPDSAcquisitionFeed.single_entry(work, annotator)
            assert isinstance(feed, WorkEntry)
            expect = OPDSAcquisitionFeed.entry_as_response(feed).data

            response = work_fixture.manager.work_controller.permalink(
                identifier_type, identifier
            )

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers["Content-Type"]

    def test_permalink_returns_fulfillment_links_for_authenticated_patrons_with_fulfillment(
        self, work_fixture: WorkFixture
    ):
        auth = dict(Authorization=work_fixture.valid_auth)

        with work_fixture.request_context_with_library("/", headers=auth):
            content_link = "https://content"

            # We have two patrons.
            patron_1 = work_fixture.controller.authenticated_patron(
                work_fixture.valid_credentials
            )
            patron_2 = work_fixture.db.patron()

            # But the request was initiated by the first patron.
            flask.request.patron = patron_1  # type: ignore

            identifier_type = Identifier.GUTENBERG_ID
            identifier = "1234567890"
            edition, _ = work_fixture.db.edition(
                title="Test Book",
                identifier_type=identifier_type,
                identifier_id=identifier,
                with_license_pool=True,
            )
            work = work_fixture.db.work(
                "Test Book", presentation_edition=edition, with_license_pool=True
            )
            pool = work.license_pools[0]
            [delivery_mechanism] = pool.delivery_mechanisms

            loan_info = LoanInfo(
                pool.collection,
                pool.data_source.name,
                pool.identifier.type,
                pool.identifier.identifier,
                utc_now(),
                utc_now() + datetime.timedelta(seconds=3600),
            )
            work_fixture.manager.d_circulation.queue_checkout(pool, loan_info)

            fulfillment = FulfillmentInfo(
                pool.collection,
                pool.data_source,
                pool.identifier.type,
                pool.identifier.identifier,
                content_link=content_link,
                content_type=MediaTypes.EPUB_MEDIA_TYPE,
                content=None,
                content_expires=None,
            )
            work_fixture.manager.d_circulation.queue_fulfill(pool, fulfillment)

            # Both patrons have loans:
            # - the first patron's loan and fulfillment will be created via API.
            # - the second patron's loan will be created via loan_to method.
            work_fixture.manager.loans.borrow(
                pool.identifier.type,
                pool.identifier.identifier,
                delivery_mechanism.delivery_mechanism.id,
            )
            work_fixture.manager.loans.fulfill(
                pool.id,
                delivery_mechanism.delivery_mechanism.id,
            )

            patron1_loan = pool.loans[0]
            # We have to create a Resource object manually
            # to assign a URL to the fulfillment that will be used to generate an acquisition link.
            patron1_loan.fulfillment.resource = Resource(url=fulfillment.content_link)

            patron2_loan, _ = pool.loan_to(patron_2)

            # We want to make sure that only the first patron's fulfillment will be in the feed.
            active_loans_by_work = {work: patron1_loan}
            annotator = LibraryAnnotator(
                None,
                None,
                work_fixture.db.default_library(),
                active_loans_by_work=active_loans_by_work,
            )
            feed = OPDSAcquisitionFeed.single_entry(work, annotator)
            assert isinstance(feed, WorkEntry)
            expect = OPDSAcquisitionFeed.entry_as_response(feed).data

            response = work_fixture.manager.work_controller.permalink(
                identifier_type, identifier
            )

        assert 200 == response.status_code
        assert expect == response.get_data()
        assert OPDSFeed.ENTRY_TYPE == response.headers["Content-Type"]

    def test_recommendations(self, work_fixture: WorkFixture):
        # Test the ability to get a feed of works recommended by an
        # external service.
        [self.lp] = work_fixture.english_1.license_pools
        self.edition = self.lp.presentation_edition
        self.datasource = self.lp.data_source.name
        self.identifier = self.lp.identifier

        # Prep an empty recommendation.
        source = DataSource.lookup(work_fixture.db.session, self.datasource)
        metadata = Metadata(source)
        mock_api = MockNoveListAPI(work_fixture.db.session)

        args = [self.identifier.type, self.identifier.identifier]
        kwargs: dict[str, Any] = dict(novelist_api=mock_api)

        # We get a 400 response if the pagination data is bad.
        with work_fixture.request_context_with_library("/?size=abc"):
            response = work_fixture.manager.work_controller.recommendations(
                *args, **kwargs
            )
            assert 400 == response.status_code

        # Or if the facet data is bad.
        with work_fixture.request_context_with_library("/?order=nosuchorder"):
            response = work_fixture.manager.work_controller.recommendations(
                *args, **kwargs
            )
            assert 400 == response.status_code

        # Or if the search index is misconfigured.
        work_fixture.assert_bad_search_index_gives_problem_detail(
            lambda: work_fixture.manager.work_controller.recommendations(
                *args, **kwargs
            )
        )

        # If no NoveList API is configured, the lane does not exist.
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.recommendations(*args)
        assert 404 == response.status_code
        assert "http://librarysimplified.org/terms/problem/unknown-lane" == response.uri
        assert "Recommendations not available" == response.detail

        # If the NoveList API is configured, the search index is asked
        # about its recommendations.
        #
        # This test no longer makes sense, the external_search no longer blindly returns information
        # The query_works is not overidden, so we mock it manually
        work_fixture.manager.external_search.query_works = MagicMock(
            return_value=fake_hits([work_fixture.english_1])
        )
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.recommendations(
                *args, **kwargs
            )

        # A feed is returned with the data from the
        # ExternalSearchIndex.
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)
        assert "Titles recommended by NoveList" == feed["feed"]["title"]
        [entry] = feed.entries
        assert work_fixture.english_1.title == entry["title"]
        author = self.edition.author_contributors[0]
        expected_author_name = author.display_name or author.sort_name
        assert expected_author_name == entry.author

        # Now let's pass in a mocked AcquisitionFeed so we can check
        # the arguments used to invoke page().
        class Mock:
            @classmethod
            def page(cls, **kwargs):
                cls.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = Response("A bunch of titles")
                return resp

        kwargs["feed_class"] = Mock
        with work_fixture.request_context_with_library(
            "/?order=title&size=2&after=30&entrypoint=Audio"
        ):
            response = work_fixture.manager.work_controller.recommendations(
                *args, **kwargs
            )

        # The return value of Mock.page was used as the response
        # to the incoming request.
        assert 200 == response.status_code
        assert "A bunch of titles" == response.get_data(as_text=True)

        kwargs = Mock.called_with  # type: ignore
        assert work_fixture.db.session == kwargs.pop("_db")
        assert "Titles recommended by NoveList" == kwargs.pop("title")

        # The RecommendationLane is set up to ask for recommendations
        # for this book.
        lane = kwargs.pop("worklist")
        assert isinstance(lane, RecommendationLane)
        library = work_fixture.db.default_library()
        assert library.id == lane.library_id
        assert work_fixture.english_1 == lane.work
        assert "Recommendations for Quite British by John Bull" == lane.display_name
        assert mock_api == lane.novelist_api

        facets = kwargs.pop("facets")
        assert isinstance(facets, Facets)
        assert Facets.ORDER_TITLE == facets.order
        assert AudiobooksEntryPoint == facets.entrypoint

        pagination = kwargs.pop("pagination")
        assert 30 == pagination.offset
        assert 2 == pagination.size

        annotator = kwargs.pop("annotator")
        assert lane == annotator.lane

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the Facets, Pagination and Lane created
        # during the original request.
        route, url_kwargs = lane.url_arguments
        url_kwargs.update(dict(list(facets.items())))
        url_kwargs.update(dict(list(pagination.items())))
        with work_fixture.request_context_with_library(""):
            expect_url = url_for(
                route,
                library_short_name=library.short_name,
                _external=True,
                **url_kwargs,
            )
        assert kwargs.pop("url") == expect_url

    def test_related_books(self, work_fixture: WorkFixture):
        # Test the related_books controller.

        # Remove the contributor from the work created during setup.
        work = work_fixture.english_1
        edition = work.presentation_edition
        identifier = edition.primary_identifier
        [contribution] = edition.contributions
        contributor = contribution.contributor
        role = contribution.role
        work_fixture.db.session.delete(contribution)
        work_fixture.db.session.commit()
        assert None == edition.series

        # First, let's test a complex error case. We're asking about a
        # work with no contributors or series, and no NoveList
        # integration is configured. The 'related books' lane ends up
        # with no sublanes, so the controller acts as if the lane
        # itself does not exist.
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.related(
                identifier.type,
                identifier.identifier,
            )
            assert 404 == response.status_code
            assert (
                "http://librarysimplified.org/terms/problem/unknown-lane"
                == response.uri
            )

        # Now test some error cases where the lane exists but
        # something else goes wrong.

        # Give the work a series and a contributor, so that it will
        # get sublanes for both types of recommendations.
        edition.series = "Around the World"
        edition.add_contributor(contributor, role)

        # A grouped feed is not paginated, so we don't check pagination
        # information and there's no chance of a problem detail.

        # Theoretically, if bad faceting information is provided we'll
        # get a problem detail. But the faceting class created is
        # FeaturedFacets, which can't raise an exception during the
        # creation process -- an invalid entrypoint will simply be
        # ignored.

        # Bad search index setup -> Problem detail
        work_fixture.assert_bad_search_index_gives_problem_detail(
            lambda: work_fixture.manager.work_controller.related(
                identifier.type, identifier.identifier
            )
        )

        # The mock search engine will return this Work for every
        # search. That means this book will show up as a 'same author'
        # recommendation, a 'same series' recommentation, and a
        # 'external service' recommendation.
        same_author_and_series = work_fixture.db.work(
            title="Same author and series", with_license_pool=True
        )
        work_fixture.manager.external_search.mock_query_works([same_author_and_series])

        mock_api = MockNoveListAPI(work_fixture.db.session)

        # Create a fresh book, and set up a mock NoveList API to
        # recommend its identifier for any input.
        #
        # The mock API needs to return a list of Identifiers, so that
        # the RelatedWorksLane will ask the RecommendationLane to find
        # us a matching work instead of hiding it. But the search
        # index is also mocked, so within this test will return the
        # same book it always does -- same_author_and_series.
        overdrive = DataSource.lookup(work_fixture.db.session, DataSource.OVERDRIVE)
        metadata = Metadata(overdrive)
        recommended_identifier = work_fixture.db.identifier()
        metadata.recommendations = [recommended_identifier]
        mock_api.setup_method(metadata)

        # Now, ask for works related to work_fixture.english_1.
        with mock_search_index(work_fixture.manager.external_search):
            with work_fixture.request_context_with_library("/?entrypoint=Book"):
                response = work_fixture.manager.work_controller.related(
                    work_fixture.identifier.type,
                    work_fixture.identifier.identifier,
                    novelist_api=mock_api,
                )
        assert 200 == response.status_code
        assert OPDSFeed.ACQUISITION_FEED_TYPE == response.headers["content-type"]
        feed = feedparser.parse(response.data)
        assert "Related Books" == feed["feed"]["title"]

        # The feed contains three entries: one for each sublane.
        assert 3 == len(feed["entries"])

        # Group the entries by the sublane they're in.
        def collection_link(entry):
            [link] = [l for l in entry["links"] if l["rel"] == "collection"]
            return link["title"], link["href"]

        by_collection_link = {}
        for entry in feed["entries"]:
            title, href = collection_link(entry)
            by_collection_link[title] = (href, entry)

        # Here's the sublane for books in the same series.
        [same_series_href, same_series_entry] = by_collection_link["Around the World"]
        assert "Same author and series" == same_series_entry["title"]
        expected_series_link = "series/%s/eng/Adult" % urllib.parse.quote(
            "Around the World"
        )
        assert same_series_href.endswith(expected_series_link)

        # Here's the sublane for books by this contributor.
        [same_contributor_href, same_contributor_entry] = by_collection_link[
            "John Bull"
        ]
        assert "Same author and series" == same_contributor_entry["title"]
        expected_contributor_link = urllib.parse.quote("contributor/John Bull/eng/")
        assert same_contributor_href.endswith(expected_contributor_link)

        # Here's the sublane for recommendations from NoveList.
        [recommended_href, recommended_entry] = by_collection_link[
            "Similar titles recommended by NoveList"
        ]
        assert "Same author and series" == recommended_entry["title"]
        work_url = f"/works/{identifier.type}/{identifier.identifier}/"
        expected = urllib.parse.quote(work_url + "recommendations")
        assert True == recommended_href.endswith(expected)

        # Finally, let's pass in a mock feed class so we can look at the
        # objects passed into AcquisitionFeed.groups().
        class Mock:
            @classmethod
            def groups(cls, **kwargs):
                cls.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = Response("An OPDS feed")
                return resp

        mock_api.setup_method(metadata)
        with work_fixture.request_context_with_library("/?entrypoint=Audio"):
            response = work_fixture.manager.work_controller.related(
                work_fixture.identifier.type,
                work_fixture.identifier.identifier,
                novelist_api=mock_api,
                feed_class=Mock,
            )

        # The return value of Mock.groups was used as the response
        # to the incoming request.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        # Verify that groups() was called with the arguments we expect.
        kwargs = Mock.called_with  # type: ignore
        assert work_fixture.db.session == kwargs.pop("_db")
        assert work_fixture.manager.external_search == kwargs.pop("search_engine")
        assert "Related Books" == kwargs.pop("title")

        # We're passing in a FeaturedFacets. Each lane will have a chance
        # to adapt it to a faceting object appropriate for that lane.
        facets = kwargs.pop("facets")
        assert isinstance(facets, FeaturedFacets)
        assert AudiobooksEntryPoint == facets.entrypoint

        # We're generating a grouped feed using a RelatedBooksLane
        # that has three sublanes.
        lane = kwargs.pop("worklist")
        assert isinstance(lane, RelatedBooksLane)
        contributor_lane, novelist_lane, series_lane = lane.children

        assert isinstance(contributor_lane, ContributorLane)
        assert contributor == contributor_lane.contributor

        assert isinstance(novelist_lane, RecommendationLane)
        assert [recommended_identifier] == novelist_lane.recommendations

        assert isinstance(series_lane, SeriesLane)
        assert "Around the World" == series_lane.series

        # The Annotator is associated with the parent RelatedBooksLane.
        annotator = kwargs.pop("annotator")
        assert isinstance(annotator, LibraryAnnotator)
        assert work_fixture.db.default_library() == annotator.library
        assert lane == annotator.lane

        # Checking the URL is difficult because it requires a request
        # context, _plus_ the DatabaseBackedFacets and Lane
        # created during the original request.
        library = work_fixture.db.default_library()
        route, url_kwargs = lane.url_arguments
        url_kwargs.update(dict(list(facets.items())))
        with work_fixture.request_context_with_library(""):
            expect_url = url_for(
                route,
                lane_identifier=None,
                library_short_name=library.short_name,
                _external=True,
                **url_kwargs,
            )
        assert kwargs.pop("url") == expect_url
        assert kwargs.pop("pagination") == None
        # That's it!
        assert {} == kwargs

    def test_related_no_works(self, work_fixture: WorkFixture):
        """Test when an identifier's pool does not have a work attached to it"""
        db = work_fixture.db

        # Create a work and remove it from the licensepool
        work: Work = db.work(with_license_pool=True)
        identifier = work.presentation_edition.primary_identifier
        pool = get_one(db.session, LicensePool, work_id=work.id)
        assert isinstance(pool, LicensePool)
        pool.work_id = None
        db.session.commit()

        with work_fixture.request_context_with_library("/"):
            result = work_fixture.manager.work_controller.related(
                identifier.type, identifier.identifier
            )
        assert result == NOT_FOUND_ON_REMOTE

    def test_series(self, work_fixture: WorkFixture):
        work_fixture.collection.data_source = None
        # Test the ability of the series() method to generate an OPDS
        # feed representing all the books in a given series, subject
        # to an optional language and audience restriction.
        series_name = "Like As If Whatever Mysteries"

        # If no series is given, a ProblemDetail is returned.
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.series("", None, None)
        assert 404 == response.status_code
        assert "http://librarysimplified.org/terms/problem/unknown-lane" == response.uri

        # Similarly if the pagination data is bad.
        with work_fixture.request_context_with_library("/?size=abc"):
            response = work_fixture.manager.work_controller.series(
                series_name, None, None
            )
            assert 400 == response.status_code

        # Or if the facet data is bad
        with work_fixture.request_context_with_library("/?order=nosuchorder"):
            response = work_fixture.manager.work_controller.series(
                series_name, None, None
            )
            assert 400 == response.status_code

        # Or if the search index isn't set up.
        work_fixture.assert_bad_search_index_gives_problem_detail(
            lambda: work_fixture.manager.work_controller.series(series_name, None, None)
        )

        # Set up the mock search engine to return our work no matter
        # what query it's given. The fact that this book isn't
        # actually in the series doesn't matter, since determining
        # that is the job of a non-mocked search engine.
        work = work_fixture.db.work(with_open_access_download=True)
        search_engine = work_fixture.manager.external_search
        search_engine.mock_query_works([work])

        # If a series is provided, a feed for that series is returned.
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.series(
                series_name,
                "eng,spa",
                "Children,Young Adult",
            )
        assert 200 == response.status_code
        feed = feedparser.parse(response.data)

        # The book we added to the mock search engine is in the feed.
        # This demonstrates that series() asks the search engine for
        # books to put in the feed.
        assert series_name == feed["feed"]["title"]
        [entry] = feed["entries"]
        assert work.title == entry["title"]

        # The feed has facet links.
        links = feed["feed"]["links"]
        facet_links = [
            link for link in links if link["rel"] == "http://opds-spec.org/facet"
        ]
        assert 11 == len(facet_links)

        # The facet link we care most about is the default sort order,
        # put into place by SeriesFacets.
        [series_position] = [x for x in facet_links if x["title"] == "Series Position"]
        assert "Sort by" == series_position["opds:facetgroup"]
        assert "true" == series_position["opds:activefacet"]

        # At this point we don't want to generate real feeds anymore.
        # We can't do a real end-to-end test without setting up a real
        # search index, which is obnoxiously slow.
        #
        # Instead, we will mock AcquisitionFeed.page, and examine the
        # objects passed into it under different mock requests.
        #
        # Those objects, such as SeriesLane and SeriesFacets, are
        # tested elsewhere, in terms of their effects on search
        # objects such as Filter. Those search objects are the things
        # that are tested against a real search index (in core).
        #
        # We know from the previous test that any results returned
        # from the search engine are converted into an OPDS feed. Now
        # we verify that an incoming request results in the objects
        # we'd expect to use to generate the feed for that request.
        class Mock:
            @classmethod
            def page(cls, **kwargs):
                self.called_with = kwargs
                resp = MagicMock()
                resp.as_response.return_value = Response("An OPDS feed")
                return resp

        # Test a basic request with custom faceting, pagination, and a
        # language and audience restriction. This will exercise nearly
        # all the functionality we need to check.
        sort_key = ["sort", "pagination", "key"]
        with work_fixture.request_context_with_library(
            "/?order=title&size=100&key=%s" % json.dumps(sort_key)
        ):
            response = work_fixture.manager.work_controller.series(
                series_name, "some languages", "some audiences", feed_class=Mock
            )

        # The return value of Mock.page() is the response to the
        # incoming request.
        assert 200 == response.status_code
        assert "An OPDS feed" == response.get_data(as_text=True)

        kwargs = self.called_with  # type: ignore
        assert work_fixture.db.session == kwargs.pop("_db")

        # The feed is titled after the series.
        assert series_name == kwargs.pop("title")

        # A SeriesLane was created to ask the search index for
        # matching works.
        lane = kwargs.pop("worklist")
        assert isinstance(lane, SeriesLane)
        assert work_fixture.db.default_library().id == lane.library_id
        assert series_name == lane.series
        assert ["some languages"] == lane.languages
        assert ["some audiences"] == lane.audiences

        # A SeriesFacets was created to add an extra sort order and
        # to provide additional search index constraints that can only
        # be provided through the faceting object.
        facets = kwargs.pop("facets")
        assert isinstance(facets, SeriesFacets)

        # The 'order' in the query string went into the SeriesFacets
        # object.
        assert "title" == facets.order

        # The 'key' and 'size' went into a SortKeyPagination object.
        pagination = kwargs.pop("pagination")
        assert isinstance(pagination, SortKeyPagination)
        assert sort_key == pagination.last_item_on_previous_page
        assert 100 == pagination.size

        # The lane, facets, and pagination were all taken into effect
        # when constructing the feed URL.
        annotator = kwargs.pop("annotator")
        assert lane == annotator.lane
        with work_fixture.request_context_with_library("/"):
            assert annotator.feed_url(
                lane, facets=facets, pagination=pagination
            ) == kwargs.pop("url")

        # The (mocked) search engine associated with the CirculationManager was
        # passed in.
        assert work_fixture.manager.external_search == kwargs.pop("search_engine")

        # No other arguments were passed into Mock.page.
        assert {} == kwargs

        # In the previous request we provided a custom sort order (by
        # title) Let's end with one more test to verify that series
        # position is the *default* sort order.
        with work_fixture.request_context_with_library("/"):
            response = work_fixture.manager.work_controller.series(
                series_name, None, None, feed_class=Mock
            )
        facets = self.called_with.pop("facets")  # type: ignore
        assert isinstance(facets, SeriesFacets)
        assert "series" == facets.order
