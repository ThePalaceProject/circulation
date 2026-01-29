from unittest.mock import MagicMock, create_autospec

import pytest

from palace.manager.core.classifier import Classifier
from palace.manager.core.entrypoint import AudiobooksEntryPoint
from palace.manager.data_layer.identifier import IdentifierData
from palace.manager.feed.facets.feed import Facets, FeaturedFacets
from palace.manager.feed.worklist.contributor import ContributorLane
from palace.manager.feed.worklist.recommendation import (
    RecommendationLane,
    RelatedBooksLane,
)
from palace.manager.feed.worklist.series import SeriesLane
from palace.manager.integration.metadata.novelist import NoveListAPI
from palace.manager.search.filter import Filter
from palace.manager.sqlalchemy.model.contributor import Contributor
from palace.manager.sqlalchemy.model.identifier import Identifier
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def mock_novelist_api() -> MagicMock:
    """Create a mock NoveListAPI with empty recommendations."""
    mock_api = create_autospec(NoveListAPI)
    mock_api.lookup_recommendations.return_value = []
    return mock_api


class TestRecommendationLane:
    def test_modify_search_filter_hook(
        self, db: DatabaseTransactionFixture, mock_novelist_api: MagicMock
    ):
        work = db.work(with_license_pool=True)

        # With an empty recommendation result, the Filter is set up
        # to return nothing.
        lane = RecommendationLane(
            db.default_library(),
            work,
            "",
            novelist_api=mock_novelist_api,
        )
        filter = Filter()
        assert False == filter.match_nothing
        modified = lane.modify_search_filter_hook(filter)
        assert modified == filter
        assert True == filter.match_nothing

        # When there are recommendations, the Filter is modified to
        # match only those ISBNs.
        i1 = db.identifier()
        i2 = db.identifier()
        lane.recommendations = [i1, i2]
        filter = Filter()
        assert [] == filter.identifiers
        modified = lane.modify_search_filter_hook(filter)
        assert modified == filter
        assert [i1, i2] == filter.identifiers
        assert False == filter.match_nothing

    def test_overview_facets(
        self, db: DatabaseTransactionFixture, mock_novelist_api: MagicMock
    ):
        work = db.work(with_license_pool=True)

        # A FeaturedFacets object is adapted to a Facets object with
        # specific settings.
        featured = FeaturedFacets(0.44, entrypoint=AudiobooksEntryPoint)
        lane = RecommendationLane(
            db.default_library(),
            work,
            "",
            novelist_api=mock_novelist_api,
        )
        overview = lane.overview_facets(db.session, featured)
        assert isinstance(overview, Facets)
        assert Facets.AVAILABLE_ALL == overview.availability
        assert Facets.ORDER_AUTHOR == overview.order

        # Entry point was preserved.
        assert AudiobooksEntryPoint == overview.entrypoint

    def test_fetch_recommendations(
        self, db: DatabaseTransactionFixture, mock_novelist_api: MagicMock
    ):
        work = db.work(with_license_pool=True)

        known_identifier = db.identifier()
        known_identifier_data = IdentifierData.from_identifier(known_identifier)
        unknown_identifier_data = IdentifierData(
            type=Identifier.ISBN, identifier="hey there"
        )

        lane = RecommendationLane(
            db.default_library(),
            work,
            "",
            novelist_api=mock_novelist_api,
        )

        # Unknown identifiers are filtered out of the recommendations
        mock_novelist_api.lookup_recommendations.return_value = [
            known_identifier_data,
            unknown_identifier_data,
        ]
        result = lane.fetch_recommendations(db.session)
        assert result == [known_identifier]

        # The results are Identifiers, not IdentifierData.
        [result_identifier] = result
        assert isinstance(result_identifier, Identifier)


class RelatedBooksFixture:
    def __init__(self, db: DatabaseTransactionFixture):
        self.db = db
        self.work = db.work(
            with_license_pool=True, audience=Classifier.AUDIENCE_YOUNG_ADULT
        )
        [self.lp] = self.work.license_pools
        self.edition = self.work.presentation_edition


@pytest.fixture(scope="function")
def related_books_fixture(db: DatabaseTransactionFixture) -> RelatedBooksFixture:
    return RelatedBooksFixture(db)


class TestRelatedBooksLane:
    def test_initialization(self, related_books_fixture: RelatedBooksFixture):
        # Asserts that a RelatedBooksLane won't be initialized for a work
        # without related books

        # A book without a series or a contributor on a circ manager without
        # NoveList recommendations raises an error.
        db = related_books_fixture.db
        db.session.delete(related_books_fixture.edition.contributions[0])
        db.session.commit()

        pytest.raises(
            ValueError,
            RelatedBooksLane,
            db.default_library(),
            related_books_fixture.work,
            "",
        )

        # A book with a contributor initializes a RelatedBooksLane.
        luthor, i = db.contributor("Luthor, Lex")
        related_books_fixture.edition.add_contributor(luthor, [Contributor.Role.EDITOR])

        result = RelatedBooksLane(db.default_library(), related_books_fixture.work, "")
        assert related_books_fixture.work == result.work
        [sublane] = result.children
        assert True == isinstance(sublane, ContributorLane)
        assert sublane.contributor == luthor

        # As does a book in a series.
        related_books_fixture.edition.series = "All By Myself"
        result = RelatedBooksLane(db.default_library(), related_books_fixture.work, "")
        assert 2 == len(result.children)
        [contributor, series] = result.children
        assert True == isinstance(series, SeriesLane)

        # When NoveList is configured and recommendations are available,
        # a RecommendationLane will be included.
        mock_api = create_autospec(NoveListAPI)
        mock_api.lookup_recommendations.return_value = [
            IdentifierData.from_identifier(db.identifier())
        ]
        result = RelatedBooksLane(
            db.default_library(), related_books_fixture.work, "", novelist_api=mock_api
        )
        assert 3 == len(result.children)

        [novelist_recommendations] = [
            x for x in result.children if isinstance(x, RecommendationLane)
        ]
        assert (
            "Similar titles recommended by NoveList"
            == novelist_recommendations.display_name
        )

        # The book's language and audience list is passed down to all sublanes.
        assert ["eng"] == result.languages
        for sublane in result.children:
            assert result.languages == sublane.languages
            if isinstance(sublane, SeriesLane):
                assert [result.source_audience] == sublane.audiences
            else:
                assert sorted(list(result.audiences)) == sorted(list(sublane.audiences))

        contributor, recommendations, series = result.children
        assert True == isinstance(recommendations, RecommendationLane)
        assert True == isinstance(series, SeriesLane)
        assert True == isinstance(contributor, ContributorLane)

    def test_contributor_lane_generation(
        self, related_books_fixture: RelatedBooksFixture
    ):
        db = related_books_fixture.db

        original = related_books_fixture.edition.contributions[0].contributor
        luthor, i = db.contributor("Luthor, Lex")
        related_books_fixture.edition.add_contributor(luthor, Contributor.Role.EDITOR)

        # Lex Luthor doesn't show up because he's only an editor,
        # and an author is listed.
        result = RelatedBooksLane(db.default_library(), related_books_fixture.work, "")
        assert 1 == len(result.children)
        [sublane] = result.children
        assert original == sublane.contributor

        # A book with multiple contributors results in multiple
        # ContributorLane sublanes.
        lane, i = db.contributor("Lane, Lois")
        related_books_fixture.edition.add_contributor(
            lane, Contributor.Role.PRIMARY_AUTHOR
        )
        result = RelatedBooksLane(db.default_library(), related_books_fixture.work, "")
        assert 2 == len(result.children)
        sublane_contributors = list()

        for c in result.children:
            sublane_contributors.append(c.contributor)
        assert {lane, original} == set(sublane_contributors)

        # When there are no AUTHOR_ROLES present, contributors in
        # displayable secondary roles appear.
        for contribution in related_books_fixture.edition.contributions:
            if contribution.role in Contributor.AUTHOR_ROLES:
                db.session.delete(contribution)
        db.session.commit()

        result = RelatedBooksLane(db.default_library(), related_books_fixture.work, "")
        assert 1 == len(result.children)
        [sublane] = result.children
        assert luthor == sublane.contributor

    def test_works_query(self, related_books_fixture: RelatedBooksFixture):
        """RelatedBooksLane is an invisible, groups lane without works."""

        db = related_books_fixture.db
        related_books_fixture.edition.series = "All By Myself"
        lane = RelatedBooksLane(db.default_library(), related_books_fixture.work, "")
        assert [] == lane.works(db.session)
