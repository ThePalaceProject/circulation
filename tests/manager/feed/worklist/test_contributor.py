import pytest

from palace.manager.core.entrypoint import AudiobooksEntryPoint
from palace.manager.data_layer.contributor import ContributorData
from palace.manager.feed.facets.contributor import ContributorFacets
from palace.manager.feed.facets.feed import Facets, FeaturedFacets
from palace.manager.feed.worklist.base import WorkList
from palace.manager.feed.worklist.contributor import ContributorLane
from palace.manager.search.filter import Filter
from palace.manager.sqlalchemy.model.contributor import Contributor
from tests.fixtures.database import DatabaseTransactionFixture


@pytest.fixture
def contributor_fixture(db: DatabaseTransactionFixture) -> Contributor:
    """Create a contributor for testing ContributorLane."""
    contributor, _ = db.contributor("Lane, Lois", viaf="7", display_name="Lois Lane")
    return contributor


class TestContributorLane:
    def test_initialization(
        self, db: DatabaseTransactionFixture, contributor_fixture: Contributor
    ):
        with pytest.raises(ValueError) as excinfo:
            ContributorLane(db.default_library(), None)
        assert "ContributorLane can't be created without contributor" in str(
            excinfo.value
        )

        parent = WorkList()
        parent.initialize(db.default_library())

        lane = ContributorLane(
            db.default_library(),
            contributor_fixture,
            parent,
            languages=["a"],
            audiences=["b"],
        )
        assert contributor_fixture == lane.contributor
        assert ["a"] == lane.languages
        assert ["b"] == lane.audiences
        assert [lane] == parent.children

        # The contributor_key will be used in links to other pages
        # of this Lane and so on.
        assert "Lois Lane" == lane.contributor_key

        # If the contributor used to create a ContributorLane has no
        # display name, their sort name is used as the
        # contributor_key.
        contributor = ContributorData(sort_name="Lane, Lois")
        lane = ContributorLane(db.default_library(), contributor)
        assert contributor == lane.contributor
        assert "Lane, Lois" == lane.contributor_key

    def test_url_arguments(
        self, db: DatabaseTransactionFixture, contributor_fixture: Contributor
    ):
        lane = ContributorLane(
            db.default_library(),
            contributor_fixture,
            languages=["eng", "spa"],
            audiences=["Adult", "Children"],
        )
        route, kwargs = lane.url_arguments
        assert lane.ROUTE == route

        assert (
            dict(
                contributor_name=lane.contributor_key,
                languages="eng,spa",
                audiences="Adult,Children",
            )
            == kwargs
        )

    def test_modify_search_filter_hook(
        self, db: DatabaseTransactionFixture, contributor_fixture: Contributor
    ):
        lane = ContributorLane(db.default_library(), contributor_fixture)
        filter = Filter()
        lane.modify_search_filter_hook(filter)
        assert contributor_fixture == filter.author

    def test_overview_facets(
        self, db: DatabaseTransactionFixture, contributor_fixture: Contributor
    ):
        # A FeaturedFacets object is adapted to a ContributorFacets object.
        # This guarantees that a ContributorLane's contributions to a
        # grouped feed will be ordered correctly.
        featured = FeaturedFacets(0.44, entrypoint=AudiobooksEntryPoint)
        lane = ContributorLane(db.default_library(), contributor_fixture)
        overview = lane.overview_facets(db.session, featured)
        assert isinstance(overview, ContributorFacets)
        assert Facets.AVAILABLE_ALL == overview.availability
        assert Facets.ORDER_TITLE == overview.order

        # Entry point was preserved.
        assert AudiobooksEntryPoint == overview.entrypoint
