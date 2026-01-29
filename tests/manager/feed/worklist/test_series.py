import pytest

from palace.manager.core.classifier import Classifier
from palace.manager.core.entrypoint import AudiobooksEntryPoint
from palace.manager.feed.facets.feed import Facets, FeaturedFacets
from palace.manager.feed.facets.series import SeriesFacets
from palace.manager.feed.worklist.dynamic import WorkBasedLane
from palace.manager.feed.worklist.series import SeriesLane
from palace.manager.search.filter import Filter
from tests.fixtures.database import DatabaseTransactionFixture


class TestSeriesLane:
    def test_initialization(self, db: DatabaseTransactionFixture):
        # An error is raised if SeriesLane is created with an empty string.
        with pytest.raises(ValueError):
            SeriesLane(db.default_library(), "")
        with pytest.raises(ValueError):
            SeriesLane(db.default_library(), None)

        work = db.work(language="spa", audience=[Classifier.AUDIENCE_CHILDREN])
        work_based_lane = WorkBasedLane(db.default_library(), work)
        child = SeriesLane(
            db.default_library(),
            "Alrighty Then",
            parent=work_based_lane,
            languages=["eng"],
            audiences=["another audience"],
        )

        # The series provided in the constructor is stored as .series.
        assert "Alrighty Then" == child.series

        # The SeriesLane is added as a child of its parent
        # WorkBasedLane -- something that doesn't happen by default.
        assert [child] == work_based_lane.children

        # As a side effect of that, this lane's audiences and
        # languages were changed to values consistent with its parent.
        assert [work_based_lane.source_audience] == child.audiences
        assert work_based_lane.languages == child.languages

        # If for some reason there's no audience for the work used as
        # a basis for the parent lane, the parent lane's audience
        # filter is used as a basis for the child lane's audience filter.
        work_based_lane.source_audience = None
        child = SeriesLane(db.default_library(), "No Audience", parent=work_based_lane)
        assert work_based_lane.audiences == child.audiences

    def test_modify_search_filter_hook(self, db: DatabaseTransactionFixture):
        lane = SeriesLane(db.default_library(), "So That Happened")
        filter = Filter()
        lane.modify_search_filter_hook(filter)
        assert "So That Happened" == filter.series

    def test_overview_facets(self, db: DatabaseTransactionFixture):
        # A FeaturedFacets object is adapted to a SeriesFacets object.
        # This guarantees that a SeriesLane's contributions to a
        # grouped feed will be ordered correctly.
        featured = FeaturedFacets(0.44, entrypoint=AudiobooksEntryPoint)
        lane = SeriesLane(db.default_library(), "Alrighty Then")
        overview = lane.overview_facets(db.session, featured)
        assert isinstance(overview, SeriesFacets)
        assert Facets.AVAILABLE_ALL == overview.availability
        assert Facets.ORDER_SERIES_POSITION == overview.order

        # Entry point was preserved.
        assert AudiobooksEntryPoint == overview.entrypoint
