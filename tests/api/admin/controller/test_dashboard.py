import csv
import datetime
from datetime import timedelta
from unittest import mock

import pytest

from core.feed.annotator.admin import AdminAnnotator
from core.model import CirculationEvent, Genre, WorkGenre, get_one_or_create
from core.util.datetime_helpers import utc_now
from tests.fixtures.api_admin import AdminControllerFixture
from tests.fixtures.api_controller import ControllerFixture


class DashboardFixture(AdminControllerFixture):
    def __init__(self, controller_fixture: ControllerFixture):
        super().__init__(controller_fixture)

        self.english_1 = self.ctrl.db.work(
            "Quite British",
            "John Bull",
            language="eng",
            fiction=True,
            with_open_access_download=True,
        )
        self.english_1.license_pools[0].collection = self.ctrl.collection
        self.works = [self.english_1]

        self.manager.external_search.mock_query_works(self.works)


@pytest.fixture(scope="function")
def dashboard_fixture(controller_fixture: ControllerFixture) -> DashboardFixture:
    return DashboardFixture(controller_fixture)


class TestDashboardController:
    def test_circulation_events(self, dashboard_fixture: DashboardFixture):
        [lp] = dashboard_fixture.english_1.license_pools
        types = [
            CirculationEvent.DISTRIBUTOR_CHECKIN,
            CirculationEvent.DISTRIBUTOR_CHECKOUT,
            CirculationEvent.DISTRIBUTOR_HOLD_PLACE,
            CirculationEvent.DISTRIBUTOR_HOLD_RELEASE,
            CirculationEvent.DISTRIBUTOR_TITLE_ADD,
        ]
        time = utc_now() - timedelta(minutes=len(types))
        for type in types:
            get_one_or_create(
                dashboard_fixture.ctrl.db.session,
                CirculationEvent,
                license_pool=lp,
                type=type,
                start=time,
                end=time,
            )
            time += timedelta(minutes=1)

        with dashboard_fixture.request_context_with_library_and_admin("/"):
            response = (
                dashboard_fixture.manager.admin_dashboard_controller.circulation_events()
            )
            url = AdminAnnotator(
                dashboard_fixture.manager.d_circulation,  # type: ignore
                dashboard_fixture.ctrl.db.default_library(),
            ).permalink_for(lp.identifier)

        events = response["circulation_events"]
        assert types[::-1] == [event["type"] for event in events]
        assert [dashboard_fixture.english_1.title] * len(types) == [
            event["book"]["title"] for event in events
        ]
        assert [url] * len(types) == [event["book"]["url"] for event in events]

        # request fewer events
        with dashboard_fixture.request_context_with_library_and_admin("/?num=2"):
            response = (
                dashboard_fixture.manager.admin_dashboard_controller.circulation_events()
            )
            url = AdminAnnotator(
                dashboard_fixture.manager.d_circulation,  # type: ignore
                dashboard_fixture.ctrl.db.default_library(),
            ).permalink_for(lp.identifier)

        assert 2 == len(response["circulation_events"])

    def test_bulk_circulation_events(self, dashboard_fixture: DashboardFixture):
        [lp] = dashboard_fixture.english_1.license_pools
        edition = dashboard_fixture.english_1.presentation_edition
        identifier = dashboard_fixture.english_1.presentation_edition.primary_identifier
        genres = dashboard_fixture.ctrl.db.session.query(Genre).all()
        get_one_or_create(
            dashboard_fixture.ctrl.db.session,
            WorkGenre,
            work=dashboard_fixture.english_1,
            genre=genres[0],
            affinity=0.2,
        )

        # We use local time here, rather than UTC time, because we use
        # local time when finding the correct date in bulk_circulation_events
        # because it is a user supplied date. See the get_date method.
        time = datetime.datetime.now() - timedelta(minutes=1)
        event, ignore = get_one_or_create(
            dashboard_fixture.ctrl.db.session,
            CirculationEvent,
            license_pool=lp,
            type=CirculationEvent.DISTRIBUTOR_CHECKOUT,
            start=time,
            end=time,
        )
        time += timedelta(minutes=1)

        # Try an end-to-end test, getting all circulation events for
        # the current day.
        with dashboard_fixture.ctrl.app.test_request_context("/"):
            (
                response,
                requested_date,
                date_end,
                library_short_name,
            ) = (
                dashboard_fixture.manager.admin_dashboard_controller.bulk_circulation_events()
            )
        reader = csv.reader(
            [row for row in response.split("\r\n") if row], dialect=csv.excel
        )
        rows = [row for row in reader][1::]  # skip header row
        assert 1 == len(rows)
        [row] = rows
        assert CirculationEvent.DISTRIBUTOR_CHECKOUT == row[1]
        assert identifier.identifier == row[2]
        assert identifier.type == row[3]
        assert edition.title == row[4]
        assert genres[0].name == row[12]

        # Now verify that this works by passing incoming query
        # parameters into a LocalAnalyticsExporter object.
        class MockLocalAnalyticsExporter:
            def export(self, _db, date_start, date_end, locations, library):
                self.called_with = (_db, date_start, date_end, locations, library)
                return "A CSV file"

        exporter = MockLocalAnalyticsExporter()
        with dashboard_fixture.ctrl.request_context_with_library(
            "/?date=2018-01-01&dateEnd=2018-01-04&locations=loc1,loc2"
        ):
            (
                response,
                requested_date,
                date_end,
                library_short_name,
            ) = dashboard_fixture.manager.admin_dashboard_controller.bulk_circulation_events(
                analytics_exporter=exporter
            )

            # export() was called with the arguments we expect.
            #
            args = list(exporter.called_with)
            assert dashboard_fixture.ctrl.db.session == args.pop(0)
            assert datetime.date(2018, 1, 1) == args.pop(0)
            # This is the start of the day _after_ the dateEnd we
            # specified -- we want all events that happened _before_
            # 2018-01-05.
            assert datetime.date(2018, 1, 5) == args.pop(0)
            assert "loc1,loc2" == args.pop(0)
            assert dashboard_fixture.ctrl.db.default_library() == args.pop(0)
            assert [] == args

            # The data returned is whatever export() returned.
            assert "A CSV file" == response

            # The other data is necessary to build a filename for the
            # "CSV file".
            assert "2018-01-01" == requested_date

            # Note that the date_end is the date we requested --
            # 2018-01-04 -- not the cutoff time passed in to export(),
            # which is the start of the subsequent day.
            assert "2018-01-04" == date_end
            assert (
                dashboard_fixture.ctrl.db.default_library().short_name
                == library_short_name
            )

    def test_stats_calls_with_correct_arguments(
        self, dashboard_fixture: DashboardFixture
    ):
        # Ensure that the injected statistics function is called properly.
        stats_mock = mock.MagicMock(return_value={})
        with dashboard_fixture.request_context_with_admin(
            "/", admin=dashboard_fixture.admin
        ):
            response = dashboard_fixture.manager.admin_dashboard_controller.stats(
                stats_function=stats_mock
            )
        assert 1 == stats_mock.call_count
        assert (
            dashboard_fixture.admin,
            dashboard_fixture.ctrl.db.session,
        ) == stats_mock.call_args.args
        assert {} == stats_mock.call_args.kwargs
