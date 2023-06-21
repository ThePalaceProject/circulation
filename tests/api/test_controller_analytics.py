import flask
import pytest

from api.problem_details import INVALID_ANALYTICS_EVENT_TYPE
from core.analytics import Analytics
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import CirculationEvent, ExternalIntegration, create, get_one
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.vendor_id import VendorIDFixture


class AnalyticsFixture(CirculationControllerFixture):
    def __init__(
        self, db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
    ):
        super().__init__(db, vendor_id_fixture)
        [self.lp] = self.english_1.license_pools
        self.identifier = self.lp.identifier


@pytest.fixture(scope="function")
def analytics_fixture(
    db: DatabaseTransactionFixture, vendor_id_fixture: VendorIDFixture
):
    return AnalyticsFixture(db, vendor_id_fixture)


class TestAnalyticsController:
    def test_track_event(self, analytics_fixture: AnalyticsFixture):
        db = analytics_fixture.db

        integration, ignore = create(
            db.session,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol="core.local_analytics_provider",
        )
        integration.setting(
            LocalAnalyticsProvider.LOCATION_SOURCE
        ).value = LocalAnalyticsProvider.LOCATION_SOURCE_NEIGHBORHOOD

        # The Analytics singleton will have already been instantiated,
        # so here we simulate a reload of its configuration with `refresh`.
        analytics_fixture.manager.analytics = Analytics(db.session, refresh=True)

        with analytics_fixture.request_context_with_library("/"):
            response = analytics_fixture.manager.analytics_controller.track_event(
                analytics_fixture.identifier.type,
                analytics_fixture.identifier.identifier,
                "invalid_type",
            )
            assert 400 == response.status_code
            assert INVALID_ANALYTICS_EVENT_TYPE.uri == response.uri

        # If there is no active patron, or if the patron has no
        # associated neighborhood, the CirculationEvent is created
        # with no location.
        patron = db.patron()
        for request_patron in (None, patron):
            with analytics_fixture.request_context_with_library("/"):
                flask.request.patron = request_patron  # type: ignore
                response = analytics_fixture.manager.analytics_controller.track_event(
                    analytics_fixture.identifier.type,
                    analytics_fixture.identifier.identifier,
                    "open_book",
                )
                assert 200 == response.status_code

                circulation_event = get_one(
                    db.session,
                    CirculationEvent,
                    type="open_book",
                    license_pool=analytics_fixture.lp,
                )
                assert None is not circulation_event
                assert None == circulation_event.location
                db.session.delete(circulation_event)

        # If the patron has an associated neighborhood, and the
        # analytics controller is set up to use patron neighborhood as
        # event location, then the CirculationEvent is created with
        # that neighborhood as its location.
        patron.neighborhood = "Mars Grid 4810579"
        with analytics_fixture.request_context_with_library("/"):
            flask.request.patron = patron  # type: ignore
            response = analytics_fixture.manager.analytics_controller.track_event(
                analytics_fixture.identifier.type,
                analytics_fixture.identifier.identifier,
                "open_book",
            )
            assert 200 == response.status_code

            circulation_event = get_one(
                db.session,
                CirculationEvent,
                type="open_book",
                license_pool=analytics_fixture.lp,
            )
            assert circulation_event is not None
            assert patron.neighborhood == circulation_event.location
            db.session.delete(circulation_event)
