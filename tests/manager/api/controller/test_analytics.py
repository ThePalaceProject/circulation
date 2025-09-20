import pytest

from palace.manager.api.problem_details import INVALID_ANALYTICS_EVENT_TYPE
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.util import get_one
from tests.fixtures.api_controller import CirculationControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


class AnalyticsFixture(CirculationControllerFixture):
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        services_fixture.services.analytics.analytics.override(Analytics())
        super().__init__(db, services_fixture)
        [self.lp] = self.english_1.license_pools
        self.identifier = self.lp.identifier


@pytest.fixture(scope="function")
def analytics_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
):
    with AnalyticsFixture.fixture(db, services_fixture) as fixture:
        yield fixture


class TestAnalyticsController:
    def test_track_event(self, analytics_fixture: AnalyticsFixture):
        db = analytics_fixture.db

        with analytics_fixture.request_context_with_library("/"):
            response = analytics_fixture.manager.analytics_controller.track_event(
                analytics_fixture.identifier.type,
                analytics_fixture.identifier.identifier,
                "invalid_type",
            )
            assert 400 == response.status_code
            assert INVALID_ANALYTICS_EVENT_TYPE.uri == response.uri

        patron = db.patron()
        with analytics_fixture.request_context_with_library("/") as ctx:
            setattr(ctx.request, "patron", patron)
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
            db.session.delete(circulation_event)
