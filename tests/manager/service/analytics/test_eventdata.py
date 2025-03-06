from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class TestAnalyticsEventData:
    def test_user_agent(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
    ) -> None:
        edition = db.edition()
        pool = db.licensepool(edition=edition)
        library = db.default_library()

        # user agent present
        user_agent = "test_user_agent"
        headers = {"User-Agent": user_agent}
        with flask_app_fixture.test_request_context("/", headers=headers):
            event = AnalyticsEventData.create(
                library, pool, CirculationEvent.CM_CHECKOUT
            )
        assert event.user_agent == user_agent

        # user agent empty
        headers = {"User-Agent": ""}
        with flask_app_fixture.test_request_context("/", headers=headers):
            event = AnalyticsEventData.create(
                library, pool, CirculationEvent.CM_CHECKOUT
            )
        assert event.user_agent is None

        # no user agent header.
        headers = {}
        with flask_app_fixture.test_request_context("/", headers=headers):
            event = AnalyticsEventData.create(
                library, pool, CirculationEvent.CM_CHECKOUT
            )
        assert event.user_agent is None

        # call outside of request context
        event = AnalyticsEventData.create(library, pool, CirculationEvent.CM_CHECKOUT)
        assert event.user_agent is None
