from unittest.mock import PropertyMock, patch

import pytest

from palace.manager.service.analytics import eventdata
from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.flask import FlaskAppFixture


class TestAnalyticsEventData:
    def test_user_agent(
        self,
        db: DatabaseTransactionFixture,
        flask_app_fixture: FlaskAppFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(LogLevel.warning)

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

        # call outside of request context, we don't log a warning, as this is an expected case
        # we just set user_agent to None
        event = AnalyticsEventData.create(library, pool, CirculationEvent.CM_CHECKOUT)
        assert event.user_agent is None
        assert len(caplog.records) == 0

        # Exception getting user agent. This isn't expected, but isn't fatal, so we log a warning
        with patch.object(eventdata, "flask") as mock_flask:
            type(mock_flask.request).user_agent = PropertyMock(
                side_effect=Exception("Test exception")
            )
            event = AnalyticsEventData.create(
                library, pool, CirculationEvent.CM_CHECKOUT
            )
        assert event.user_agent is None
        assert len(caplog.records) == 1
        assert "Unable to resolve the user_agent" in caplog.text
