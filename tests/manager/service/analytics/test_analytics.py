import datetime
from collections.abc import Generator
from unittest.mock import MagicMock

import pytest

from palace.manager.api.s3_analytics_provider import S3AnalyticsProvider
from palace.manager.core.local_analytics_provider import LocalAnalyticsProvider
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from tests.fixtures.api_controller import ControllerFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.services import ServicesFixture


@pytest.fixture(scope="function")
def analytics_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> Generator[ControllerFixture, None, None]:
    fixture = ControllerFixture(db, services_fixture)
    with fixture.wired_container():
        yield fixture


class TestAnalytics:
    def test_is_configured(self):
        analytics = Analytics()
        assert analytics.is_configured() == True

        analytics = Analytics(s3_analytics_enabled=True)
        assert analytics.is_configured() == True

        # If somehow we don't have providers, we don't have analytics
        analytics.providers = []
        assert analytics.is_configured() == False

    def test_init_analytics(self):
        analytics = Analytics()

        assert len(analytics.providers) == 1
        assert type(analytics.providers[0]) == LocalAnalyticsProvider

        analytics = Analytics(
            s3_analytics_enabled=True,
            s3_service=MagicMock(),
        )

        assert len(analytics.providers) == 2
        assert type(analytics.providers[0]) == LocalAnalyticsProvider
        assert type(analytics.providers[1]) == S3AnalyticsProvider

    def test_user_agent_capture(
        self,
        analytics_fixture: ControllerFixture,
    ):
        db = analytics_fixture.db
        edition = db.edition()
        pool = db.licensepool(edition=edition)
        library = db.default_library()

        # user agent present
        user_agent = "test_user_agent"
        headers = {"User-Agent": user_agent}
        analytics, provider = self.setup_analytics_mocks()
        with analytics_fixture.request_context_with_library("/", headers=headers):
            analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)
            kwargs = provider.collect_event.call_args.kwargs
            assert kwargs["user_agent"] == user_agent
            args = provider.collect_event.call_args[0]
            assert args[0] == library
            assert args[1] == pool
            assert args[2] == CirculationEvent.CM_CHECKOUT
            assert isinstance(args[3], datetime.datetime)

        # user agent empty
        user_agent = ""
        headers = {"User-Agent": user_agent}
        analytics, provider = self.setup_analytics_mocks()
        with analytics_fixture.request_context_with_library("/", headers=headers):
            analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)
            kwargs = provider.collect_event.call_args.kwargs
            assert kwargs["user_agent"] is None

        # no user agent header.
        headers = {}
        analytics, provider = self.setup_analytics_mocks()
        with analytics_fixture.request_context_with_library("/", headers=headers):
            analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)
            assert provider.collect_event.call_args.kwargs["user_agent"] is None

        # call outside of request context
        analytics, provider = self.setup_analytics_mocks()
        analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)
        assert provider.collect_event.call_args.kwargs["user_agent"] is None

    def setup_analytics_mocks(self):
        provider = MagicMock()
        analytics = Analytics()
        analytics.providers.append(provider)
        return analytics, provider
