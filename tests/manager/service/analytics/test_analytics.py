from unittest.mock import MagicMock, patch

import pytest

from palace.manager.service.analytics import analytics as analytics_module
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.analytics.local import LocalAnalyticsProvider
from palace.manager.service.analytics.s3 import S3AnalyticsProvider
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from tests.fixtures.database import DatabaseTransactionFixture


class TestAnalytics:
    def test_is_configured(self) -> None:
        analytics = Analytics()
        assert analytics.is_configured() == True

        analytics = Analytics(s3_analytics_enabled=True)
        assert analytics.is_configured() == True

        # If somehow we don't have providers, we don't have analytics
        analytics.providers = []
        assert analytics.is_configured() == False

    def test_init_analytics(self) -> None:
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

    def test_collect_event_passes_geo_to_event_data(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """collect_event() resolves geo and passes country/state to AnalyticsEventData."""
        library = db.default_library()
        pool = db.licensepool(edition=db.edition())
        library.settings_dict = dict(library.settings_dict)
        library.settings_dict["country"] = "CA"
        library.settings_dict["state"] = "Ontario"

        collected: list = []
        analytics = Analytics()
        analytics.collect = lambda event, session=None: collected.append(event)
        analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)

        assert len(collected) == 1
        assert collected[0].country == "CA"
        assert collected[0].state == "Ontario"

    def test_collect_event_uses_fallback_if_resolve_geo_fails(
        self,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """collect_event() falls back to defaults and logs a warning when resolve_geo raises."""
        library = db.default_library()
        pool = db.licensepool(edition=db.edition())

        collected: list = []
        analytics = Analytics()
        analytics.collect = lambda event, session=None: collected.append(event)

        with patch.object(
            analytics_module, "resolve_geo", side_effect=RuntimeError("boom")
        ):
            analytics.collect_event(library, pool, CirculationEvent.CM_CHECKOUT)

        assert len(collected) == 1
        assert collected[0].country == "US"
        assert collected[0].state == "All"
        assert "Unable to resolve geographic settings" in caplog.text
