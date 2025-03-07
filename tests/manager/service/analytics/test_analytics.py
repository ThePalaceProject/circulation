from unittest.mock import MagicMock

from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.analytics.local import LocalAnalyticsProvider
from palace.manager.service.analytics.s3 import S3AnalyticsProvider


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
