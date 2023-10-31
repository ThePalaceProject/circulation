from unittest.mock import MagicMock

from api.s3_analytics_provider import S3AnalyticsProvider
from core.analytics import Analytics
from core.local_analytics_provider import LocalAnalyticsProvider

# We can't import mock_analytics_provider from within a test,
# and we can't tell Analytics to do so either. We need to tell
# it to perform an import relative to the module the Analytics
# class is in.

MOCK_PROTOCOL = "..mock_analytics_provider"


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
