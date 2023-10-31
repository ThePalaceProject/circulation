from api.s3_analytics_provider import S3AnalyticsProvider
from core.analytics import Analytics
from core.local_analytics_provider import LocalAnalyticsProvider
from core.service.storage.container import Storage

# We can't import mock_analytics_provider from within a test,
# and we can't tell Analytics to do so either. We need to tell
# it to perform an import relative to the module the Analytics
# class is in.

MOCK_PROTOCOL = "..mock_analytics_provider"


class TestAnalytics:
    def test_is_configured(self):
        analytics = Analytics(config={})
        print(analytics.config)
        assert analytics.is_configured() == False

        analytics = Analytics(config=dict(local_analytics_enabled=True))
        assert analytics.is_configured() == True

        analytics = Analytics(config=dict(s3_analytics_enabled=True))
        assert analytics.is_configured() == True

    def test_init_analytics(self):
        analytics = Analytics(
            config=dict(local_analytics_enabled=True, location_source=None),
            storage=None,
        )

        assert len(analytics.providers) == 1
        assert type(analytics.providers[0]) == LocalAnalyticsProvider

        analytics = Analytics(
            config=dict(
                local_analytics_enabled=True,
                s3_analytics_enabled=True,
                location_source=None,
            ),
            storage=Storage(
                config=dict(
                    aws_access_key="key",
                    aws_secret_key="secret",
                    region="us-west-2",
                    analytics_bucket="bucket",
                    url_template="http://template{key}",
                )
            ),
        )

        assert len(analytics.providers) == 2
        assert type(analytics.providers[0]) == LocalAnalyticsProvider
        assert type(analytics.providers[1]) == S3AnalyticsProvider
