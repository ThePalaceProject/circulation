from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from api.s3_analytics_provider import S3AnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider
from core.util.datetime_helpers import utc_now
from core.util.log import LoggerMixin

if TYPE_CHECKING:
    from core.service.storage.s3 import S3Service


class Analytics(LoggerMixin):
    """Dispatches methods for analytics providers."""

    def __init__(
        self,
        s3_analytics_enabled: bool = False,
        s3_service: Optional[S3Service] = None,
    ) -> None:
        self.providers = [LocalAnalyticsProvider()]

        if s3_analytics_enabled:
            if s3_service is not None:
                self.providers.append(S3AnalyticsProvider(s3_service))
            else:
                self.log.info(
                    "S3 analytics is not configured: No analytics bucket was specified."
                )

    def collect_event(self, library, license_pool, event_type, time=None, **kwargs):
        if not time:
            time = utc_now()

        for provider in self.providers:
            provider.collect_event(library, license_pool, event_type, time, **kwargs)

    def is_configured(self):
        return len(self.providers) > 0
