from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from api.s3_analytics_provider import S3AnalyticsProvider
from core.local_analytics_provider import LocalAnalyticsProvider
from core.service.analytics.configuration import AnalyticsConfiguration
from core.util.datetime_helpers import utc_now
from core.util.log import LoggerMixin

if TYPE_CHECKING:
    from dependency_injector.providers import Container

    from core.service.storage.container import Storage


class Analytics(LoggerMixin):
    """Dispatches methods for analytics providers."""

    def __init__(
        self,
        config: Optional[dict] = None,
        storage: Optional[Container[Storage]] = None,
    ) -> None:
        self.providers = []
        self.config = AnalyticsConfiguration.from_values(
            **(config if config is not None else {})
        )
        if self.config.local_analytics_enabled:
            self.providers.append(LocalAnalyticsProvider(self.config))

        if self.config.s3_analytics_enabled:
            if storage is not None and storage.analytics() is not None:
                self.providers.append(S3AnalyticsProvider(storage, self.config))
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
        return self.config.local_analytics_enabled or self.config.s3_analytics_enabled
