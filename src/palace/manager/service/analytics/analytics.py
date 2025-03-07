from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.analytics.local import LocalAnalyticsProvider
from palace.manager.service.analytics.provider import AnalyticsProvider
from palace.manager.service.analytics.s3 import S3AnalyticsProvider
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.log import LoggerMixin

if TYPE_CHECKING:
    from palace.manager.service.storage.s3 import S3Service


class Analytics(LoggerMixin, AnalyticsProvider):
    """Dispatches methods for analytics providers."""

    def __init__(
        self,
        s3_analytics_enabled: bool = False,
        s3_service: S3Service | None = None,
    ) -> None:
        self.providers: list[AnalyticsProvider] = [LocalAnalyticsProvider()]

        if s3_analytics_enabled:
            if s3_service is not None:
                self.providers.append(S3AnalyticsProvider(s3_service))
            else:
                self.log.info(
                    "S3 analytics is not configured: No analytics bucket was specified."
                )

    def collect(
        self,
        event: AnalyticsEventData,
        session: Session | None = None,
    ) -> None:
        for provider in self.providers:
            provider.collect(event, session)

    def collect_event(
        self,
        library: Library,
        license_pool: LicensePool | None,
        event_type: str,
        time: datetime | None = None,
        old_value: int | None = None,
        new_value: int | None = None,
        patron: Patron | None = None,
    ) -> None:
        session = Session.object_session(library)
        event = AnalyticsEventData.create(
            library,
            license_pool,
            event_type,
            time,
            old_value,
            new_value,
            patron,
        )
        self.collect(event, session=session)

    def is_configured(self) -> bool:
        return len(self.providers) > 0
