from typing import Optional

from core.service.configuration import ServiceConfiguration


class AnalyticsConfiguration(ServiceConfiguration):
    local_analytics_enabled: bool = False
    s3_analytics_enabled: bool = False
    location_source: Optional[str] = None
