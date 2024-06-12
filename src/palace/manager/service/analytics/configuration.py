from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class AnalyticsConfiguration(ServiceConfiguration):
    s3_analytics_enabled: bool = False
