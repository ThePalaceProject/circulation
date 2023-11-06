from core.service.configuration import ServiceConfiguration


class AnalyticsConfiguration(ServiceConfiguration):
    s3_analytics_enabled: bool = False
