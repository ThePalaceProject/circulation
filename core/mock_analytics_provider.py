class MockAnalyticsProvider:
    """A mock analytics provider that keeps track of how many times it's called."""

    def __init__(self, integration=None, services=None, library=None):
        """
        Since this is a mock analytics provider, it doesn't need to do anything
        with the integration or services. It just needs to keep track of how
        many times it's called.

        :param integration: The ExternalIntegration that configures this analytics service.
        :param services: The Service object that provides services to this provider.
        :param library: The library this analytics provider is associated with.
        """
        self.count = 0
        self.event = None
        if integration:
            self.url = integration.url

    def collect_event(self, library, lp, event_type, time=None, **kwargs):
        self.count = self.count + 1
        self.event_type = event_type
        self.time = time


# The Analytics class looks for the name "Provider".
Provider = MockAnalyticsProvider
