from palace.api.lcp.collection import LCPAPI
from palace.core.model import ExternalIntegration
from palace.core.testing import DatabaseTest as BaseDatabaseTest


class DatabaseTest(BaseDatabaseTest):
    def setup_method(self):
        self._integration = None
        self._authentication_provider = None

        super().setup_method()

        self._integration = self._external_integration(
            protocol=LCPAPI.NAME, goal=ExternalIntegration.LICENSE_GOAL
        )
