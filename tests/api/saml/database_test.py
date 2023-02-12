from palace.api.saml.provider import SAMLWebSSOAuthenticationProvider
from palace.core.model import ExternalIntegration
from palace.core.testing import DatabaseTest as BaseDatabaseTest


class DatabaseTest(BaseDatabaseTest):
    def setup_method(self):
        super().setup_method()

        self._integration = self._external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
        self._authentication_provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )
