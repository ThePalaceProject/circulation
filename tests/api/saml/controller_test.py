from palace.api.saml.provider import SAMLWebSSOAuthenticationProvider
from palace.core.model import ExternalIntegration
from tests.api.test_controller import ControllerTest as BaseControllerTest


class ControllerTest(BaseControllerTest):
    def setup_method(self):
        self._integration = None
        super().setup_method()

        self._integration = self._external_integration(
            protocol=SAMLWebSSOAuthenticationProvider.NAME,
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
        )
