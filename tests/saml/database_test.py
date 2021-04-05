from api.saml.provider import SAMLWebSSOAuthenticationProvider
from core.model import ExternalIntegration
from core.testing import DatabaseTest as BaseDatabaseTest


class DatabaseTest(BaseDatabaseTest):
    def setup_method(self):
        super(DatabaseTest, self).setup_method()

        self._integration = self._external_integration(
            protocol="api.saml.provider",
            goal=ExternalIntegration.PATRON_AUTH_GOAL,
            libraries=self._default_library
        )

        # We have to make sure that the external integration has an ID
        # because it's used in AuthenticationProvider's constructor.
        self._db.commit()

        self._authentication_provider = SAMLWebSSOAuthenticationProvider(
            self._default_library, self._integration
        )
