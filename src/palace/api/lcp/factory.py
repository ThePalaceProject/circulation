from palace.api.lcp.hash import HasherFactory
from palace.api.lcp.server import LCPServer
from palace.core.lcp.credential import LCPCredentialFactory
from palace.core.model.configuration import ConfigurationFactory, ConfigurationStorage


class LCPServerFactory:
    """Creates a new instance of LCPServer"""

    def create(self, integration_association):
        """Creates a new instance of LCPServer

        :param integration_association: Association with an external integration
        :type integration_association: palace.core.model.configuration.HasExternalIntegration

        :return: New instance of LCPServer
        :rtype: LCPServer
        """
        configuration_storage = ConfigurationStorage(integration_association)
        configuration_factory = ConfigurationFactory()
        hasher_factory = HasherFactory()
        credential_factory = LCPCredentialFactory()
        lcp_server = LCPServer(
            configuration_storage,
            configuration_factory,
            hasher_factory,
            credential_factory,
        )

        return lcp_server
