from api.lcp.hash import HasherFactory
from api.lcp.server import LCPServer
from core.lcp.credential import LCPCredentialFactory


class LCPServerFactory:
    """Creates a new instance of LCPServer"""

    def create(self, integration_association) -> LCPServer:
        """Creates a new instance of LCPServer

        :param integration_association: Association with an external integration
        :type integration_association: core.model.configuration.HasExternalIntegration

        :return: New instance of LCPServer
        :rtype: LCPServer
        """
        hasher_factory = HasherFactory()
        credential_factory = LCPCredentialFactory()
        lcp_server = LCPServer(
            integration_association.configuration,
            hasher_factory,
            credential_factory,
        )

        return lcp_server
