from api.discovery.opds_registration import OpdsRegistrationService
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry


class DiscoveryRegistry(IntegrationRegistry[OpdsRegistrationService]):
    def __init__(self) -> None:
        super().__init__(Goals.DISCOVERY_GOAL)

        self.register(OpdsRegistrationService, canonical="OPDS Registration")
