from palace.manager.api.discovery.opds_registration import OpdsRegistrationService
from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.base import IntegrationRegistry


class DiscoveryRegistry(IntegrationRegistry[OpdsRegistrationService]):
    def __init__(self) -> None:
        super().__init__(Goals.DISCOVERY_GOAL)

        self.register(OpdsRegistrationService, canonical="OPDS Registration")
