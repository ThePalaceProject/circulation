from palace.manager.api.metadata.base import MetadataServiceType
from palace.manager.api.metadata.novelist import NoveListAPI
from palace.manager.api.metadata.nyt import NYTBestSellerAPI
from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.base import IntegrationRegistry


class MetadataRegistry(IntegrationRegistry[MetadataServiceType]):
    def __init__(self) -> None:
        super().__init__(Goals.METADATA_GOAL)

        self.register(NYTBestSellerAPI, canonical="New York Times")
        self.register(NoveListAPI, canonical="NoveList Select")
