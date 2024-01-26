from api.metadata.base import MetadataServiceType
from api.metadata.novelist import NoveListAPI
from api.metadata.nyt import NYTBestSellerAPI
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry


class MetadataRegistry(IntegrationRegistry[MetadataServiceType]):
    def __init__(self) -> None:
        super().__init__(Goals.METADATA_GOAL)

        self.register(NYTBestSellerAPI, canonical="New York Times")
        self.register(NoveListAPI, canonical="NoveList Select")
