from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.marc import MARCExporter


class CatalogServicesRegistry(IntegrationRegistry[MARCExporter]):
    def __init__(self) -> None:
        super().__init__(Goals.CATALOG_GOAL)
        self.register(MARCExporter)
