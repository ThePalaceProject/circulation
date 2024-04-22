from palace.manager.core.marc import MARCExporter
from palace.manager.integration.goals import Goals
from palace.manager.integration.registry.base import IntegrationRegistry


class CatalogServicesRegistry(IntegrationRegistry[MARCExporter]):
    def __init__(self) -> None:
        super().__init__(Goals.CATALOG_GOAL)
        self.register(MARCExporter)
