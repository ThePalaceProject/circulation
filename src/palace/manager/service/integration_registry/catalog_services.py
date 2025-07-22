from __future__ import annotations

from typing import TYPE_CHECKING

from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.base import IntegrationRegistry

if TYPE_CHECKING:
    from palace.manager.integration.catalog.marc.exporter import (  # noqa: autoflake
        MarcExporter,
    )


class CatalogServicesRegistry(IntegrationRegistry["MarcExporter"]):
    def __init__(self) -> None:
        from palace.manager.integration.catalog.marc.exporter import MarcExporter

        super().__init__(Goals.CATALOG_GOAL)
        self.register(MarcExporter)
