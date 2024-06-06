from __future__ import annotations

from dependency_injector.containers import DeclarativeContainer
from dependency_injector.providers import Provider, Singleton

from palace.manager.service.integration_registry.catalog_services import (
    CatalogServicesRegistry,
)
from palace.manager.service.integration_registry.discovery import DiscoveryRegistry
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.service.integration_registry.metadata import MetadataRegistry
from palace.manager.service.integration_registry.patron_auth import PatronAuthRegistry


class IntegrationRegistryContainer(DeclarativeContainer):
    catalog_services: Provider[CatalogServicesRegistry] = Singleton(
        CatalogServicesRegistry
    )

    discovery: Provider[DiscoveryRegistry] = Singleton(DiscoveryRegistry)

    license_providers: Provider[LicenseProvidersRegistry] = Singleton(
        LicenseProvidersRegistry
    )

    metadata: Provider[MetadataRegistry] = Singleton(MetadataRegistry)

    patron_auth: Provider[PatronAuthRegistry] = Singleton(PatronAuthRegistry)
