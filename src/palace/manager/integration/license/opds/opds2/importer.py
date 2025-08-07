from __future__ import annotations

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.integration.license.opds.odl.importer import OPDS2WithODLImporter
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.opds2.settings import OPDS2ImporterSettings
from palace.manager.integration.license.opds.requests import (
    OPDS2AuthType,
    get_opds_requests,
)
from palace.manager.opds import opds2
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection


def importer_from_collection(
    collection: Collection, registry: LicenseProvidersRegistry
) -> OPDS2WithODLImporter[opds2.Publication, OPDS2ImporterSettings]:
    """Create an OPDS2Importer from a Collection."""
    if not registry.equivalent(collection.protocol, OPDS2API):
        raise PalaceValueError(
            f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS2 collection."
        )
    settings = integration_settings_load(
        OPDS2API.settings_class(), collection.integration_configuration
    )
    request = get_opds_requests(
        (
            OPDS2AuthType.BASIC
            if settings.username and settings.password
            else OPDS2AuthType.NONE
        ),
        settings.username,
        settings.password,
        settings.external_account_id,
    )
    extractor = OPDS2WithODLExtractor(
        settings.external_account_id, settings.data_source
    )
    return OPDS2WithODLImporter(
        request, extractor, opds2.Publication.model_validate, settings
    )
