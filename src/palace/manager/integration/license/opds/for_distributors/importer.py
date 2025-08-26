from __future__ import annotations

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.importer import OpdsImporter
from palace.manager.integration.license.opds.opds1.extractor import (
    Opds1Extractor,
    OPDS1Feed,
    OPDS1Publication,
)
from palace.manager.integration.license.opds.requests import OAuthOpdsRequest
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection

OpdsForDistributorsImporterT = OpdsImporter[OPDS1Feed, OPDS1Publication]


def importer_from_collection(
    collection: Collection, registry: LicenseProvidersRegistry
) -> OpdsForDistributorsImporterT:
    if not registry.equivalent(collection.protocol, OPDSForDistributorsAPI):
        raise PalaceValueError(
            f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not an OPDS for Distributors collection."
        )
    settings = integration_settings_load(
        OPDSForDistributorsAPI.settings_class(), collection.integration_configuration
    )
    request = OAuthOpdsRequest(
        settings.external_account_id, settings.username, settings.password
    )
    extractor = Opds1Extractor(
        settings.external_account_id,
        settings.data_source,
        settings.primary_identifier_source,
        opds_for_distributors=True,
    )
    return OpdsImporter(
        request,
        extractor,
        settings.external_account_id,
        settings.custom_accept_header,
        [],
    )
