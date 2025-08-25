from __future__ import annotations

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.importer import OpdsImporter
from palace.manager.integration.license.opds.opds1.api import OPDSAPI
from palace.manager.integration.license.opds.opds1.extractor import (
    Opds1Extractor,
    OPDS1Feed,
    OPDS1Publication,
)
from palace.manager.integration.license.opds.requests import (
    OpdsAuthType,
    get_opds_requests,
)
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection

Opds1ImporterT = OpdsImporter[OPDS1Feed, OPDS1Publication]


def importer_from_collection(
    collection: Collection, registry: LicenseProvidersRegistry
) -> Opds1ImporterT:
    if not registry.equivalent(collection.protocol, OPDSAPI):
        raise PalaceValueError(
            f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS1 collection."
        )
    settings = integration_settings_load(
        OPDSAPI.settings_class(), collection.integration_configuration
    )
    request = get_opds_requests(
        (
            OpdsAuthType.BASIC
            if settings.username and settings.password
            else OpdsAuthType.NONE
        ),
        settings.username,
        settings.password,
        settings.external_account_id,
    )
    extractor = Opds1Extractor(
        settings.external_account_id,
        settings.data_source,
        settings.primary_identifier_source,
    )
    return OpdsImporter(
        request,
        extractor,
        settings.external_account_id,
        settings.custom_accept_header,
        [],
    )
