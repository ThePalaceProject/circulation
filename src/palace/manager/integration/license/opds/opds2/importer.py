from __future__ import annotations

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.importer import OpdsImporter
from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.integration.license.opds.opds2.api import OPDS2API
from palace.manager.integration.license.opds.requests import (
    OpdsAuthType,
    get_opds_requests,
)
from palace.manager.opds import opds2
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http.http import HTTP

Opds2ImporterT = OpdsImporter[PublicationFeedNoValidation, opds2.Publication]


def importer_from_collection(
    collection: Collection, registry: LicenseProvidersRegistry
) -> Opds2ImporterT:
    """Create an OPDS2Importer from a Collection."""
    if not registry.equivalent(collection.protocol, OPDS2API):
        raise PalaceValueError(
            f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS2 collection."
        )
    settings = integration_settings_load(
        OPDS2API.settings_class(), collection.integration_configuration
    )
    requests_session = HTTP.session(settings.max_retry_count)
    request = get_opds_requests(
        (
            OpdsAuthType.BASIC
            if settings.username and settings.password
            else OpdsAuthType.NONE
        ),
        settings.username,
        settings.password,
        settings.external_account_id,
        requests_session,
    )
    extractor = OPDS2WithODLExtractor(
        opds2.Publication.model_validate,
        settings.external_account_id,
        settings.data_source,
    )
    return OpdsImporter(
        request,
        extractor,
        settings.external_account_id,
        settings.custom_accept_header,
        settings.ignored_identifier_types,
    )
