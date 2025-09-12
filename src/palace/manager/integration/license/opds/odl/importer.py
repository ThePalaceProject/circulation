from __future__ import annotations

from pydantic import TypeAdapter

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.integration.base import integration_settings_load
from palace.manager.integration.license.opds.importer import OpdsImporter
from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
from palace.manager.integration.license.opds.odl.extractor import OPDS2WithODLExtractor
from palace.manager.integration.license.opds.requests import (
    OpdsAuthType,
    get_opds_requests,
)
from palace.manager.opds.odl.odl import Opds2OrOpds2WithOdlPublication
from palace.manager.opds.opds2 import PublicationFeedNoValidation
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.util.http.http import HTTP

_ODL_PUBLICATION_ADAPTOR: TypeAdapter[Opds2OrOpds2WithOdlPublication] = TypeAdapter(
    Opds2OrOpds2WithOdlPublication
)


Opds2WithODLImporterT = OpdsImporter[
    PublicationFeedNoValidation, Opds2OrOpds2WithOdlPublication
]


def importer_from_collection(
    collection: Collection, registry: LicenseProvidersRegistry
) -> Opds2WithODLImporterT:
    """
    Create an OPDS2WithODLImporter from a OPDS2+ODL (OPDS2WithODLApi protocol) Collection.
    """
    if not registry.equivalent(collection.protocol, OPDS2WithODLApi):
        raise PalaceValueError(
            f"Collection {collection.name} [id={collection.id} protocol={collection.protocol}] is not a OPDS2+ODL collection."
        )
    settings = integration_settings_load(
        OPDS2WithODLApi.settings_class(), collection.integration_configuration
    )
    requests_session = HTTP.session(settings.max_retry_count)
    request = get_opds_requests(
        settings.auth_type,
        settings.username,
        settings.password,
        settings.external_account_id,
        requests_session,
    )
    extractor = OPDS2WithODLExtractor(
        _ODL_PUBLICATION_ADAPTOR.validate_python,
        settings.external_account_id,
        settings.data_source,
        settings.skipped_license_formats,
        settings.auth_type == OpdsAuthType.OAUTH,
    )
    return OpdsImporter(
        request,
        extractor,
        settings.external_account_id,
        settings.custom_accept_header,
        settings.ignored_identifier_types,
    )
