from __future__ import annotations

from celery.canvas import Signature

from palace.manager.api.circulation.base import SupportsImport, SupportsReaping
from palace.manager.integration.license.opds.base.api import BaseOPDSAPI
from palace.manager.integration.license.opds.opds1.settings import (
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
)


class OPDSAPI(
    BaseOPDSAPI[OPDSImporterSettings, OPDSImporterLibrarySettings],
    SupportsImport,
    SupportsReaping,
):
    @classmethod
    def settings_class(cls) -> type[OPDSImporterSettings]:
        return OPDSImporterSettings

    @classmethod
    def library_settings_class(cls) -> type[OPDSImporterLibrarySettings]:
        return OPDSImporterLibrarySettings

    @classmethod
    def description(cls) -> str:
        return "Import books from a publicly-accessible OPDS feed."

    @classmethod
    def label(cls) -> str:
        return "OPDS Import"

    @classmethod
    def import_task(cls, collection_id: int, force: bool = False) -> Signature:
        from palace.manager.celery.tasks.opds1 import import_collection

        return import_collection.s(collection_id, force=force)

    @classmethod
    def reap_task(cls, collection_id: int) -> Signature:
        # Local import to avoid a circular import between this module and the
        # opds1 celery tasks module, which imports OPDSAPI.
        from palace.manager.celery.tasks.opds1 import import_and_reap_not_found_chord

        return import_and_reap_not_found_chord(collection_id)
