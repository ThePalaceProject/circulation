from __future__ import annotations

from palace.manager.integration.license.opds.base.api import BaseOPDSAPI
from palace.manager.integration.license.opds.opds1.settings import (
    OPDSImporterLibrarySettings,
    OPDSImporterSettings,
)


class OPDSAPI(BaseOPDSAPI):
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
