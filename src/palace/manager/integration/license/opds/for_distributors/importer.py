from __future__ import annotations

from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.format import FormatData
from palace.manager.integration.license.opds.for_distributors.api import (
    OPDSForDistributorsAPI,
)
from palace.manager.integration.license.opds.for_distributors.settings import (
    OPDSForDistributorsSettings,
)
from palace.manager.integration.license.opds.opds1.importer import OPDSImporter
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanism,
    LicensePool,
    RightsStatus,
)
from palace.manager.sqlalchemy.model.resource import Hyperlink
from palace.manager.sqlalchemy.model.work import Work


class OPDSForDistributorsImporter(OPDSImporter):
    NAME = OPDSForDistributorsAPI.label()

    @classmethod
    def settings_class(cls) -> type[OPDSForDistributorsSettings]:
        return OPDSForDistributorsSettings

    def update_work_for_edition(
        self,
        edition: Edition,
        is_open_access: bool = False,
    ) -> tuple[LicensePool | None, Work | None]:
        """After importing a LicensePool, set its availability appropriately.

        Books imported through OPDS For Distributors can be designated as
        either Open Access (handled elsewhere) or licensed (handled here). For
        licensed content, a library that can perform this import is deemed to
        have a license for the title and can distribute unlimited copies.
        """
        pool, work = super().update_work_for_edition(edition, is_open_access=False)
        if pool:
            pool.unlimited_access = True

        return pool, work

    @classmethod
    def _add_format_data(cls, circulation: CirculationData) -> None:
        for link in circulation.links:
            if (
                link.rel == Hyperlink.GENERIC_OPDS_ACQUISITION
                and link.media_type in OPDSForDistributorsAPI.SUPPORTED_MEDIA_TYPES
            ):
                circulation.formats.append(
                    FormatData(
                        content_type=link.media_type,
                        drm_scheme=DeliveryMechanism.BEARER_TOKEN,
                        link=link,
                        rights_uri=RightsStatus.IN_COPYRIGHT,
                    )
                )
