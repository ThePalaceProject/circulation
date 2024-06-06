from __future__ import annotations

from typing import TYPE_CHECKING

from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.base import IntegrationRegistry

if TYPE_CHECKING:
    from palace.manager.api.circulation import CirculationApiType  # noqa: autoflake


class LicenseProvidersRegistry(IntegrationRegistry["CirculationApiType"]):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)

        from palace.manager.api.axis import Axis360API
        from palace.manager.api.bibliotheca import BibliothecaAPI
        from palace.manager.api.enki import EnkiAPI
        from palace.manager.api.odl import ODLAPI
        from palace.manager.api.odl2 import ODL2API
        from palace.manager.api.opds_for_distributors import OPDSForDistributorsAPI
        from palace.manager.api.overdrive import OverdriveAPI
        from palace.manager.core.opds2_import import OPDS2API
        from palace.manager.core.opds_import import OPDSAPI

        self.register(OverdriveAPI, canonical=OverdriveAPI.label())
        self.register(BibliothecaAPI, canonical=BibliothecaAPI.label())
        self.register(Axis360API, canonical=Axis360API.label())
        self.register(EnkiAPI, canonical=EnkiAPI.label())
        self.register(OPDSForDistributorsAPI, canonical=OPDSForDistributorsAPI.label())
        self.register(ODLAPI, canonical=ODLAPI.label())
        self.register(ODL2API, canonical=ODL2API.label())
        self.register(OPDSAPI, canonical=OPDSAPI.label())
        self.register(OPDS2API, canonical=OPDS2API.label())
