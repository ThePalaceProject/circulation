from __future__ import annotations

from typing import TYPE_CHECKING

from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry

if TYPE_CHECKING:
    from api.circulation import CirculationApiType  # noqa: autoflake


class LicenseProvidersRegistry(IntegrationRegistry["CirculationApiType"]):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)

        from api.axis import Axis360API
        from api.bibliotheca import BibliothecaAPI
        from api.enki import EnkiAPI
        from api.odl import ODLAPI
        from api.odl2 import ODL2API
        from api.opds_for_distributors import OPDSForDistributorsAPI
        from api.overdrive import OverdriveAPI
        from core.opds2_import import OPDS2API
        from core.opds_import import OPDSAPI

        self.register(OverdriveAPI, canonical=OverdriveAPI.label())
        self.register(BibliothecaAPI, canonical=BibliothecaAPI.label())
        self.register(Axis360API, canonical=Axis360API.label())
        self.register(EnkiAPI, canonical=EnkiAPI.label())
        self.register(OPDSForDistributorsAPI, canonical=OPDSForDistributorsAPI.label())
        self.register(ODLAPI, canonical=ODLAPI.label())
        self.register(ODL2API, canonical=ODL2API.label())
        self.register(OPDSAPI, canonical=OPDSAPI.label())
        self.register(OPDS2API, canonical=OPDS2API.label())
