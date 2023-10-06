from __future__ import annotations

from typing import TYPE_CHECKING

from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.model.configuration import ExternalIntegration

if TYPE_CHECKING:
    from api.circulation import BaseCirculationAPI  # noqa: autoflake
    from core.integration.settings import BaseSettings  # noqa: autoflake


class LicenseProvidersRegistry(
    IntegrationRegistry["BaseCirculationAPI[BaseSettings, BaseSettings]"]
):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)

        from api.axis import Axis360API
        from api.bibliotheca import BibliothecaAPI
        from api.enki import EnkiAPI
        from api.odilo import OdiloAPI
        from api.odl import ODLAPI
        from api.odl2 import ODL2API
        from api.opds_for_distributors import OPDSForDistributorsAPI
        from api.overdrive import OverdriveAPI
        from core.opds2_import import OPDS2API
        from core.opds_import import OPDSAPI

        self.register(OverdriveAPI, canonical=ExternalIntegration.OVERDRIVE)
        self.register(OdiloAPI, canonical=ExternalIntegration.ODILO)
        self.register(BibliothecaAPI, canonical=ExternalIntegration.BIBLIOTHECA)
        self.register(Axis360API, canonical=ExternalIntegration.AXIS_360)
        self.register(EnkiAPI, canonical=EnkiAPI.ENKI_EXTERNAL)
        self.register(OPDSForDistributorsAPI, canonical=OPDSForDistributorsAPI.label())
        self.register(ODLAPI, canonical=ODLAPI.label())
        self.register(ODL2API, canonical=ODL2API.label())
        self.register(OPDSAPI, canonical=ExternalIntegration.OPDS_IMPORT)
        self.register(OPDS2API, canonical=ExternalIntegration.OPDS2_IMPORT)
