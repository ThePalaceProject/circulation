from __future__ import annotations

from typing import Dict, Type

from api.circulation import BaseCirculationAPIProtocol
from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.model.configuration import ExternalIntegration


class LicenseProvidersRegistry(IntegrationRegistry[BaseCirculationAPIProtocol]):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)

        from api.axis import Axis360API
        from api.bibliotheca import BibliothecaAPI
        from api.enki import EnkiAPI
        from api.lcp.collection import LCPAPI
        from api.odilo import OdiloAPI
        from api.odl import ODLAPI, SharedODLAPI
        from api.odl2 import ODL2API
        from api.opds_for_distributors import OPDSForDistributorsAPI
        from api.overdrive import OverdriveAPI
        from core.opds2_import import OPDS2Importer
        from core.opds_import import OPDSImporter

        apis: Dict[str, Type[BaseCirculationAPIProtocol]] = {
            ExternalIntegration.OVERDRIVE: OverdriveAPI,
            ExternalIntegration.ODILO: OdiloAPI,
            ExternalIntegration.BIBLIOTHECA: BibliothecaAPI,
            ExternalIntegration.AXIS_360: Axis360API,
            EnkiAPI.ENKI_EXTERNAL: EnkiAPI,
            OPDSForDistributorsAPI.NAME: OPDSForDistributorsAPI,
            ODLAPI.NAME: ODLAPI,
            ODL2API.NAME: ODL2API,
            SharedODLAPI.NAME: SharedODLAPI,
            LCPAPI.NAME: LCPAPI,
            OPDSImporter.NAME: OPDSImporter,
            OPDS2Importer.NAME: OPDS2Importer,
        }

        for name, api in apis.items():
            self.register(api, canonical=name)
