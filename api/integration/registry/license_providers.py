from __future__ import annotations

from typing import TYPE_CHECKING, Union

from core.integration.goals import Goals
from core.integration.registry import IntegrationRegistry
from core.model.configuration import ExternalIntegration

if TYPE_CHECKING:
    from api.circulation import BaseCirculationAPI  # noqa: autoflake
    from core.integration.settings import BaseSettings  # noqa: autoflake
    from core.opds_import import OPDSImporter  # noqa: autoflake


class LicenseProvidersRegistry(
    IntegrationRegistry[
        Union["BaseCirculationAPI[BaseSettings, BaseSettings]", "OPDSImporter"]
    ]
):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)
        self.update(CirculationLicenseProvidersRegistry())
        self.update(OpenAccessLicenseProvidersRegistry())


class CirculationLicenseProvidersRegistry(
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

        self.register(OverdriveAPI, canonical=ExternalIntegration.OVERDRIVE)
        self.register(OdiloAPI, canonical=ExternalIntegration.ODILO)
        self.register(BibliothecaAPI, canonical=ExternalIntegration.BIBLIOTHECA)
        self.register(Axis360API, canonical=ExternalIntegration.AXIS_360)
        self.register(EnkiAPI, canonical=EnkiAPI.ENKI_EXTERNAL)
        self.register(OPDSForDistributorsAPI, canonical=OPDSForDistributorsAPI.NAME)
        self.register(ODLAPI, canonical=ODLAPI.NAME)
        self.register(ODL2API, canonical=ODL2API.NAME)


class OpenAccessLicenseProvidersRegistry(IntegrationRegistry["OPDSImporter"]):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)
        from core.opds2_import import OPDS2Importer
        from core.opds_import import OPDSImporter

        self.register(OPDSImporter, canonical=OPDSImporter.NAME)
        self.register(OPDS2Importer, canonical=OPDS2Importer.NAME)
