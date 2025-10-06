from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.base import IntegrationRegistry

if TYPE_CHECKING:
    from palace.manager.api.circulation.base import CirculationApiType
    from palace.manager.sqlalchemy.model.collection import Collection


class LicenseProvidersRegistry(IntegrationRegistry["CirculationApiType"]):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)

        from palace.manager.integration.license.bibliotheca import BibliothecaAPI
        from palace.manager.integration.license.boundless.api import BoundlessApi
        from palace.manager.integration.license.opds.for_distributors.api import (
            OPDSForDistributorsAPI,
        )
        from palace.manager.integration.license.opds.odl.api import OPDS2WithODLApi
        from palace.manager.integration.license.opds.opds1.api import OPDSAPI
        from palace.manager.integration.license.opds.opds2.api import OPDS2API
        from palace.manager.integration.license.overdrive.api import OverdriveAPI

        self.register(OverdriveAPI, canonical=OverdriveAPI.label())
        self.register(BibliothecaAPI, canonical=BibliothecaAPI.label())
        self.register(BoundlessApi, canonical=BoundlessApi.label())
        self.register(OPDSForDistributorsAPI, canonical=OPDSForDistributorsAPI.label())
        self.register(OPDS2WithODLApi, canonical=OPDS2WithODLApi.label())
        self.register(OPDSAPI, canonical=OPDSAPI.label())
        self.register(OPDS2API, canonical=OPDS2API.label())

    def from_collection(
        self, db: Session, collection: Collection
    ) -> CirculationApiType:
        impl_cls = self[collection.protocol]
        return impl_cls(db, collection)
