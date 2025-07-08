from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from palace.manager.integration.goals import Goals
from palace.manager.service.integration_registry.base import IntegrationRegistry

if TYPE_CHECKING:
    from palace.manager.circulation.base import CirculationApiType
    from palace.manager.sqlalchemy.model.collection import Collection


class LicenseProvidersRegistry(IntegrationRegistry["CirculationApiType"]):
    def __init__(self) -> None:
        super().__init__(Goals.LICENSE_GOAL)

        from palace.manager.api.bibliotheca import BibliothecaAPI
        from palace.manager.api.boundless.api import BoundlessApi
        from palace.manager.api.enki import EnkiAPI
        from palace.manager.api.odl.api import OPDS2WithODLApi
        from palace.manager.api.opds_for_distributors import OPDSForDistributorsAPI
        from palace.manager.api.overdrive.api import OverdriveAPI
        from palace.manager.core.opds2_import import OPDS2API
        from palace.manager.core.opds_import import OPDSAPI

        self.register(OverdriveAPI, canonical=OverdriveAPI.label())
        self.register(BibliothecaAPI, canonical=BibliothecaAPI.label())
        self.register(BoundlessApi, canonical=BoundlessApi.label())
        self.register(EnkiAPI, canonical=EnkiAPI.label())
        self.register(OPDSForDistributorsAPI, canonical=OPDSForDistributorsAPI.label())
        self.register(OPDS2WithODLApi, canonical=OPDS2WithODLApi.label())
        self.register(OPDSAPI, canonical=OPDSAPI.label())
        self.register(OPDS2API, canonical=OPDS2API.label())

    def from_collection(
        self, db: Session, collection: Collection
    ) -> CirculationApiType:
        impl_cls = self[collection.protocol]
        return impl_cls(db, collection)
