from __future__ import annotations

from sqlalchemy.orm import Session

from palace.manager.core.monitor import (
    IdentifierSweepMonitor,
)
from palace.manager.integration.license.overdrive.api import OverdriveAPI
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.identifier import Identifier


class OverdriveCollectionReaper(IdentifierSweepMonitor):
    """Check for books that are in the local collection but have left our
    Overdrive collection.
    """

    SERVICE_NAME = "Overdrive Collection Reaper"
    PROTOCOL = OverdriveAPI.label()
    DEFAULT_BATCH_SIZE = 10

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api_class: type[OverdriveAPI] = OverdriveAPI,
    ) -> None:
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier: Identifier) -> None:
        self.api.update_licensepool(identifier.identifier)
