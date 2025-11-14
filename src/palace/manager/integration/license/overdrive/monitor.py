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


class OverdriveFormatSweep(IdentifierSweepMonitor):
    """Check the current formats of every Overdrive book
    in our collection.
    """

    SERVICE_NAME = "Overdrive Format Sweep"
    DEFAULT_BATCH_SIZE = 10
    PROTOCOL = OverdriveAPI.label()

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api_class: type[OverdriveAPI] = OverdriveAPI,
    ) -> None:
        super().__init__(_db, collection)
        self.api = api_class(_db, collection)

    def process_item(self, identifier: Identifier) -> None:
        pools = identifier.licensed_through
        for pool in pools:
            self.api.update_formats(pool)
            # if there are multiple pools they should all have the same formats
            # so we break after processing the first one
            break
