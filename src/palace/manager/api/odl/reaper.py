from __future__ import annotations

from typing import Any

from sqlalchemy.orm import Session

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.core.metadata_layer import TimestampData
from palace.manager.core.monitor import CollectionMonitor
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Hold
from palace.manager.util.datetime_helpers import utc_now


class OPDS2WithODLHoldReaper(CollectionMonitor):
    """Check for holds that have expired and delete them, and update
    the holds queues for their pools."""

    SERVICE_NAME = "ODL2 Hold Reaper"
    PROTOCOL = OPDS2WithODLApi.label()

    def __init__(
        self,
        _db: Session,
        collection: Collection,
        api: OPDS2WithODLApi | None = None,
        **kwargs: Any,
    ):
        super().__init__(_db, collection, **kwargs)
        self.api = api or OPDS2WithODLApi(_db, collection)

    def run_once(self, progress: TimestampData) -> TimestampData:
        # Find holds that have expired.
        expired_holds = (
            self._db.query(Hold)
            .join(Hold.license_pool)
            .filter(LicensePool.collection_id == self.api.collection_id)
            .filter(Hold.end < utc_now())
            .filter(Hold.position == 0)
        )

        changed_pools = set()
        total_deleted_holds = 0
        for hold in expired_holds:
            changed_pools.add(hold.license_pool)
            self._db.delete(hold)
            # log circulation event:  hold expired
            total_deleted_holds += 1

        for pool in changed_pools:
            self.api.update_licensepool(pool)

        message = "Holds deleted: %d. License pools updated: %d" % (
            total_deleted_holds,
            len(changed_pools),
        )
        progress = TimestampData(achievements=message)
        return progress
