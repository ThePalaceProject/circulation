from __future__ import annotations

from palace.manager.feed.worklist.database import DatabaseBackedWorkList
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.work import Work


class SpecificWorkList(DatabaseBackedWorkList):
    """A WorkList that only finds specific works, identified by ID."""

    def __init__(self, work_ids):
        super().__init__()
        self.work_ids = work_ids

    def modify_database_query_hook(self, _db, qu):
        qu = qu.filter(
            Work.id.in_(self.work_ids),
            LicensePool.work_id.in_(self.work_ids),  # Query optimization
        )
        return qu
