from datetime import datetime
from typing import Any

from sqlalchemy.orm.session import Session

from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.log import LoggerMixin


class LocalAnalyticsProvider(LoggerMixin):
    def collect_event(
        self,
        library: Library,
        license_pool: LicensePool | None,
        event_type: str,
        time: datetime,
        old_value: Any = None,
        new_value: Any = None,
        user_agent: str | None = None,
        patron: Patron | None = None,
        **kwargs
    ):
        _db = Session.object_session(library)

        return CirculationEvent.log(
            _db,
            license_pool,
            event_type,
            old_value,
            new_value,
            start=time,
            library=library,
        )
