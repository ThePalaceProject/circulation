from sqlalchemy.orm.session import Session

from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.util.log import LoggerMixin


class LocalAnalyticsProvider(LoggerMixin):
    def collect_event(
        self,
        library,
        license_pool,
        event_type,
        time,
        old_value=None,
        new_value=None,
        patron=None,
        **kwargs
    ):
        if not library and not license_pool:
            raise ValueError("Either library or license_pool must be provided.")
        if library:
            _db = Session.object_session(library)
        else:
            _db = Session.object_session(license_pool)

        return CirculationEvent.log(
            _db,
            license_pool,
            event_type,
            old_value,
            new_value,
            start=time,
            library=library,
        )
