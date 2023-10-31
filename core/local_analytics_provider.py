from flask_babel import lazy_gettext as _
from sqlalchemy.orm.session import Session

from core.model import CirculationEvent
from core.util.log import LoggerMixin


class LocalAnalyticsProvider(LoggerMixin):
    NAME = _("Local Analytics")

    DESCRIPTION = _("Store analytics events in the 'circulationevents' database table.")

    # A given site can only have one analytics provider.
    CARDINALITY = 1

    def collect_event(
        self,
        library,
        license_pool,
        event_type,
        time,
        old_value=None,
        new_value=None,
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


# The Analytics class looks for the name "Provider".
Provider = LocalAnalyticsProvider
