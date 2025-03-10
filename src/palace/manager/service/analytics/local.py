from sqlalchemy.orm.session import Session

from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.analytics.provider import AnalyticsProvider
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.util import get_one_or_create
from palace.manager.util.log import LoggerMixin


class LocalAnalyticsProvider(AnalyticsProvider, LoggerMixin):
    def collect(
        self,
        event: AnalyticsEventData,
        session: Session | None = None,
    ) -> None:
        if session is None:
            self.log.error("No session provided unable to collect event")
            return

        """Log a CirculationEvent to the database, assuming it
         hasn't already been recorded.
         """
        circ_event, was_new = get_one_or_create(
            session,
            CirculationEvent,
            license_pool_id=event.license_pool_id,
            type=event.type,
            start=event.start,
            library_id=event.library_id,
            create_method_kwargs=dict(
                old_value=event.old_value,
                new_value=event.new_value,
                delta=event.delta,
                end=event.end,
                location=event.location,
            ),
        )
        if was_new:
            self.log.info(f"EVENT {event.type} {event.old_value}=>{event.new_value}")
