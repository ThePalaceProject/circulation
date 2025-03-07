from abc import ABC, abstractmethod

from sqlalchemy.orm import Session

from palace.manager.service.analytics.eventdata import AnalyticsEventData


class AnalyticsProvider(ABC):
    @abstractmethod
    def collect(
        self,
        event: AnalyticsEventData,
        session: Session | None = None,
    ) -> None:
        """
        Write the event to the appropriate data store.
        """
        ...
