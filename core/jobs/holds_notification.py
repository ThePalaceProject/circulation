from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import or_

from core.model import Base
from core.model.patron import Hold
from core.monitor import SweepMonitor
from core.util.datetime_helpers import utc_now
from core.util.notifications import PushNotifications

if TYPE_CHECKING:
    from sqlalchemy.orm import Query, Session

    from core.model.collection import Collection


class HoldsNotificationMonitor(SweepMonitor):
    """Sweep across all holds that are ready to be checked out by the user (position=0)"""

    def __init__(
        self,
        _db: Session,
        collection: Collection | None = None,
        batch_size: int | None = None,
        notifications: PushNotifications | None = None,
    ) -> None:
        super().__init__(_db, collection, batch_size)
        self.notifications = notifications or PushNotifications(
            self.services.config.sitewide.base_url(),
            self.services.fcm.app(),
        )

    MODEL_CLASS: type[Base] | None = Hold
    SERVICE_NAME: str | None = "Holds Notification"

    def scope_to_collection(self, qu: Query, collection: Collection) -> Query:
        """Do not scope to collection"""
        return qu

    def item_query(self) -> Query:
        now = utc_now()
        query = super().item_query()
        query = query.filter(
            Hold.position == 0,
            Hold.end > now,
            or_(
                Hold.patron_last_notified != now.date(),
                Hold.patron_last_notified == None,
            ),
        )
        return query

    def process_items(self, items: list[Hold]) -> None:
        self.notifications.send_holds_notifications(items)
