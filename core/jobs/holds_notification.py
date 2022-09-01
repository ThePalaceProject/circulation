from __future__ import annotations

from typing import TYPE_CHECKING

from core.model import Base
from core.model.patron import Hold
from core.monitor import SweepMonitor
from core.util.notifications import PushNotifications

if TYPE_CHECKING:
    from sqlalchemy.orm import Query

    from core.model.collection import Collection


class HoldsNotificationMonitor(SweepMonitor):
    """Sweep across all holds that are ready to be checked out by the user (position=0)"""

    MODEL_CLASS: type[Base] | None = Hold
    SERVICE_NAME: str | None = "Holds Notification"

    def scope_to_collection(self, qu: Query, collection: Collection) -> Query:
        """Do not scope to collection"""
        return qu

    def item_query(self) -> Query:
        query = super().item_query()
        query = query.filter(Hold.position == 0)
        return query

    def process_items(self, items: list[Hold]) -> None:
        PushNotifications.send_holds_notifications(items)
