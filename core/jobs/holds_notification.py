from typing import Optional, Type

from core.model import Base
from core.model.patron import Hold
from core.monitor import SweepMonitor
from core.util.notifications import PushNotifications


class HoldsNotificationMonitor(SweepMonitor):
    """Sweep across all holds that are ready to be checked out by the user (position=0)"""

    MODEL_CLASS: Optional[Type[Base]] = Hold
    SERVICE_NAME: Optional[str] = "Holds Notification"

    def scope_to_collection(self, qu, collection):
        """Do not scope to collection"""
        return qu

    def item_query(self):
        query = super().item_query()
        query = query.filter(Hold.position == 0)
        return query

    def process_items(self, items):
        PushNotifications.send_holds_notifications(items)
