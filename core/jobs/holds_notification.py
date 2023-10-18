from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import or_

from core.config import Configuration, ConfigurationConstants
from core.model import Base
from core.model.configuration import ConfigurationSetting
from core.model.patron import Hold
from core.monitor import SweepMonitor
from core.util.datetime_helpers import utc_now
from core.util.notifications import PushNotifications

if TYPE_CHECKING:
    from sqlalchemy.orm import Query

    from core.model.collection import Collection


class HoldsNotificationMonitor(SweepMonitor):
    """Sweep across all holds that are ready to be checked out by the user (position=0)"""

    MODEL_CLASS: type[Base] | None = Hold
    SERVICE_NAME: str | None = "Holds Notification"

    def run_once(self, *ignore):
        setting = ConfigurationSetting.sitewide(
            self._db, Configuration.PUSH_NOTIFICATIONS_STATUS
        )
        if setting.value == ConfigurationConstants.FALSE:
            self.log.info(
                "Push notifications have been turned off in the sitewide settings, skipping this job"
            )
            return
        return super().run_once(*ignore)

    def scope_to_collection(self, qu: Query, collection: Collection) -> Query:
        """Do not scope to collection"""
        return qu

    def item_query(self) -> Query:
        query = super().item_query()
        query = query.filter(
            Hold.position == 0,
            or_(
                Hold.patron_last_notified != utc_now().date(),
                Hold.patron_last_notified == None,
            ),
        )
        return query

    def process_items(self, items: list[Hold]) -> None:
        PushNotifications.send_holds_notifications(items)
