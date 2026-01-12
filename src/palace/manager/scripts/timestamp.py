from __future__ import annotations

from datetime import datetime
from typing import Any

from palace.manager.core.monitor import TimestampData
from palace.manager.scripts.base import Script
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.coverage import Timestamp


class TimestampScript(Script):
    """A script that automatically records a timestamp whenever it runs."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.timestamp_collection: Collection | None = None

    def update_timestamp(
        self,
        timestamp_data: TimestampData | None,
        start: datetime,
        exception: str | None,
    ) -> None:
        """Update the appropriate Timestamp for this script.

        :param timestamp_data: A TimestampData representing what the script
          itself thinks its timestamp should look like. Data will be filled in
          where it is missing, but it will not be modified if present.

        :param start: The time at which this script believes the
          service started running. The script itself may change this
          value for its own purposes.

        :param exception: The exception with which this script
          believes the service stopped running. The script itself may
          change this value for its own purposes.
        """
        if timestamp_data is None:
            timestamp_data = TimestampData()
        timestamp_data.finalize(
            self.script_name,
            Timestamp.SCRIPT_TYPE,
            self.timestamp_collection,
            start=start,
            exception=exception,
        )
        timestamp_data.apply(self._db)
