from __future__ import annotations

import random
import string
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.service.analytics.eventdata import AnalyticsEventData
from palace.manager.service.analytics.provider import AnalyticsProvider
from palace.manager.sqlalchemy.constants import MediaTypes

if TYPE_CHECKING:
    from palace.manager.service.storage.s3 import S3Service


class S3AnalyticsProvider(AnalyticsProvider):
    """Analytics provider storing data in a S3 bucket."""

    def __init__(self, s3_service: S3Service | None):
        self.s3_service = s3_service

    def collect(
        self,
        event: AnalyticsEventData,
        session: Session | None = None,
    ) -> None:
        content = event.model_dump_json()

        storage = self._get_storage()
        analytics_file_key = self._get_file_key(event)

        storage.store(
            analytics_file_key,
            content,
            MediaTypes.APPLICATION_JSON_MEDIA_TYPE,
        )

    def _get_file_key(
        self,
        event: AnalyticsEventData,
    ) -> str:
        """The path to the analytics data file."""
        root = event.library_short_name
        time_part = str(event.start)

        # ensure the uniqueness of file name (in case of overlapping events)
        collection = event.collection_id if event.collection_id else "NONE"
        random_string = "".join(random.choices(string.ascii_lowercase, k=10))
        file_name = "-".join([time_part, event.type, str(collection), random_string])

        # nest file in directories that allow for easy purging by year, month or day
        return "/".join(
            [
                str(root),
                str(event.start.year),
                str(event.start.month),
                str(event.start.day),
                file_name + ".json",
            ]
        )

    def _get_storage(self) -> S3Service:
        """Return the CMs configured storage service.
        Raises an exception if the storage service is not configured.

        :return: StorageServiceBase object
        """
        if self.s3_service is None:
            raise CannotLoadConfiguration(
                "No storage service is configured with an analytics bucket."
            )

        return self.s3_service
