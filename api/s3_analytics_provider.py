from __future__ import annotations

import datetime
import json
import random
import string
from typing import TYPE_CHECKING

from core.config import CannotLoadConfiguration
from core.local_analytics_provider import LocalAnalyticsProvider
from core.model import Library, LicensePool, MediaTypes

if TYPE_CHECKING:
    from core.service.storage.s3 import S3Service


class S3AnalyticsProvider(LocalAnalyticsProvider):
    """Analytics provider storing data in a S3 bucket."""

    def __init__(self, s3_service: S3Service | None):
        self.s3_service = s3_service

    @staticmethod
    def _create_event_object(
        library: Library,
        license_pool: LicensePool,
        event_type: str,
        time: datetime.datetime,
        old_value,
        new_value,
        neighborhood: str | None = None,
    ) -> dict:
        """Create a Python dict containing required information about the event.

        :param library: Library associated with the event

        :param license_pool: License pool associated with the event

        :param event_type: Type of the event

        :param time: Event's timestamp

        :param old_value: Old value of the metric changed by the event

        :param new_value: New value of the metric changed by the event

        :param neighborhood: Geographic location of the event

        :return: Python dict containing required information about the event
        """
        start = time
        if not start:
            start = datetime.datetime.utcnow()
        end = start

        if new_value is None or old_value is None:
            delta = None
        else:
            delta = new_value - old_value

        data_source = license_pool.data_source if license_pool else None
        identifier = license_pool.identifier if license_pool else None
        collection = license_pool.collection if license_pool else None
        work = license_pool.work if license_pool else None
        edition = work.presentation_edition if work else None
        if not edition and license_pool:
            edition = license_pool.presentation_edition

        event = {
            "type": event_type,
            "start": start,
            "end": end,
            "library_id": library.id,
            "library_name": library.name,
            "library_short_name": library.short_name,
            "old_value": old_value,
            "new_value": new_value,
            "delta": delta,
            "location": neighborhood,
            "license_pool_id": license_pool.id if license_pool else None,
            "publisher": edition.publisher if edition else None,
            "imprint": edition.imprint if edition else None,
            "issued": edition.issued if edition else None,
            "published": datetime.datetime.combine(
                edition.published, datetime.datetime.min.time()
            )
            if edition and edition.published
            else None,
            "medium": edition.medium if edition else None,
            "collection": collection.name if collection else None,
            "identifier_type": identifier.type if identifier else None,
            "identifier": identifier.identifier if identifier else None,
            "data_source": data_source.name if data_source else None,
            "distributor": data_source.name if data_source else None,
            "audience": work.audience if work else None,
            "fiction": work.fiction if work else None,
            "summary_text": work.summary_text if work else None,
            "quality": work.quality if work else None,
            "rating": work.rating if work else None,
            "popularity": work.popularity if work else None,
            "genre": ", ".join(map(lambda genre: genre.name, work.genres))
            if work
            else None,
            "availability_time": license_pool.availability_time
            if license_pool
            else None,
            "licenses_owned": license_pool.licenses_owned if license_pool else None,
            "licenses_available": license_pool.licenses_available
            if license_pool
            else None,
            "licenses_reserved": license_pool.licenses_reserved
            if license_pool
            else None,
            "patrons_in_hold_queue": license_pool.patrons_in_hold_queue
            if license_pool
            else None,
            # TODO: We no longer support self-hosted books, so this should always be False.
            #  this value is still included in the response for backwards compatibility,
            #  but should be removed in a future release.
            "self_hosted": False,
            "title": work.title if work else None,
            "author": work.author if work else None,
            "series": work.series if work else None,
            "series_position": work.series_position if work else None,
            "language": work.language if work else None,
            "open_access": license_pool.open_access if license_pool else None,
        }

        return event

    def collect_event(
        self,
        library,
        license_pool,
        event_type,
        time,
        old_value=None,
        new_value=None,
        **kwargs,
    ):
        """Log the event using the appropriate for the specific provider's mechanism.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param library: Library associated with the event
        :type library: core.model.library.Library

        :param license_pool: License pool associated with the event
        :type license_pool: core.model.licensing.LicensePool

        :param event_type: Type of the event
        :type event_type: str

        :param time: Event's timestamp
        :type time: datetime.datetime

        :param neighborhood: Geographic location of the event
        :type neighborhood: str

        :param old_value: Old value of the metric changed by the event
        :type old_value: Any

        :param new_value: New value of the metric changed by the event
        :type new_value: Any
        """

        if not library and not license_pool:
            raise ValueError("Either library or license_pool must be provided.")

        event = self._create_event_object(
            library, license_pool, event_type, time, old_value, new_value
        )
        content = json.dumps(
            event,
            default=str,
            ensure_ascii=True,
        )

        storage = self._get_storage()
        analytics_file_key = self._get_file_key(library, license_pool, event_type, time)

        storage.store(
            analytics_file_key,
            content,
            MediaTypes.APPLICATION_JSON_MEDIA_TYPE,
        )

    def _get_file_key(
        self,
        library: Library,
        license_pool: LicensePool | None,
        event_type: str,
        end_time: datetime.datetime,
        start_time: datetime.datetime | None = None,
    ):
        """The path to the analytics data file for the given library, license
        pool and date range."""
        root = library.short_name
        if start_time:
            time_part = str(start_time) + "-" + str(end_time)
        else:
            time_part = str(end_time)

        # ensure the uniqueness of file name (in case of overlapping events)
        collection = license_pool.collection_id if license_pool else "NONE"
        random_string = "".join(random.choices(string.ascii_lowercase, k=10))
        file_name = "-".join([time_part, event_type, str(collection), random_string])
        # nest file in directories that allow for easy purging by year, month or day
        return "/".join(
            [
                str(root),
                str(end_time.year),
                str(end_time.month),
                str(end_time.day),
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
