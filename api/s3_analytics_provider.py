import datetime
import json
from typing import Any, Dict, List

from flask_babel import lazy_gettext as _
from sqlalchemy.orm import Session

from core.config import CannotLoadConfiguration
from core.local_analytics_provider import LocalAnalyticsProvider
from core.mirror import MirrorUploader
from core.model import (
    ExternalIntegration,
    Library,
    LicensePool,
    MediaTypes,
    Representation,
    get_one,
)
from core.model.configuration import (
    ConfigurationAttributeType,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
    ExternalIntegrationLink,
)
from core.s3 import S3Uploader, S3UploaderConfiguration


class S3AnalyticsProviderConfiguration(ConfigurationGrouping):
    """Contains configuration settings of the S3 Analytics provider."""

    NO_MIRROR_INTEGRATION = "NO_MIRROR"

    DEFAULT_MIRROR_OPTION = ConfigurationOption(NO_MIRROR_INTEGRATION, "None")

    analytics_mirror = ConfigurationMetadata(
        key="mirror_integration_id",
        label=_("Analytics Mirror"),
        description=_(
            "S3-compatible service to use for storing analytics events. "
            "The service must already be configured under 'Storage Services'."
        ),
        type=ConfigurationAttributeType.SELECT,
        required=True,
        default=NO_MIRROR_INTEGRATION,
        options=[DEFAULT_MIRROR_OPTION],
    )


class S3AnalyticsProvider(LocalAnalyticsProvider):
    """Analytics provider storing data in a S3 bucket."""

    NAME = _("S3 Analytics")
    DESCRIPTION = _("Store analytics events in a S3 bucket.")

    SETTINGS = (
        LocalAnalyticsProvider.SETTINGS + S3AnalyticsProviderConfiguration.to_settings()
    )

    @staticmethod
    def _create_event_object(
        library: Library,
        license_pool: LicensePool,
        event_type: str,
        time: datetime.datetime,
        old_value,
        new_value,
        neighborhood: str,
    ) -> Dict:
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
        **kwargs
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
        if library:
            _db = Session.object_session(library)
        else:
            _db = Session.object_session(license_pool)
        if library and self.library_id and library.id != self.library_id:
            return

        neighborhood = None
        if self.location_source == self.LOCATION_SOURCE_NEIGHBORHOOD:
            neighborhood = kwargs.pop("neighborhood", None)

        event = self._create_event_object(
            library, license_pool, event_type, time, old_value, new_value, neighborhood
        )
        content = json.dumps(
            event,
            default=str,
            ensure_ascii=True,
        )
        s3_uploader: S3Uploader = self._get_s3_uploader(_db)
        analytics_file_url = s3_uploader.analytics_file_url(
            library, license_pool, event_type, time
        )

        # Create a temporary Representation object because S3Uploader can work only with Representation objects.
        # NOTE: It won't be stored in the database.
        representation = Representation(
            media_type=MediaTypes.APPLICATION_JSON_MEDIA_TYPE, content=content
        )
        s3_uploader.mirror_one(representation, analytics_file_url)

    def _get_s3_uploader(self, db: Session) -> S3Uploader:
        """Get an S3Uploader object associated with the provider's selected storage service.

        :param db: Database session

        :return: S3Uploader object associated with the provider's selected storage service
        """
        # To find the storage integration for the exporter, first find the
        # external integration link associated with the provider's external
        # integration.
        integration_link = get_one(
            db,
            ExternalIntegrationLink,
            external_integration_id=self.integration_id,
            purpose=ExternalIntegrationLink.ANALYTICS,
        )

        if not integration_link:
            raise CannotLoadConfiguration(
                "The provider doesn't have an associated storage service"
            )

        # Then use the "other" integration value to find the storage integration.
        storage_integration = get_one(
            db, ExternalIntegration, id=integration_link.other_integration_id
        )

        if not storage_integration:
            raise CannotLoadConfiguration(
                "The provider doesn't have an associated storage service"
            )

        analytics_bucket = storage_integration.setting(
            S3UploaderConfiguration.ANALYTICS_BUCKET_KEY
        ).value

        if not analytics_bucket:
            raise CannotLoadConfiguration(
                "The associated storage service does not have {} bucket".format(
                    S3UploaderConfiguration.ANALYTICS_BUCKET_KEY
                )
            )

        s3_uploader = MirrorUploader.implementation(storage_integration)

        return s3_uploader

    @classmethod
    def get_storage_settings(cls, db: Session) -> List[Dict[str, Any]]:
        """Return the provider's configuration settings including available storage options.

        :param db: Database session

        :return: List containing the provider's configuration settings
        """
        storage_integrations = ExternalIntegration.for_goal(
            db, ExternalIntegration.STORAGE_GOAL
        )

        for storage_integration in storage_integrations:
            configuration_settings = [
                setting
                for setting in storage_integration.settings
                if setting.key == S3UploaderConfiguration.ANALYTICS_BUCKET_KEY
            ]

            if configuration_settings:
                if configuration_settings[0].value:
                    S3AnalyticsProviderConfiguration.analytics_mirror.options.append(
                        ConfigurationOption(
                            storage_integration.id, storage_integration.name
                        )
                    )

        cls.SETTINGS = S3AnalyticsProviderConfiguration.to_settings()

        return cls.SETTINGS


Provider = S3AnalyticsProvider
