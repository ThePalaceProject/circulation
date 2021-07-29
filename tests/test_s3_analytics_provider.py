import datetime
import json

import pytest
from api.s3_analytics_provider import S3AnalyticsProvider
from mock import MagicMock, create_autospec

from core.classifier import Classifier
from core.config import CannotLoadConfiguration
from core.mirror import MirrorUploader
from core.model import (
    CirculationEvent,
    DataSource,
    ExternalIntegration,
    ExternalIntegrationLink,
    MediaTypes,
    create,
)
from core.s3 import S3Uploader, S3UploaderConfiguration
from core.testing import DatabaseTest


class TestS3AnalyticsProvider(DatabaseTest):
    @staticmethod
    def timestamp_to_string(timestamp):
        """Return a string representation of a datetime object.

        :param timestamp: datetime object storing a timestamp
        :type timestamp: datetime.datetime

        :return: String representation of the timestamp
        :rtype: str
        """
        return timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")

    def setup_method(self):
        super(TestS3AnalyticsProvider, self).setup_method()

        self._analytics_integration, _ = create(
            self._db,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=S3AnalyticsProvider.__module__,
        )
        self._analytics_provider = S3AnalyticsProvider(
            self._analytics_integration, self._default_library
        )

    def test_exception_is_raised_when_there_is_no_external_integration_link(self):
        # Act, Assert
        with pytest.raises(CannotLoadConfiguration):
            self._analytics_provider.collect_event(
                self._default_library,
                None,
                CirculationEvent.NEW_PATRON,
                datetime.datetime.utcnow(),
            )

    def test_exception_is_raised_when_there_is_no_storage_integration(self):
        # Arrange
        # Create an external integration link but don't create a storage integration
        create(
            self._db,
            ExternalIntegrationLink,
            external_integration_id=self._analytics_integration.id,
            purpose=ExternalIntegrationLink.ANALYTICS,
        )

        # Act, Assert
        with pytest.raises(CannotLoadConfiguration):
            self._analytics_provider.collect_event(
                self._default_library,
                None,
                CirculationEvent.NEW_PATRON,
                datetime.datetime.utcnow(),
            )

    def test_exception_is_raised_when_there_is_no_analytics_bucket(self):
        # Arrange
        # Create a storage service
        storage_integration, _ = create(
            self._db,
            ExternalIntegration,
            goal=ExternalIntegration.STORAGE_GOAL,
            protocol=ExternalIntegration.S3,
        )

        # Create an external integration link to the storage service
        create(
            self._db,
            ExternalIntegrationLink,
            external_integration_id=self._analytics_integration.id,
            other_integration_id=storage_integration.id,
            purpose=ExternalIntegrationLink.ANALYTICS,
        )

        # Act, Assert
        with pytest.raises(CannotLoadConfiguration):
            self._analytics_provider.collect_event(
                self._default_library,
                None,
                CirculationEvent.NEW_PATRON,
                datetime.datetime.utcnow(),
            )

    def test_analytics_data_without_associated_license_pool_is_correctly_stored_in_s3(
        self,
    ):
        # Arrange
        # Create an S3 Analytics integration
        analytics_integration, _ = create(
            self._db,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=S3AnalyticsProvider.__module__,
        )
        # Create an S3 Analytics provider
        provider = S3AnalyticsProvider(analytics_integration, self._default_library)

        # Create an S3 storage service
        storage_integration, _ = create(
            self._db,
            ExternalIntegration,
            goal=ExternalIntegration.STORAGE_GOAL,
            protocol=ExternalIntegration.S3,
        )
        # Set up a bucket name used for storing analytics data
        storage_integration.setting(
            S3UploaderConfiguration.ANALYTICS_BUCKET_KEY
        ).value = "analytics"

        # Create a link to the S3 storage service
        create(
            self._db,
            ExternalIntegrationLink,
            external_integration_id=analytics_integration.id,
            other_integration_id=storage_integration.id,
            purpose=ExternalIntegrationLink.ANALYTICS,
        )

        # Set up a mock instead of real S3Uploader class acting as the S3 storage service
        s3_uploader = create_autospec(spec=S3Uploader)
        MirrorUploader.implementation = MagicMock(return_value=s3_uploader)

        # Set up event's metadata
        event_time = datetime.datetime.utcnow()
        event_time_formatted = self.timestamp_to_string(event_time)
        event_type = CirculationEvent.NEW_PATRON

        # Act
        provider.collect_event(self._default_library, None, event_type, event_time)

        # Assert
        s3_uploader.analytics_file_url.assert_called_once_with(
            self._default_library, None, event_type, event_time
        )
        s3_uploader.mirror_one.assert_called_once()
        representation, _ = s3_uploader.mirror_one.call_args[0]

        assert MediaTypes.JSON_MEDIA_TYPE == representation.media_type

        content = representation.content
        event = json.loads(content)

        assert event_type == event["type"]
        assert event_time_formatted == event["start"]
        assert event_time_formatted == event["end"]
        assert self._default_library.id == event["library_id"]

    def test_analytics_data_with_associated_license_pool_is_correctly_stored_in_s3(
        self,
    ):
        # Arrange
        # Create an S3 Analytics integration
        analytics_integration, _ = create(
            self._db,
            ExternalIntegration,
            goal=ExternalIntegration.ANALYTICS_GOAL,
            protocol=S3AnalyticsProvider.__module__,
        )
        # Create an S3 Analytics provider
        provider = S3AnalyticsProvider(analytics_integration, self._default_library)

        # Create an S3 storage service
        storage_integration, _ = create(
            self._db,
            ExternalIntegration,
            goal=ExternalIntegration.STORAGE_GOAL,
            protocol=ExternalIntegration.S3,
        )
        # Set up a bucket name used for storing analytics data
        storage_integration.setting(
            S3UploaderConfiguration.ANALYTICS_BUCKET_KEY
        ).value = "analytics"

        # Create a link to the S3 storage service
        create(
            self._db,
            ExternalIntegrationLink,
            external_integration_id=analytics_integration.id,
            other_integration_id=storage_integration.id,
            purpose=ExternalIntegrationLink.ANALYTICS,
        )

        # Set up a mock instead of real S3Uploader class acting as the S3 storage service
        s3_uploader = create_autospec(spec=S3Uploader)
        MirrorUploader.implementation = MagicMock(return_value=s3_uploader)

        # Create a test book
        work = self._work(
            data_source_name=DataSource.GUTENBERG,
            title="Test Book",
            authors=("Test Author 1", "Test Author 2"),
            genre="Test Genre",
            language="eng",
            audience=Classifier.AUDIENCE_ADULT,
            with_license_pool=True,
        )
        license_pool = work.license_pools[0]
        edition = work.presentation_edition

        # Set up event's metadata
        event_time = datetime.datetime.utcnow()
        event_time_formatted = self.timestamp_to_string(event_time)
        event_type = CirculationEvent.CM_CHECKOUT

        # Act
        provider.collect_event(
            self._default_library, license_pool, event_type, event_time
        )

        # Assert
        s3_uploader.analytics_file_url.assert_called_once_with(
            self._default_library, license_pool, event_type, event_time
        )
        s3_uploader.mirror_one.assert_called_once()
        representation, _ = s3_uploader.mirror_one.call_args[0]

        assert MediaTypes.JSON_MEDIA_TYPE == representation.media_type

        content = representation.content
        event = json.loads(content)
        data_source = license_pool.data_source if license_pool else None
        identifier = license_pool.identifier if license_pool else None
        collection = license_pool.collection if license_pool else None
        work = license_pool.work if license_pool else None

        assert event_type == event["type"]
        assert event_time_formatted == event["start"]
        assert event_time_formatted == event["end"]
        assert self._default_library.id == event["library_id"]
        assert license_pool.id == event["license_pool_id"]
        assert edition.publisher == event["publisher"]
        assert edition.imprint == event["imprint"]
        assert edition.issued == event["issued"]
        assert edition.published == event["published"]
        assert edition.medium == event["medium"]
        assert collection.name == event["collection"]
        assert identifier.type == event["identifier_type"]
        assert identifier.identifier == event["identifier"]
        assert data_source.name == event["data_source"]
        assert work.audience == event["audience"]
        assert work.fiction == event["fiction"]
        assert work.summary_text == event["summary_text"]
        assert work.quality == event["quality"]
        assert work.rating == event["rating"]
        assert work.popularity == event["popularity"]
        assert work.genres[0].name == event["genre"]
        assert (
            self.timestamp_to_string(license_pool.availability_time)
            == event["availability_time"]
        )
        assert license_pool.licenses_owned == event["licenses_owned"]
        assert license_pool.licenses_available == event["licenses_available"]
        assert license_pool.licenses_reserved == event["licenses_reserved"]
        assert license_pool.patrons_in_hold_queue == event["patrons_in_hold_queue"]
        assert license_pool.self_hosted == event["self_hosted"]
        assert work.title == event["title"]
        assert work.series == event["series"]
        assert work.series_position == event["series_position"]
        assert work.language == event["language"]
