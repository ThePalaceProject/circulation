from __future__ import annotations

import datetime
import json
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, create_autospec

import pytest

from api.s3_analytics_provider import S3AnalyticsProvider
from core.classifier import Classifier
from core.config import CannotLoadConfiguration
from core.model import CirculationEvent, DataSource, MediaTypes
from core.service.storage.s3 import S3Service

if TYPE_CHECKING:
    from tests.fixtures.database import DatabaseTransactionFixture


class S3AnalyticsFixture:
    def __init__(self, db: DatabaseTransactionFixture) -> None:
        self.db = db

        self.analytics_storage = create_autospec(S3Service)
        self.analytics_provider = S3AnalyticsProvider(
            self.analytics_storage,
        )


@pytest.fixture(scope="function")
def s3_analytics_fixture(db: DatabaseTransactionFixture):
    return S3AnalyticsFixture(db)


class TestS3AnalyticsProvider:
    @staticmethod
    def timestamp_to_string(timestamp):
        """Return a string representation of a datetime object.

        :param timestamp: datetime object storing a timestamp
        :type timestamp: datetime.datetime

        :return: String representation of the timestamp
        :rtype: str
        """
        return str(timestamp)

    def test_exception_is_raised_when_no_analytics_bucket_configured(
        self, s3_analytics_fixture: S3AnalyticsFixture
    ):
        # The services container returns None when there is no analytics storage service configured
        provider = S3AnalyticsProvider(None)

        # Act, Assert
        with pytest.raises(CannotLoadConfiguration):
            provider.collect_event(
                s3_analytics_fixture.db.default_library(),
                None,
                CirculationEvent.NEW_PATRON,
                datetime.datetime.utcnow(),
            )

    def test_analytics_data_without_associated_license_pool_is_correctly_stored_in_s3(
        self, s3_analytics_fixture: S3AnalyticsFixture
    ):
        # Set up event's metadata
        event_time = datetime.datetime.utcnow()
        event_time_formatted = self.timestamp_to_string(event_time)
        event_type = CirculationEvent.NEW_PATRON

        s3_analytics_fixture.analytics_provider._get_file_key = MagicMock()

        # Act
        s3_analytics_fixture.analytics_provider.collect_event(
            s3_analytics_fixture.db.default_library(), None, event_type, event_time
        )

        # Assert
        s3_analytics_fixture.analytics_provider._get_file_key.assert_called_once_with(
            s3_analytics_fixture.db.default_library(), None, event_type, event_time
        )
        s3_analytics_fixture.analytics_storage.store.assert_called_once()
        (
            key,
            content,
            content_type,
        ) = s3_analytics_fixture.analytics_storage.store.call_args.args

        assert content_type == MediaTypes.APPLICATION_JSON_MEDIA_TYPE
        assert key == s3_analytics_fixture.analytics_provider._get_file_key.return_value
        event = json.loads(content)

        assert event["type"] == event_type
        assert event["start"] == event_time_formatted
        assert event["end"] == event_time_formatted
        assert event["library_id"] == s3_analytics_fixture.db.default_library().id

    def test_analytics_data_with_associated_license_pool_is_correctly_stored_in_s3(
        self, s3_analytics_fixture: S3AnalyticsFixture
    ):
        # Create a test book
        work = s3_analytics_fixture.db.work(
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

        s3_analytics_fixture.analytics_provider._get_file_key = MagicMock()

        # Act
        s3_analytics_fixture.analytics_provider.collect_event(
            s3_analytics_fixture.db.default_library(),
            license_pool,
            event_type,
            event_time,
        )

        # Assert
        s3_analytics_fixture.analytics_storage.store.assert_called_once()
        (
            key,
            content,
            content_type,
        ) = s3_analytics_fixture.analytics_storage.store.call_args.args

        assert content_type == MediaTypes.APPLICATION_JSON_MEDIA_TYPE
        assert key == s3_analytics_fixture.analytics_provider._get_file_key.return_value

        event = json.loads(content)
        data_source = license_pool.data_source if license_pool else None
        identifier = license_pool.identifier if license_pool else None
        collection = license_pool.collection if license_pool else None
        work = license_pool.work if license_pool else None

        assert event["type"] == event_type
        assert event["start"] == event_time_formatted
        assert event["end"] == event_time_formatted
        assert event["library_id"] == s3_analytics_fixture.db.default_library().id
        assert event["license_pool_id"] == license_pool.id
        assert event["publisher"] == edition.publisher
        assert event["imprint"] == edition.imprint
        assert event["issued"] == edition.issued
        assert event["published"] == edition.published
        assert event["medium"] == edition.medium
        assert event["collection"] == collection.name
        assert event["identifier_type"] == identifier.type
        assert event["identifier"] == identifier.identifier
        assert event["data_source"] == data_source.name
        assert event["audience"] == work.audience
        assert event["fiction"] == work.fiction
        assert event["summary_text"] == work.summary_text
        assert event["quality"] == work.quality
        assert event["rating"] == work.rating
        assert event["popularity"] == work.popularity
        assert event["genre"] == work.genres[0].name
        assert event["availability_time"] == self.timestamp_to_string(
            license_pool.availability_time
        )
        assert event["licenses_owned"] == license_pool.licenses_owned
        assert event["licenses_available"] == license_pool.licenses_available
        assert event["licenses_reserved"] == license_pool.licenses_reserved
        assert event["patrons_in_hold_queue"] == license_pool.patrons_in_hold_queue
        assert event["self_hosted"] is False
        assert event["title"] == work.title
        assert event["series"] == work.series
        assert event["series_position"] == work.series_position
        assert event["language"] == work.language
