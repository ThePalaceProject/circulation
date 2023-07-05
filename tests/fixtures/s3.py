import functools
import os
from typing import Any, Iterable
from urllib.parse import urlsplit

import boto3
import pytest

from core.model import ExternalIntegration
from core.s3 import S3Uploader, S3UploaderConfiguration
from tests.fixtures.database import DatabaseTransactionFixture


class S3UploaderFixture:
    transaction: DatabaseTransactionFixture

    def __init__(self, transaction: DatabaseTransactionFixture):
        self.transaction = transaction

    def integration(self, **settings):
        """Create and configure a simple S3 integration."""
        integration = self.transaction.external_integration(
            ExternalIntegration.S3, ExternalIntegration.STORAGE_GOAL, settings=settings
        )
        integration.username = settings.get("username", "username")
        integration.password = settings.get("password", "password")
        return integration

    @staticmethod
    def add_settings_value(settings, key, value):
        """Adds a value to settings dictionary

        :param settings: Settings dictionary
        :type settings: Dict

        :param key: Key
        :type key: string

        :param value: Value
        :type value: Any

        :return: Updated settings dictionary
        :rtype: Dict
        """
        if value:
            if settings:
                settings[key] = value

            else:
                settings = {key: value}

        return settings

    def create_s3_uploader(
        self,
        client_class=None,
        uploader_class=None,
        region=None,
        addressing_style=None,
        **settings,
    ):
        """Creates a new instance of S3 uploader

        :param client_class: (Optional) Custom class to be used instead of boto3's client class
        :type client_class: Optional[Type]

        :param: uploader_class: (Optional) Custom class which will be used insted of S3Uploader
        :type uploader_class: Optional[Type]

        :param region: (Optional) S3 region
        :type region: Optional[string]

        :param addressing_style: (Optional) S3 addressing style
        :type addressing_style: Optional[string]

        :param settings: Kwargs used for initializing an external integration
        :type: Optional[Dict]

        :return: New intance of S3 uploader
        :rtype: S3Uploader
        """
        settings = self.add_settings_value(
            settings, S3UploaderConfiguration.S3_REGION, region
        )
        settings = self.add_settings_value(
            settings, S3UploaderConfiguration.S3_ADDRESSING_STYLE, addressing_style
        )
        integration = self.integration(**settings)
        uploader_class = uploader_class or S3Uploader

        return uploader_class(integration, client_class=client_class)


@pytest.fixture
def s3_uploader_fixture(
    db,
) -> S3UploaderFixture:
    return S3UploaderFixture(db)


class S3UploaderIntegrationFixture(S3UploaderFixture):
    SIMPLIFIED_TEST_MINIO_ENDPOINT_URL = os.environ.get(
        "SIMPLIFIED_TEST_MINIO_ENDPOINT_URL", "http://localhost:9000"
    )
    SIMPLIFIED_TEST_MINIO_USER = os.environ.get(
        "SIMPLIFIED_TEST_MINIO_USER", "minioadmin"
    )
    SIMPLIFIED_TEST_MINIO_PASSWORD = os.environ.get(
        "SIMPLIFIED_TEST_MINIO_PASSWORD", "minioadmin"
    )
    _, SIMPLIFIED_TEST_MINIO_HOST, _, _, _ = urlsplit(
        SIMPLIFIED_TEST_MINIO_ENDPOINT_URL
    )

    minio_s3_client: Any
    """boto3 client connected to locally running MinIO instance"""

    s3_client_class = None
    """Factory function used for creating a boto3 client inside S3Uploader"""

    def __init__(self, transaction: DatabaseTransactionFixture):
        super().__init__(transaction)
        self.minio_s3_client = boto3.client(
            "s3",
            aws_access_key_id=S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_USER,
            aws_secret_access_key=S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_PASSWORD,
            endpoint_url=S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_ENDPOINT_URL,
        )
        self.s3_client_class = functools.partial(
            boto3.client,
            endpoint_url=S3UploaderIntegrationFixture.SIMPLIFIED_TEST_MINIO_ENDPOINT_URL,
        )

    def close(self):
        response = self.minio_s3_client.list_buckets()

        for bucket in response["Buckets"]:
            bucket_name = bucket["Name"]
            response = self.minio_s3_client.list_objects(Bucket=bucket_name)

            for object in response.get("Contents", []):
                object_key = object["Key"]
                self.minio_s3_client.delete_object(Bucket=bucket_name, Key=object_key)

            self.minio_s3_client.delete_bucket(Bucket=bucket_name)

    def create_s3_uploader(
        self,
        client_class=None,
        uploader_class=None,
        region=None,
        addressing_style=None,
        **settings,
    ):
        """Creates a new instance of S3 uploader

        :param client_class: (Optional) Custom class to be used instead of boto3's client class
        :type client_class: Optional[Type]

        :param: uploader_class: (Optional) Custom class which will be used insted of S3Uploader
        :type uploader_class: Optional[Type]

        :param region: (Optional) S3 region
        :type region: Optional[string]

        :param addressing_style: (Optional) S3 addressing style
        :type addressing_style: Optional[string]

        :param settings: Kwargs used for initializing an external integration
        :type: Optional[Dict]

        :return: New intance of S3 uploader
        :rtype: S3Uploader
        """
        if settings and "username" not in settings:
            self.add_settings_value(
                settings, "username", self.SIMPLIFIED_TEST_MINIO_USER
            )
        if settings and "password" not in settings:
            self.add_settings_value(
                settings, "password", self.SIMPLIFIED_TEST_MINIO_PASSWORD
            )
        if not client_class:
            client_class = self.s3_client_class

        return super().create_s3_uploader(
            client_class, uploader_class, region, addressing_style, **settings
        )


@pytest.fixture
def s3_uploader_integration_fixture(
    db,
) -> Iterable[S3UploaderIntegrationFixture]:
    fixture = S3UploaderIntegrationFixture(db)
    yield fixture
    fixture.close()
