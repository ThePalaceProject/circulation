from unittest.mock import MagicMock

import pytest

from core.service.container import Services
from core.service.storage.container import Storage
from core.service.storage.s3 import S3Service


class MockStorageFixture:
    def __init__(self):
        self.storage = Storage()
        self.analytics = MagicMock(spec=S3Service)
        self.storage.analytics.override(self.analytics)
        self.public = MagicMock(spec=S3Service)
        self.storage.public.override(self.public)
        self.s3_client = MagicMock()
        self.storage.s3_client.override(self.s3_client)


@pytest.fixture
def mock_storage_fixture() -> MockStorageFixture:
    return MockStorageFixture()


class MockServicesFixture:
    """
    Provide a services container with all the services mocked out
    by MagicMock objects.
    """

    def __init__(self, storage: MockStorageFixture):
        self.services = Services()
        self.services.storage.override(storage.storage)
        self.storage = storage


@pytest.fixture
def mock_services_fixture(
    mock_storage_fixture: MockStorageFixture,
) -> MockServicesFixture:
    return MockServicesFixture(mock_storage_fixture)
