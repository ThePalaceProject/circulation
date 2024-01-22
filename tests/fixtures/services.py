from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from unittest.mock import MagicMock, create_autospec

import boto3
import pytest

from core.analytics import Analytics
from core.external_search import ExternalSearchIndex
from core.search.revision_directory import SearchRevisionDirectory
from core.search.service import SearchServiceOpensearch1
from core.service.analytics.container import AnalyticsContainer
from core.service.container import Services, wire_container
from core.service.logging.container import Logging
from core.service.logging.log import setup_logging
from core.service.search.container import Search
from core.service.storage.container import Storage
from core.service.storage.s3 import S3Service


@contextmanager
def mock_services_container(
    services_container: Services,
) -> Generator[None, None, None]:
    from core.service import container

    container._container_instance = services_container
    yield
    container._container_instance = None


@dataclass
class ServicesLoggingFixture:
    logging_container: Logging
    logging_mock: MagicMock


@pytest.fixture
def services_logging_fixture() -> ServicesLoggingFixture:
    logging_container = Logging()
    logging_mock = create_autospec(setup_logging)
    logging_container.logging.override(logging_mock)
    return ServicesLoggingFixture(logging_container, logging_mock)


@dataclass
class ServicesStorageFixture:
    storage_container: Storage
    s3_client_mock: MagicMock
    analytics_mock: MagicMock
    public_mock: MagicMock


@pytest.fixture
def services_storage_fixture() -> ServicesStorageFixture:
    storage_container = Storage()
    s3_client_mock = create_autospec(boto3.client)
    analytics_mock = create_autospec(S3Service.factory)
    public_mock = create_autospec(S3Service.factory)
    storage_container.s3_client.override(s3_client_mock)
    storage_container.analytics.override(analytics_mock)
    storage_container.public.override(public_mock)
    return ServicesStorageFixture(
        storage_container, s3_client_mock, analytics_mock, public_mock
    )


@dataclass
class ServicesSearchFixture:
    search_container: Search
    client_mock: MagicMock
    service_mock: MagicMock
    revision_directory_mock: MagicMock
    index_mock: MagicMock


@pytest.fixture
def services_search_fixture() -> ServicesSearchFixture:
    search_container = Search()
    client_mock = create_autospec(boto3.client)
    service_mock = create_autospec(SearchServiceOpensearch1)
    revision_directory_mock = create_autospec(SearchRevisionDirectory.create)
    index_mock = create_autospec(ExternalSearchIndex)
    search_container.client.override(client_mock)
    search_container.service.override(service_mock)
    search_container.revision_directory.override(revision_directory_mock)
    search_container.index.override(index_mock)
    return ServicesSearchFixture(
        search_container, client_mock, service_mock, revision_directory_mock, index_mock
    )


@dataclass
class ServicesAnalyticsFixture:
    analytics_container: AnalyticsContainer
    analytics_mock: MagicMock


@pytest.fixture
def services_analytics_fixture() -> ServicesAnalyticsFixture:
    analytics_container = AnalyticsContainer()
    analytics_mock = create_autospec(Analytics)
    analytics_container.analytics.override(analytics_mock)
    return ServicesAnalyticsFixture(analytics_container, analytics_mock)


class ServicesFixture:
    """
    Provide a real services container, with all services mocked out.
    """

    def __init__(
        self,
        logging: ServicesLoggingFixture,
        storage: ServicesStorageFixture,
        search: ServicesSearchFixture,
        analytics: ServicesAnalyticsFixture,
    ) -> None:
        self.logging_fixture = logging
        self.storage_fixture = storage
        self.search_fixture = search
        self.analytics_fixture = analytics

        self.services = Services()
        self.services.logging.override(logging.logging_container)
        self.services.storage.override(storage.storage_container)
        self.services.search.override(search.search_container)
        self.services.analytics.override(analytics.analytics_container)


@pytest.fixture(autouse=True)
def services_fixture(
    services_logging_fixture: ServicesLoggingFixture,
    services_storage_fixture: ServicesStorageFixture,
    services_search_fixture: ServicesSearchFixture,
    services_analytics_fixture: ServicesAnalyticsFixture,
) -> Generator[ServicesFixture, None, None]:
    fixture = ServicesFixture(
        logging=services_logging_fixture,
        storage=services_storage_fixture,
        search=services_search_fixture,
        analytics=services_analytics_fixture,
    )
    with mock_services_container(fixture.services):
        yield fixture


@pytest.fixture
def services_fixture_wired(
    services_fixture: ServicesFixture,
) -> Generator[ServicesFixture, None, None]:
    wire_container(services_fixture.services)
    yield services_fixture
    services_fixture.services.unwire()
