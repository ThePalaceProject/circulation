from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, create_autospec

import boto3
import pytest
from celery import Celery

from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.revision_directory import SearchRevisionDirectory
from palace.manager.search.service import SearchServiceOpensearch1
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.analytics.container import AnalyticsContainer
from palace.manager.service.celery.container import CeleryContainer
from palace.manager.service.container import Services, wire_container
from palace.manager.service.email.configuration import EmailConfiguration
from palace.manager.service.email.container import Email
from palace.manager.service.logging.container import Logging
from palace.manager.service.logging.log import setup_logging
from palace.manager.service.search.container import Search
from palace.manager.service.sitewide import SitewideConfiguration
from palace.manager.service.storage.container import Storage
from palace.manager.service.storage.s3 import S3Service


@contextmanager
def mock_services_container(
    services_container: Services,
) -> Generator[None, None, None]:
    from palace.manager.service import container

    container._container_instance = services_container
    try:
        yield
    finally:
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
    revision_directory_mock = create_autospec(SearchRevisionDirectory)
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


@dataclass
class ServicesEmailFixture:
    email_container: Email
    mock_emailer: MagicMock
    sender_email: str


@pytest.fixture
def services_email_fixture() -> ServicesEmailFixture:
    email_container = Email()
    sender_email = "test@email.com"
    email_container.config.from_dict(
        EmailConfiguration(sender=sender_email).model_dump()
    )
    mock_emailer = MagicMock()
    email_container.emailer.override(mock_emailer)
    return ServicesEmailFixture(email_container, mock_emailer, sender_email)


@dataclass
class ServicesCeleryFixture:
    celery_container: CeleryContainer
    app: Celery


@pytest.fixture
def services_celery_fixture() -> ServicesCeleryFixture:
    celery_container = CeleryContainer()
    celery_mock_app = MagicMock()
    celery_container.app.override(celery_mock_app)
    return ServicesCeleryFixture(celery_container, celery_mock_app)


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
        email: ServicesEmailFixture,
        celery: ServicesCeleryFixture,
    ) -> None:
        self.logging_fixture = logging
        self.storage_fixture = storage
        self.search_fixture = search
        self.analytics_fixture = analytics
        self.email_fixture = email
        self.celery_fixture = celery

        self.services = Services()
        self.services.logging.override(logging.logging_container)
        self.services.storage.override(storage.storage_container)
        self.services.search.override(search.search_container)
        self.services.analytics.override(analytics.analytics_container)
        self.services.email.override(email.email_container)
        self.services.celery.override(celery.celery_container)

        # setup basic configuration from default settings
        self.services.config.from_dict(
            {"sitewide": SitewideConfiguration().model_dump()}
        )

    def build_config_mapping(self, path: list[str], value: Any) -> dict[str, Any]:
        path_segment = path.pop()
        if not path:
            return {path_segment: value}
        else:
            return {path_segment: self.build_config_mapping(path, value)}

    def set_config_option(self, key: str, value: Any) -> None:
        path = key.split(".")
        path.reverse()
        self.services.config.from_dict(self.build_config_mapping(path, value))

    def set_sitewide_config_option(self, key: str, value: Any) -> None:
        self.set_config_option(f"sitewide.{key}", value)

    def set_base_url(self, base_url: str | None) -> None:
        self.set_sitewide_config_option("base_url", base_url)

    @contextmanager
    def wired(self) -> Generator[None, None, None]:
        wire_container(self.services)
        try:
            yield
        finally:
            self.services.unwire()


@pytest.fixture
def services_fixture(
    services_logging_fixture: ServicesLoggingFixture,
    services_storage_fixture: ServicesStorageFixture,
    services_search_fixture: ServicesSearchFixture,
    services_analytics_fixture: ServicesAnalyticsFixture,
    services_email_fixture: ServicesEmailFixture,
    services_celery_fixture: ServicesCeleryFixture,
) -> Generator[ServicesFixture, None, None]:
    fixture = ServicesFixture(
        logging=services_logging_fixture,
        storage=services_storage_fixture,
        search=services_search_fixture,
        analytics=services_analytics_fixture,
        email=services_email_fixture,
        celery=services_celery_fixture,
    )
    with mock_services_container(fixture.services):
        yield fixture


@pytest.fixture
def services_fixture_wired(
    services_fixture: ServicesFixture,
) -> Generator[ServicesFixture, None, None]:
    with services_fixture.wired():
        yield services_fixture
