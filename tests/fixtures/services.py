from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, Mock, create_autospec

import boto3
import pytest
from opensearchpy import OpenSearch

from palace.manager.search.external_search import ExternalSearchIndex
from palace.manager.search.revision_directory import SearchRevisionDirectory
from palace.manager.search.service import SearchServiceOpensearch1
from palace.manager.service.analytics.analytics import Analytics
from palace.manager.service.container import Services, wire_container
from palace.manager.service.email.configuration import EmailConfiguration
from palace.manager.service.logging.log import setup_logging
from palace.manager.service.sitewide import SitewideConfiguration
from palace.manager.service.storage.s3 import S3Service


@contextmanager
def mock_services_container(
    services_container: Services,
) -> Generator[None]:
    from palace.manager.service import container

    container._container_instance = services_container
    try:
        yield
    finally:
        container._container_instance = None


class MockServicesFixture:
    def __init__(self) -> None:
        self.logging = create_autospec(setup_logging)

        self.s3_client = create_autospec(boto3.client)
        self.s3_analytics = create_autospec(S3Service.factory)
        self.s3_public = create_autospec(S3Service.factory)

        self.search_client = create_autospec(OpenSearch)
        self.search_service = create_autospec(SearchServiceOpensearch1)
        self.search_revision_directory = create_autospec(SearchRevisionDirectory)
        self.search_index = create_autospec(ExternalSearchIndex)

        self.analytics = create_autospec(Analytics)

        self.emailer_sender = "test@email.com"
        self.emailer = MagicMock()

        self.celery_app = MagicMock()

    def reset_mocks(self) -> None:
        for item in self.__dict__.values():
            if isinstance(item, Mock):
                item.reset_mock(return_value=True, side_effect=True)


@pytest.fixture(scope="session")
def mock_services_session_fixture() -> MockServicesFixture:
    """
    Fixture to provide mock services for testing. This fixture is scoped to the session
    so that we only have the overhead of creating the mock services via autospec once
    per test session, which can be expensive.

    Note: This fixture shouldn't be used directly, but rather through the `mock_services_fixture`
    which resets the mocks after each test.
    """
    return MockServicesFixture()


@pytest.fixture(scope="function")
def mock_services_fixture(
    mock_services_session_fixture: MockServicesFixture,
) -> Generator[MockServicesFixture]:
    """
    Fixture to provide mock services for testing.
    """
    yield mock_services_session_fixture
    mock_services_session_fixture.reset_mocks()


class ServicesFixture:
    """
    Provide a real services container, with all services mocked out.
    """

    def __init__(
        self,
        mock_services: MockServicesFixture,
    ) -> None:
        self.mock_services = mock_services

        self.services = Services()

        # Mock out logging
        logging_container = self.services.logging()
        logging_container.logging.override(mock_services.logging)

        # Mock out storage
        storage_container = self.services.storage()
        storage_container.s3_client.override(mock_services.s3_client)
        storage_container.analytics.override(mock_services.s3_analytics)
        storage_container.public.override(mock_services.s3_public)

        # Mock out search
        search_container = self.services.search()
        search_container.client.override(mock_services.search_client)
        search_container.service.override(mock_services.search_service)
        search_container.revision_directory.override(
            mock_services.search_revision_directory
        )
        search_container.index.override(mock_services.search_index)

        # Mock out analytics
        analytics_container = self.services.analytics()
        analytics_container.analytics.override(mock_services.analytics)

        # Mock out email
        email_container = self.services.email()
        email_container.config.from_dict(
            EmailConfiguration(sender=mock_services.emailer_sender).model_dump()
        )
        email_container.emailer.override(mock_services.emailer)

        # Mock out celery
        celery_container = self.services.celery()
        celery_container.app.override(mock_services.celery_app)

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
    def wired(self) -> Generator[None]:
        wire_container(self.services)
        try:
            yield
        finally:
            self.services.unwire()


@pytest.fixture
def services_fixture(
    mock_services_fixture: MockServicesFixture,
) -> Generator[ServicesFixture]:
    fixture = ServicesFixture(
        mock_services=mock_services_fixture,
    )
    with mock_services_container(fixture.services):
        yield fixture


@pytest.fixture
def services_fixture_wired(
    services_fixture: ServicesFixture,
) -> Generator[ServicesFixture]:
    with services_fixture.wired():
        yield services_fixture
