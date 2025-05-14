import os
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from celery import Celery
from celery.app import autoretry
from celery.worker import WorkController

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import task_queue_config
from palace.manager.service.celery.configuration import CeleryConfiguration
from palace.manager.service.celery.container import CeleryContainer
from tests.fixtures.database import MockSessionMaker
from tests.fixtures.services import ServicesFixture


@pytest.fixture(scope="session")
def celery_worker_parameters() -> Mapping[str, Any]:
    """
    Change the init parameters of Celery workers.

    Normally when testing, we want to make sure that if there is an issue with the task
    the worker will shut down after a certain amount of time. We default this to 30 sec.
    However, when debugging it can be useful to set this to None, so you can set breakpoints
    in the worker code, without the worker timing out and shutting down.
    """
    timeout = os.environ.get(
        "PALACE_TEST_CELERY_WORKER_SHUTDOWN_TIMEOUT", "30.0"
    ).lower()
    shutdown_timeout = None if timeout == "none" or timeout == "" else float(timeout)
    return {"shutdown_timeout": shutdown_timeout}


@pytest.fixture(scope="session")
def celery_pydantic_config() -> CeleryConfiguration:
    """Configure the test Celery app.

    The config returned will then be used to configure the `celery_app` fixture.
    """
    return CeleryConfiguration.model_construct(
        broker_url="memory://",
        result_backend="cache+memory://",
    )


@pytest.fixture(scope="session")
def celery_config(celery_pydantic_config: CeleryConfiguration) -> Mapping[str, Any]:
    """Configure the test Celery app.

    The config returned will then be used to configure the `celery_app` fixture.
    """
    return celery_pydantic_config.model_dump() | task_queue_config()


@pytest.fixture(scope="session")
def celery_parameters() -> Mapping[str, Any]:
    """Change the init parameters of test Celery app.

    The dict returned will be used as parameters when instantiating `~celery.Celery`.
    """
    return {"task_cls": "palace.manager.celery.task:Task"}


@pytest.fixture(scope="session")
def celery_includes() -> Sequence[str]:
    """Include modules when a worker starts."""
    return ("palace.manager.celery.tasks",)


@dataclass
class CeleryRetriesMock:
    mock: MagicMock

    @property
    def retry_count(self) -> int:
        """Return the number of times the task has been retried."""
        call_args = self.mock.call_args
        if call_args is None:
            return 0

        return call_args.kwargs.get("retries", 0)


@dataclass
class CeleryFixture:
    container: CeleryContainer
    app: Celery
    config: CeleryConfiguration
    worker: WorkController
    session_maker: MockSessionMaker

    @contextmanager
    def patch_retry_backoff(self) -> Generator[CeleryRetriesMock, None, None]:
        """
        Patch the retry backoff to always return 0, so we don't have to wait for
        a retry to happen within our tests.

        Returns a CeleryRetriesMock object that can be used to check how many times
        the task has been retried.
        """
        with patch.object(
            autoretry, "get_exponential_backoff_interval", return_value=0
        ) as mock:
            yield CeleryRetriesMock(mock=mock)


@pytest.fixture()
def celery_fixture(
    services_fixture: ServicesFixture,
    mock_session_maker: MockSessionMaker,
    celery_session_app: Celery,
    celery_session_worker: WorkController,
    celery_pydantic_config: CeleryConfiguration,
) -> Generator[CeleryFixture, None, None]:
    """Fixture to provide a Celery app and worker for testing."""

    # Make sure our services container has the correct celery app setup
    container = services_fixture.services.celery()
    container.config.from_dict(celery_pydantic_config.model_dump())
    container.app.override(celery_session_app)

    # Make sure that the app created by the container is set as current and default
    celery_session_app.set_default()
    celery_session_app.set_current()

    # We mock out the session maker, so it doesn't try to create a new session,
    # instead it should use the same session as the test transaction.
    with (
        patch.object(Task, "_session_maker", mock_session_maker),
        patch.object(
            Task, "services", PropertyMock(return_value=services_fixture.services)
        ),
    ):
        yield CeleryFixture(
            container,
            celery_session_app,
            celery_pydantic_config,
            celery_session_worker,
            mock_session_maker,
        )
