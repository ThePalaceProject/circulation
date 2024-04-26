import os
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from unittest.mock import PropertyMock, patch

import pytest
from celery import Celery
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
    return CeleryConfiguration.construct(broker_url="memory://")  # type: ignore[arg-type]


@pytest.fixture(scope="session")
def celery_config(celery_pydantic_config: CeleryConfiguration) -> Mapping[str, Any]:
    """Configure the test Celery app.

    The config returned will then be used to configure the `celery_app` fixture.
    """
    return celery_pydantic_config.dict() | task_queue_config()


@pytest.fixture(scope="session")
def celery_parameters() -> Mapping[str, Any]:
    """Change the init parameters of test Celery app.

    The dict returned will be used as parameters when instantiating `~celery.Celery`.
    """
    return {"task_cls": "palace.manager.celery.task:Task"}


@pytest.fixture(scope="session")
def celery_includes() -> Sequence[str]:
    """Include modules when a worker starts."""
    return ("palace.manager.celery.app",)


@dataclass
class CeleryFixture:
    container: CeleryContainer
    app: Celery
    config: CeleryConfiguration
    worker: WorkController
    session_maker: MockSessionMaker


@pytest.fixture()
def celery_fixture(
    services_fixture: ServicesFixture,
    mock_session_maker: MockSessionMaker,
    celery_app: Celery,
    celery_worker: WorkController,
    celery_pydantic_config: CeleryConfiguration,
) -> Generator[CeleryFixture, None, None]:
    """Fixture to provide a Celery app and worker for testing."""

    # Make sure our services container has the correct celery app setup
    container = services_fixture.celery_fixture.celery_container
    container.config.from_dict(celery_pydantic_config.dict())
    container.app.override(celery_app)

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
            celery_app,
            celery_pydantic_config,
            celery_worker,
            mock_session_maker,
        )
