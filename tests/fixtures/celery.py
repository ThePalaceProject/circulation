from __future__ import annotations

import os
from collections.abc import Generator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Self
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from celery import Celery
from celery.app import autoretry
from celery.worker import WorkController
from sqlalchemy import select
from sqlalchemy.orm import Session

from palace.manager.celery.task import Task
from palace.manager.celery.tasks import apply
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.data_layer.policy.replacement import ReplacementPolicy
from palace.manager.service.celery.celery import task_queue_config
from palace.manager.service.celery.configuration import CeleryConfiguration
from palace.manager.service.celery.container import CeleryContainer
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanismTuple,
    LicensePool,
)
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one
from tests.fixtures.database import DatabaseTransactionFixture, MockSessionMaker
from tests.fixtures.http import MockHttpClientFixture
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
        """
        Return the number of times the task has been retried.

        If the task hits its max retries, this number will be one more than the
        number of times the task was actually retried, since the last call to
        the mock happens before the task fails permanently.
        """
        return self.mock.call_count


@dataclass
class CeleryFixture:
    container: CeleryContainer
    app: Celery
    config: CeleryConfiguration
    worker: WorkController
    session_maker: MockSessionMaker

    @contextmanager
    def patch_retry_backoff(self) -> Generator[CeleryRetriesMock]:
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
) -> Generator[CeleryFixture]:
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


@dataclass(frozen=True)
class ApplyBibliographicCall:
    bibliographic: BibliographicData
    edition_id: int | None = None
    collection_id: int | None = None
    replace: ReplacementPolicy | None = None

    def mock_apply(self, db: Session) -> None:
        """Apply the bibliographic data to the database."""
        if self.edition_id is None:
            edition, _ = self.bibliographic.edition(db)
        else:
            edition_or_none = get_one(db, Edition, id=self.edition_id)
            if edition_or_none is None:
                raise ValueError(f"Edition with id {self.edition_id} not found")
            edition = edition_or_none

        if self.collection_id is not None:
            collection = Collection.by_id(db, self.collection_id)
            if collection is None:
                raise ValueError(f"Collection with id {self.collection_id} not found")
        else:
            collection = None

        self.bibliographic.apply(
            db,
            edition,
            collection,
            replace=self.replace,
            disable_async_calculation=True,
            create_coverage_record=False,
        )


@dataclass(frozen=True)
class ApplyCirculationCall:
    circulation: CirculationData
    collection_id: int
    replace: ReplacementPolicy | None = None

    def mock_apply(self, db: Session) -> None:
        """Apply the circulation data to the database."""
        collection = Collection.by_id(db, self.collection_id)
        if collection is None:
            raise ValueError(f"Collection with id {self.collection_id} not found")

        self.circulation.apply(
            db,
            collection,
            replace=self.replace,
        )


class ApplyTaskFixture:
    """
    A test fixture that helps with testing tasks that enqueue celery
    bibliographic_apply and circulation_apply tasks.

    Often in tests we don't want to actually enqueue these tasks and have
    them run asynchronously. Instead, we want to be able to test the full
    workflow, assuming that the task we are testing, and all the apply tasks
    run to completion.
    """

    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        mock_bibliographic_apply: MagicMock,
        mock_circulation_apply: MagicMock,
    ) -> None:
        self._db = db
        self.client = http_client
        self.apply_queue: list[ApplyBibliographicCall | ApplyCirculationCall] = []
        self.mock_bibliographic = mock_bibliographic_apply
        self.mock_circulation = mock_circulation_apply

        # Setup the mocks
        self.mock_bibliographic.delay.side_effect = self._mock_bibliographic_apply
        self.mock_circulation.delay.side_effect = self._mock_circulation_apply

    @classmethod
    @contextmanager
    def fixture(
        cls,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
    ) -> Generator[Self]:
        with (
            patch.object(apply, "bibliographic_apply") as mock_bibliographic_apply,
            patch.object(apply, "circulation_apply") as mock_circulation_apply,
        ):
            yield cls(db, http_client, mock_bibliographic_apply, mock_circulation_apply)

    def _mock_bibliographic_apply(
        self,
        bibliographic: BibliographicData,
        edition_id: int | None = None,
        collection_id: int | None = None,
        replace: ReplacementPolicy | None = None,
    ) -> None:
        """
        Mock bibliographic apply

        This function mocks the apply.bibliographic_apply task, to avoid this
        task being executed asynchronously. We want to be able to test the full
        workflow, assuming that the task we are testing, and all the apply tasks
        run to completion.
        """
        self.apply_queue.append(
            ApplyBibliographicCall(bibliographic, edition_id, collection_id, replace)
        )

    def _mock_circulation_apply(
        self,
        circulation: CirculationData,
        collection_id: int,
        replace: ReplacementPolicy | None = None,
    ) -> None:
        """
        Mock circulation apply

        This function mocks the apply.circulation_apply task, to avoid this
        task being executed asynchronously. We want to be able to test the full
        workflow, assuming that the task we are testing, and all the apply tasks
        run to completion.
        """
        self.apply_queue.append(
            ApplyCirculationCall(circulation, collection_id, replace)
        )

    def process_apply_queue(self) -> None:
        """
        Process the mocked apply queue.

        This function does the same basic logic as the apply tasks.
        Since we test those separately, we can assume that they works correctly.
        """
        for call in self.apply_queue:
            call.mock_apply(self._db.session)
        self.apply_queue.clear()

    def get_editions(self) -> list[Edition]:
        """Get all editions from the database."""
        return self._db.session.scalars(select(Edition).order_by(Edition.id)).all()

    def get_pools(self) -> list[LicensePool]:
        """Get all license pools from the database."""
        return (
            self._db.session.scalars(select(LicensePool).order_by(LicensePool.id))
            .unique()
            .all()
        )

    def get_works(self) -> list[Work]:
        """Get all works from the database."""
        return self._db.session.scalars(select(Work).order_by(Work.id)).unique().all()

    @staticmethod
    def get_delivery_mechanisms_from_license_pool(
        license_pool: LicensePool,
    ) -> set[DeliveryMechanismTuple]:
        """
        Get a set of DeliveryMechanismTuples from a LicensePool.

        Makes it a little easier to compare delivery mechanisms
        """
        return {
            dm.delivery_mechanism.as_tuple for dm in license_pool.delivery_mechanisms
        }

    @staticmethod
    def get_edition_by_identifier(
        editions: list[Edition], identifier: str
    ) -> Edition | None:
        """
        Find an edition in the list by its identifier.
        """
        for edition in editions:
            if edition.primary_identifier.urn == identifier:
                return edition

        return None

    @staticmethod
    def get_license_pool_by_identifier(
        pools: list[LicensePool], identifier: str
    ) -> LicensePool | None:
        """
        Find a license pool in the list by its identifier.
        """
        for pool in pools:
            if pool.identifier.urn == identifier:
                return pool

        return None

    @staticmethod
    def get_work_by_identifier(works: list[Work], identifier: str) -> Work | None:
        """Find a license pool in the list by its identifier."""
        for work in works:
            if work.presentation_edition.primary_identifier.urn == identifier:
                return work

        return None


@pytest.fixture
def apply_task_fixture(
    db: DatabaseTransactionFixture,
    http_client: MockHttpClientFixture,
) -> Generator[ApplyTaskFixture]:
    """
    A test fixture that helps with testing tasks that enqueue celery
    bibliographic_apply and circulation_apply tasks.
    """
    with ApplyTaskFixture.fixture(db, http_client) as fixture:
        yield fixture
