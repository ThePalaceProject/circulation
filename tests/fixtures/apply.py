from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from typing_extensions import Self

from palace.manager.celery.tasks import apply
from palace.manager.data_layer.bibliographic import BibliographicData
from palace.manager.data_layer.circulation import CirculationData
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.edition import Edition
from palace.manager.sqlalchemy.model.licensing import (
    DeliveryMechanismTuple,
    LicensePool,
)
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.http import MockHttpClientFixture


class ApplyTaskFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        http_client: MockHttpClientFixture,
        mock_bibliographic_apply: MagicMock,
        mock_circulation_apply: MagicMock,
    ) -> None:
        self._db = db
        self.client = http_client
        self.apply_queue: list[
            tuple[Collection | None, BibliographicData | CirculationData]
        ] = []
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

    def _apply_bibliographic(
        self, data: BibliographicData, collection: Collection | None
    ) -> None:
        edition, _ = data.edition(self._db.session)
        data.apply(
            self._db.session,
            edition,
            collection,
            disable_async_calculation=True,
            create_coverage_record=False,
        )

    def _apply_circulation(
        self, data: CirculationData, collection: Collection | None
    ) -> None:
        data.apply(self._db.session, collection)

    def _mock_bibliographic_apply(
        self,
        bibliographic: BibliographicData,
        collection_id: int | None = None,
    ) -> None:
        """
        Mock bibliographic apply

        This function mocks the apply.bibliographic_apply task, to avoid this
        task being executed asynchronously. We want to be able to test the full
        workflow, assuming that the task we are testing, and all the apply tasks
        run to completion.
        """
        collection = (
            None
            if collection_id is None
            else Collection.by_id(self._db.session, collection_id)
        )
        self.apply_queue.append((collection, bibliographic))

    def _mock_circulation_apply(
        self,
        circulation: CirculationData,
        collection_id: int | None = None,
    ) -> None:
        """
        Mock circulation apply

        This function mocks the apply.circulation_apply task, to avoid this
        task being executed asynchronously. We want to be able to test the full
        workflow, assuming that the task we are testing, and all the apply tasks
        run to completion.
        """
        collection = (
            None
            if collection_id is None
            else Collection.by_id(self._db.session, collection_id)
        )
        self.apply_queue.append((collection, circulation))

    def process_apply_queue(self) -> None:
        """
        Process the mocked apply queue.

        This function does the same basic logic as the apply tasks.
        Since we test those separately, we can assume that they works correctly.
        """
        for collection, data in self.apply_queue:
            if isinstance(data, CirculationData):
                self._apply_circulation(data, collection)
            elif isinstance(data, BibliographicData):
                self._apply_bibliographic(data, collection)
            else:
                raise ValueError(f"Unknown data type: {type(data)}")
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
    with ApplyTaskFixture.fixture(db, http_client) as fixture:
        yield fixture
