from datetime import datetime, timedelta
from unittest.mock import call, patch

import pytest
from freezegun import freeze_time
from sqlalchemy import func, select

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.overdrive import OverdriveAPI
from palace.manager.celery.tasks import opds_odl
from palace.manager.celery.tasks.opds_odl import (
    _redis_lock_recalculate_holds,
    licensepool_ids_with_holds,
    recalculate_hold_queue,
    recalculate_hold_queue_collection,
    recalculate_holds_for_licensepool,
    remove_expired_holds,
    remove_expired_holds_for_collection,
    remove_expired_holds_for_collection_task,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import License, LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Patron
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import utc_now
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.services import ServicesFixture


class OpdsTaskFixture:
    def __init__(self, db: DatabaseTransactionFixture, services: ServicesFixture):
        self.db = db
        self.services = services

        self.two_weeks_ago = utc_now() - timedelta(weeks=2)
        self.yesterday = utc_now() - timedelta(days=1)
        self.tomorrow = utc_now() + timedelta(days=1)

    def hold(
        self,
        collection: Collection,
        *,
        start: datetime,
        end: datetime,
        position: int,
        pool: LicensePool | None = None,
        patron: Patron | None = None,
    ) -> Hold:
        if patron is None:
            patron = self.db.patron()
        if pool is None:
            _, pool = self.db.edition(collection=collection, with_license_pool=True)
        hold, _ = create(
            self.db.session,
            Hold,
            patron=patron,
            license_pool=pool,
            start=start,
            end=end,
            position=position,
        )
        return hold

    def holds(
        self, collection: Collection, pool: LicensePool | None = None
    ) -> tuple[set[int], set[int]]:
        expired_holds = {
            self.hold(
                collection,
                start=self.two_weeks_ago,
                end=self.yesterday,
                position=0,
                pool=pool,
            ).id
            for idx in range(10)
        }
        ready_non_expired_holds = {
            self.hold(
                collection,
                start=self.two_weeks_ago + timedelta(days=idx),
                end=self.tomorrow,
                position=0,
                pool=pool,
            ).id
            for idx in range(10)
        }
        not_ready_non_expired_holds = {
            self.hold(
                collection,
                start=self.yesterday,
                end=self.tomorrow,
                position=idx,
                pool=pool,
            ).id
            for idx in range(10)
        }

        return expired_holds, ready_non_expired_holds | not_ready_non_expired_holds

    def pool_with_licenses(
        self, collection: Collection, num_licenses: int = 2, available: bool = False
    ) -> tuple[LicensePool, list[License]]:
        edition = self.db.edition(collection=collection)
        pool = self.db.licensepool(
            edition, open_access=False, unlimited_access=False, collection=collection
        )
        licenses = [
            self.db.license(
                pool=pool,
                checkouts_available=idx + 1 if available else 0,
                terms_concurrency=idx + 1,
            )
            for idx in range(num_licenses)
        ]
        self.holds(collection, pool=pool)
        return pool, licenses


@pytest.fixture
def opds_task_fixture(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> OpdsTaskFixture:
    return OpdsTaskFixture(db, services_fixture)


def _hold_sort_key(hold: Hold) -> int:
    position = hold.position
    assert position is not None
    return position


def test_remove_expired_holds_for_collection(
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
    celery_fixture: CeleryFixture,
):
    collection = db.collection(protocol=OPDS2WithODLApi)
    decoy_collection = db.collection(protocol=OverdriveAPI)

    expired_holds, non_expired_holds = opds_task_fixture.holds(collection)
    decoy_expired_holds, decoy_non_expired_holds = opds_task_fixture.holds(
        decoy_collection
    )

    pools_before = db.session.scalars(
        select(func.count()).select_from(LicensePool)
    ).one()

    # Remove the expired holds
    assert collection.id is not None
    removed, events = remove_expired_holds_for_collection(
        db.session,
        collection.id,
    )

    # Assert that the correct holds were removed
    current_holds = {h.id for h in db.session.scalars(select(Hold))}

    assert expired_holds.isdisjoint(current_holds)
    assert non_expired_holds.issubset(current_holds)
    assert decoy_non_expired_holds.issubset(current_holds)
    assert decoy_expired_holds.issubset(current_holds)

    assert removed == 10

    pools_after = db.session.scalars(
        select(func.count()).select_from(LicensePool)
    ).one()

    # Make sure the license pools for those holds were not deleted
    assert pools_before == pools_after

    # verify that the correct analytics calls were made
    assert len(events) == 10
    for event in events:
        assert event["event_type"] == CirculationEvent.CM_HOLD_EXPIRED


def test_licensepools_with_holds(
    db: DatabaseTransactionFixture, opds_task_fixture: OpdsTaskFixture
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)
    collection2 = db.collection(protocol=OPDS2WithODLApi)

    # create some holds on Collection2 to ensure that the query is correct
    opds_task_fixture.holds(collection2)

    # Create some license pools
    pools = [
        db.edition(collection=collection1, with_license_pool=True)[1]
        for idx in range(10)
    ]

    # Create holds for some of the license pools
    for pool in pools[5:]:
        opds_task_fixture.holds(collection1, pool=pool)

    queried_pools: list[int] = []
    iterations = 0

    # Query the license pools with holds
    assert collection1.id is not None
    while license_pools := licensepool_ids_with_holds(
        db.session,
        collection1.id,
        batch_size=2,
        after_id=queried_pools[-1] if queried_pools else None,
    ):
        queried_pools.extend(license_pools)
        iterations += 1

    assert len(queried_pools) == 5
    assert iterations == 3
    assert queried_pools == [p.id for p in pools[5:]]


@freeze_time()
def test_recalculate_holds_for_licensepool(
    db: DatabaseTransactionFixture, opds_task_fixture: OpdsTaskFixture
):
    collection = db.collection(protocol=OPDS2WithODLApi)
    pool, [license1, license2] = opds_task_fixture.pool_with_licenses(collection)

    analytics = opds_task_fixture.services.analytics_fixture.analytics_mock
    # Recalculate the hold queue
    recalculate_holds_for_licensepool(pool, timedelta(days=5))

    current_holds = pool.get_active_holds()
    assert len(current_holds) == 20
    assert current_holds[0].position == 1
    assert current_holds[-1].position == len(current_holds)

    # Make a couple of copies available and recalculate the hold queue
    license1.checkouts_available = 1
    license2.checkouts_available = 2
    reservation_time = timedelta(days=5)
    _, events = recalculate_holds_for_licensepool(pool, reservation_time)

    assert pool.licenses_reserved == 3
    assert pool.licenses_available == 0
    current_holds = pool.get_active_holds()
    assert len(current_holds) == 20

    reserved_holds = [h for h in current_holds if h.position == 0]
    waiting_holds = [h for h in current_holds if h.position and h.position > 0]

    assert len(reserved_holds) == 3
    assert len(waiting_holds) == 17

    assert all(h.end == utc_now() + reservation_time for h in reserved_holds)
    assert all(
        h.start and waiting_holds[0].start and h.start < waiting_holds[0].start
        for h in reserved_holds
    )

    waiting_holds.sort(key=_hold_sort_key)
    for idx, hold in enumerate(waiting_holds):
        assert hold.position == idx + 1
        assert hold.end is None

        expected_start = (
            waiting_holds[idx - 1].start if idx else reserved_holds[-1].start
        )
        assert hold.start and expected_start and hold.start >= expected_start

    # verify that the correct analytics events were returned
    assert len(events) == 3
    for event in events:
        assert event["event_type"] == CirculationEvent.CM_HOLD_READY_FOR_CHECKOUT


def test_remove_expired_holds_for_collection_task(
    celery_fixture: CeleryFixture,
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)

    expired_holds1, non_expired_holds1 = opds_task_fixture.holds(collection1)

    # Remove the expired holds
    remove_expired_holds_for_collection_task.delay(collection1.id).wait()

    assert len(
        opds_task_fixture.services.analytics_fixture.analytics_mock.method_calls
    ) == len(expired_holds1)

    current_holds = {h.id for h in db.session.scalars(select(Hold))}
    assert expired_holds1.isdisjoint(current_holds)

    assert non_expired_holds1.issubset(current_holds)


def test_remove_expired_holds(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)
    collection2 = db.collection(protocol=OPDS2WithODLApi)
    decoy_collection = db.collection(protocol=OverdriveAPI)

    with patch.object(
        opds_odl, "remove_expired_holds_for_collection_task"
    ) as mock_remove:
        remove_expired_holds.delay().wait()

    assert mock_remove.delay.call_count == 2
    mock_remove.delay.assert_has_calls(
        [call(collection1.id), call(collection2.id)], any_order=True
    )


def test_recalculate_hold_queue(
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    db: DatabaseTransactionFixture,
    opds_task_fixture: OpdsTaskFixture,
):
    collection1 = db.collection(protocol=OPDS2WithODLApi)
    collection2 = db.collection(protocol=OPDS2WithODLApi)
    decoy_collection = db.collection(protocol=OverdriveAPI)

    with patch.object(
        opds_odl, "recalculate_hold_queue_collection"
    ) as mock_recalculate:
        recalculate_hold_queue.delay().wait()

    assert mock_recalculate.delay.call_count == 2
    mock_recalculate.delay.assert_has_calls(
        [call(collection1.id), call(collection2.id)], any_order=True
    )


class TestRecalculateHoldQueueCollection:
    def test_success(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        opds_task_fixture: OpdsTaskFixture,
    ):
        collection = db.collection(protocol=OPDS2WithODLApi)
        pools = [
            opds_task_fixture.pool_with_licenses(
                collection, num_licenses=1, available=True
            )
            for idx in range(15)
        ]

        # Do recalculation
        recalculate_hold_queue_collection.delay(collection.id, batch_size=2).wait()

        for pool, [license] in pools:
            current_holds = pool.get_active_holds()
            assert len(current_holds) == 20
            [reserved_hold] = [h for h in current_holds if h.position == 0]
            waiting_holds = [h for h in current_holds if h.position and h.position > 0]

            assert len(waiting_holds) == 19

            assert reserved_hold.end is not None
            assert reserved_hold.start is not None
            assert waiting_holds[0].start is not None
            assert reserved_hold.start < waiting_holds[0].start

            waiting_holds.sort(key=_hold_sort_key)
            for idx, hold in enumerate(waiting_holds):
                assert hold.position == idx + 1
                assert hold.end is None
                assert hold.start is not None
                expected_start = (
                    waiting_holds[idx - 1].start if idx else reserved_hold.start
                )
                assert expected_start is not None
                assert hold.start >= expected_start

    def test_already_running(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)

        collection = db.collection(protocol=OPDS2WithODLApi)
        assert collection.id is not None
        lock = _redis_lock_recalculate_holds(redis_fixture.client, collection.id)

        # Acquire the lock, to simulate another task already running
        lock.acquire()
        recalculate_hold_queue_collection.delay(collection.id).wait()

        assert "another task holds its lock" in caplog.text

    def test_collection_deleted(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OPDS2WithODLApi)
        collection_id = collection.id
        db.session.delete(collection)

        recalculate_hold_queue_collection.delay(collection_id).wait()

        assert "because it no longer exists" in caplog.text

    def test_pool_deleted(
        self,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        db: DatabaseTransactionFixture,
        opds_task_fixture: OpdsTaskFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        collection = db.collection(protocol=OPDS2WithODLApi)
        pool, _ = opds_task_fixture.pool_with_licenses(
            collection, num_licenses=1, available=True
        )
        deleted_pool, _ = opds_task_fixture.pool_with_licenses(
            collection, num_licenses=1, available=True
        )
        deleted_pool_id = deleted_pool.id
        db.session.delete(deleted_pool)

        assert pool.licenses_reserved != 1

        with patch.object(
            opds_odl, "licensepool_ids_with_holds"
        ) as mock_licensepool_ids_with_holds:
            mock_licensepool_ids_with_holds.return_value = [deleted_pool_id, pool.id]
            recalculate_hold_queue_collection.delay(collection.id).wait()

        # The deleted pool was skipped
        assert (
            f"Skipping license pool {deleted_pool_id} because it no longer exists"
            in caplog.text
        )

        # The other pool was recalculated
        assert pool.licenses_reserved == 1
