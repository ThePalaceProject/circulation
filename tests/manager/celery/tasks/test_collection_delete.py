from pytest import LogCaptureFixture
from sqlalchemy import select

from palace.manager.celery.tasks.collection_delete import (
    _collection_name,
    collection_delete,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.licensing import LicensePool
from palace.manager.sqlalchemy.model.patron import Hold, Loan
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestCollectionDelete:
    def test__collection_name(self, db: DatabaseTransactionFixture) -> None:
        collection = db.collection(name="collection1")
        assert (
            _collection_name(collection)
            == f"{collection.name}/{collection.protocol} ({collection.id})"
        )

    def test_collection_delete_collection_does_not_exist(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """A non-existent collection should log an error."""
        caplog.set_level(LogLevel.info)
        collection_delete.delay(1).wait()
        assert "Collection with id 1 not found. Unable to delete." in caplog.text

    def test_collection_delete_task(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ) -> None:
        """A collection marked for deletion is fully removed."""
        collection = db.collection(name="collection1")
        collection.marked_for_deletion = True
        query = select(Collection).where(Collection.id == collection.id)
        assert db.session.execute(query).scalar_one_or_none() == collection
        collection_delete.delay(collection.id).wait()
        assert db.session.execute(query).scalar_one_or_none() is None

    def test_collection_delete_batched(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        caplog: LogCaptureFixture,
    ) -> None:
        """Deletion re-queues itself when there are more pools than batch_size."""
        caplog.set_level(LogLevel.info)

        collection = db.collection(name="batched")
        collection.marked_for_deletion = True
        # Create 5 license pools
        for _ in range(5):
            db.edition(with_license_pool=True, collection=collection)

        collection_id = collection.id
        pool_count = (
            db.session.query(LicensePool)
            .filter(LicensePool.collection_id == collection_id)
            .count()
        )
        assert pool_count == 5

        # Use batch_size=2 so it takes multiple rounds
        collection_delete.delay(collection_id, batch_size=2).wait()

        # The collection and all pools should be deleted
        assert db.session.get(Collection, collection_id) is None
        assert (
            db.session.query(LicensePool)
            .filter(LicensePool.collection_id == collection_id)
            .count()
            == 0
        )

        # Verify re-queueing happened
        assert "Re-queueing" in caplog.text

    def test_collection_delete_with_child_records(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
    ) -> None:
        """License pools with loans, holds, and works are cleaned up correctly."""
        collection = db.collection(name="with_children")
        collection.marked_for_deletion = True

        # Create edition/pool with a work
        work = db.work(with_license_pool=True, collection=collection)
        pool = work.license_pools[0]
        patron = db.patron()

        # Add a loan and hold
        pool.loan_to(patron)
        pool.on_hold_to(patron)

        collection_id = collection.id
        pool_id = pool.id
        work_id = work.id

        collection_delete.delay(collection_id).wait()

        # Collection, pool, loan, hold all gone
        assert db.session.get(Collection, collection_id) is None
        assert db.session.get(LicensePool, pool_id) is None
        assert db.session.query(Loan).all() == []
        assert db.session.query(Hold).all() == []

        # The orphaned work will be cleaned up by work_reaper, so it may
        # still exist (no license pools though).
        orphaned_work = db.session.get(Work, work_id)
        if orphaned_work is not None:
            assert orphaned_work.license_pools == []
