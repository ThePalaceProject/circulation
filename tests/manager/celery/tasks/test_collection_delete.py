from pytest import LogCaptureFixture
from sqlalchemy import select

from palace.manager.celery.tasks.collection_delete import (
    _collection_name,
    collection_delete,
)
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.model.collection import Collection
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


class TestCollectionDelete:
    def test__collection_name(self, db: DatabaseTransactionFixture):
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
    ):
        # A non-existent collection should log an error
        caplog.set_level(LogLevel.info)
        collection_delete.delay(1).wait()
        assert "Collection with id 1 not found. Unable to delete." in caplog.text

    def test_collection_delete_task(
        self, db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
    ):
        collection = db.collection(name="collection1")
        collection.marked_for_deletion = True
        query = select(Collection).where(Collection.id == collection.id)
        assert db.session.execute(query).scalar_one_or_none() == collection
        collection_delete.delay(collection.id).wait()
        assert db.session.execute(query).scalar_one_or_none() is None
