from pytest import LogCaptureFixture
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from core.celery.tasks.collection_delete import CollectionDeleteJob, collection_delete
from core.model import Collection
from core.service.logging.configuration import LogLevel
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture


def test_collection_delete_job_collection(db: DatabaseTransactionFixture):
    # A non-existent collection should return None
    assert CollectionDeleteJob.collection(db.session, 1) is None

    collection = db.collection(name="collection1")
    assert collection.id is not None
    assert CollectionDeleteJob.collection(db.session, collection.id) == collection


def test_collection_delete_job_collection_name(db: DatabaseTransactionFixture):
    collection = db.collection(name="collection1")
    assert (
        CollectionDeleteJob.collection_name(collection)
        == f"{collection.name}/{collection.protocol} ({collection.id})"
    )


def test_collection_delete_job_run(
    db: DatabaseTransactionFixture,
    mock_session_maker: sessionmaker,
    caplog: LogCaptureFixture,
):
    # A non-existent collection should log an error
    caplog.set_level(LogLevel.info)
    CollectionDeleteJob(mock_session_maker, 1).run()
    assert "Collection with id 1 not found. Unable to delete." in caplog.text

    collection = db.collection(name="collection1")
    collection.marked_for_deletion = True
    query = select(Collection).where(Collection.id == collection.id)

    assert db.session.execute(query).scalar_one_or_none() == collection

    assert collection.id is not None
    job = CollectionDeleteJob(mock_session_maker, collection.id)
    job.run()
    assert db.session.execute(query).scalar_one_or_none() is None
    assert f"Deleting collection" in caplog.text


def test_collection_delete_task(
    db: DatabaseTransactionFixture, celery_fixture: CeleryFixture
):
    collection = db.collection(name="collection1")
    collection.marked_for_deletion = True
    query = select(Collection).where(Collection.id == collection.id)
    assert db.session.execute(query).scalar_one_or_none() == collection
    collection_delete.delay(collection.id).wait()
    assert db.session.execute(query).scalar_one_or_none() is None
