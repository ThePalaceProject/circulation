import logging
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from fixtures.celery import CeleryFixture
from fixtures.database import DatabaseTransactionFixture
from fixtures.redis import RedisFixture
from sqlalchemy.orm.exc import ObjectDeletedError, StaleDataError

from palace.manager.api.axis import Axis360API
from palace.manager.celery.tasks import axis
from palace.manager.celery.tasks.axis import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_START_TIME,
    _redis_lock_queue_collection_import,
    import_all_collections,
    import_identifiers,
    list_identifiers_for_import,
    reap_all_collections,
    reap_collection,
    timestamp,
)
from palace.manager.core.metadata_layer import CirculationData, IdentifierData, Metadata
from palace.manager.sqlalchemy.model.identifier import Identifier
from palace.manager.util.datetime_helpers import utc_now

TEST_COLLECTION_ID = 1


class QueueCollectionImportLockFixture:
    def __init__(self, redis_fixture: RedisFixture):
        self.redis_fixture = redis_fixture
        self.redis_client = redis_fixture.client
        self.task = MagicMock()
        self.task.request.root_id = "fake"
        self.task_lock = _redis_lock_queue_collection_import(
            self.redis_client, collection_id=TEST_COLLECTION_ID
        )


@pytest.fixture
def queue_collection_import_lock_fixture(redis_fixture: RedisFixture):
    return QueueCollectionImportLockFixture(redis_fixture)


def test_queue_collection_import_lock(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    queue_collection_import_lock_fixture.task_lock.acquire()
    collection_id = TEST_COLLECTION_ID
    list_identifiers_for_import.delay(collection_id).wait()
    assert "another task holds its lock" in caplog.text


def set_caplog_level_to_info(caplog):
    caplog.set_level(
        logging.INFO,
        "palace.manager.celery.tasks.axis",
    )


def test_import_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    db.default_collection()
    collection2 = db.collection(name="test_collection", protocol=Axis360API.label())
    with patch.object(
        axis, "list_identifiers_for_import"
    ) as mock_list_identifiers_for_import:
        import_all_collections.delay().wait()

        assert mock_list_identifiers_for_import.apply_async.call_count == 1
        assert (
            mock_list_identifiers_for_import.apply_async.call_args_list[0].args[0]
            == collection2.id
        )
        assert mock_list_identifiers_for_import.apply_async.call_args_list[0].kwargs[
            "link"
        ] == import_identifiers.s(
            collection_id=collection2.id, batch_size=DEFAULT_BATCH_SIZE
        )
        assert "Finished queuing 1 collection." in caplog.text


def test_timestamp(db: DatabaseTransactionFixture):
    c1 = db.default_collection()
    ts1 = timestamp(
        _db=db.session,
        collection=c1,
        service_name="test task",
        default_start_time=DEFAULT_START_TIME,
    )

    assert ts1.start == DEFAULT_START_TIME
    assert ts1.finish is None

    ts2 = timestamp(
        _db=db.session,
        collection=c1,
        service_name="test task",
        default_start_time=DEFAULT_START_TIME,
    )

    assert ts1.id == ts2.id

    ts2.start = utc_now() - timedelta(days=1)
    ts2.finish = utc_now()
    ts3 = timestamp(
        _db=db.session,
        collection=c1,
        service_name="test task",
        default_start_time=DEFAULT_START_TIME,
    )

    assert ts2.id == ts3.id
    assert ts3.start != DEFAULT_START_TIME
    assert ts3.finish is not None


def test_list_identifiers_for_import(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())
    mock_api = MagicMock()
    current_time = utc_now()
    test_ids = ["a", "b", "c"]
    mock_api.recent_activity.return_value = (
        generate_test_metadata_and_circulation_objects(test_ids)
    )
    with patch.object(axis, "create_api") as mock_create_api:
        mock_create_api.return_value = mock_api
        identifiers = list_identifiers_for_import.delay(
            collection_id=collection.id
        ).get(timeout=100)
        assert identifiers == ["a", "b", "c"]

    ts = timestamp(
        _db=db.session,
        collection=collection,
        service_name="palace.manager.celery.tasks.axis.list_identifiers_for_import",
        default_start_time=DEFAULT_START_TIME,
    )

    assert ts.start > current_time
    assert not queue_collection_import_lock_fixture.task_lock.locked()
    assert mock_api.recent_activity.call_count == 1
    assert mock_api.recent_activity.call_args[0][0] == axis.DEFAULT_START_TIME
    assert "Finished listing identifiers in collection" in caplog.text


def generate_test_metadata_and_circulation_objects(test_ids):
    metadata_and_circulation_data_list = []
    for id in test_ids:
        data_source = "data_source"
        identifier = IdentifierData(type=Identifier.AXIS_360_ID, identifier=id)
        metadata_and_circulation_data_list.append(
            (
                Metadata(data_source=data_source, primary_identifier=identifier),
                CirculationData(data_source=data_source, primary_identifier=identifier),
            )
        )
    return metadata_and_circulation_data_list


def test_import_items(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())

    mock_api = MagicMock()
    with (
        patch.object(axis, "create_api") as mock_create_api,
        patch.object(axis, "requeue_import_identifiers_task") as requeue,
    ):
        mock_create_api.return_value = mock_api
        edition_1, lp_1 = db.edition(with_license_pool=True)
        edition_2, lp_2 = db.edition(with_license_pool=True)
        title_ids = [x.primary_identifier.identifier for x in [edition_1, edition_2]]
        mock_api.availability_by_title_ids.return_value = (
            generate_test_metadata_and_circulation_objects(title_ids)
        )
        identifiers = [x.primary_identifier.identifier for x in [edition_1, edition_2]]
        mock_api.update_book.side_effect = [
            (edition_1, False, lp_1, False),
            (edition_2, False, lp_2, False),
        ]
        import_identifiers.delay(
            collection.id, identifiers=identifiers, batch_size=25
        ).wait()

    assert mock_api.availability_by_title_ids.call_count == 1
    assert mock_api.availability_by_title_ids.call_args.kwargs["title_ids"] == title_ids
    assert mock_api.update_book.call_count == 2
    assert requeue.call_count == 0

    assert f"Edition (id={edition_1.id}" in caplog.text
    assert (
        f"Finished run importing identifiers for collection ({collection.name}, id={collection.id})"
        in caplog.text
    )
    assert f"Imported {len(title_ids)} identifiers" in caplog.text


def test_import_items_two_batches(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())

    mock_api = MagicMock()
    with (
        patch.object(axis, "create_api") as mock_create_api,
        patch.object(axis, "requeue_import_identifiers_task") as requeue,
    ):
        mock_create_api.return_value = mock_api
        edition_1, lp_1 = db.edition(with_license_pool=True)
        edition_2, lp_2 = db.edition(with_license_pool=True)
        title_ids = [x.primary_identifier.identifier for x in [edition_1, edition_2]]
        mock_api.availability_by_title_ids.return_value = (
            generate_test_metadata_and_circulation_objects(title_ids[0:1])
        )
        identifiers = [x.primary_identifier.identifier for x in [edition_1, edition_2]]
        mock_api.update_book.side_effect = [
            (edition_1, False, lp_1, False),
            (edition_2, False, lp_2, False),
        ]
        import_identifiers.delay(
            collection.id,
            identifiers=identifiers,
            batch_size=1,
            target_max_execution_time_in_seconds=0,
        ).wait()

    assert mock_api.availability_by_title_ids.call_count == 1
    assert (
        mock_api.availability_by_title_ids.call_args.kwargs["title_ids"]
        == title_ids[0:1]
    )
    assert mock_api.update_book.call_count == 1
    assert requeue.call_count == 1
    assert requeue.call_args_list[0].kwargs == {
        "processed_count": 1,
        "identifiers": title_ids[1:],
        "collection": collection,
        "batch_size": 1,
    }

    assert f"Edition (id={edition_1.id}" in caplog.text
    assert f"Finished run" not in caplog.text

    assert f"Imported {1} identifiers"


def test_reap_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection1 = db.default_collection()
    collection2 = db.collection(name="test_collection", protocol=Axis360API.label())
    with patch.object(axis, "reap_collection") as mock_reap_collection:
        reap_all_collections.delay().wait()

        assert mock_reap_collection.delay.call_count == 1
        assert mock_reap_collection.delay.call_args_list[0].kwargs == {
            "collection_id": collection2.id,
        }
        assert "Finished queuing collection for reaping." in caplog.text


def test_reap_collection_with_requeue(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())
    editions = []
    for i in range(0, 3):
        edition, lp = db.edition(
            with_license_pool=True,
            identifier_type=Identifier.AXIS_360_ID,
            collection=collection,
        )
        editions.append(edition)

    mock_api = MagicMock()
    with (
        patch.object(axis, "create_api") as mock_create_api,
        patch.object(axis, "requeue_reap_collection") as requeue,
    ):
        mock_create_api.return_value = mock_api
        reap_collection.delay(collection_id=collection.id, batch_size=2).wait()

    assert mock_api.update_licensepools_for_identifiers.call_count == 1
    assert requeue.call_count == 1
    assert requeue.call_args_list[0].kwargs["new_offset"] == 2
    assert mock_api.update_licensepools_for_identifiers.call_args_list[0].kwargs == {
        "identifiers": [x.primary_identifier for x in editions[0:2]],
    }
    assert f"Queued reap_collection task at offset=2" in caplog.text


def test_reap_collection_finish(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())
    editions = []
    for i in range(0, 1):
        edition, lp = db.edition(
            with_license_pool=True,
            identifier_type=Identifier.AXIS_360_ID,
            collection=collection,
        )
        editions.append(edition)

    mock_api = MagicMock()
    with (
        patch.object(axis, "create_api") as mock_create_api,
        patch.object(axis, "requeue_reap_collection") as requeue,
    ):
        mock_create_api.return_value = mock_api
        reap_collection.delay(collection_id=collection.id).wait()

    assert mock_api.update_licensepools_for_identifiers.call_count == 1
    assert requeue.call_count == 0
    assert mock_api.update_licensepools_for_identifiers.call_args_list[0].kwargs == {
        "identifiers": [x.primary_identifier for x in editions],
    }
    assert (
        f'Reaping of collection (name="{collection.name}", id={collection.id}) complete.'
        in caplog.text
    )


def test_retry(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())

    edition, licensepool = db.edition(
        collection=collection,
        with_license_pool=True,
        identifier_type=Identifier.AXIS_360_ID,
        identifier_id="012345678",
    )

    mock_api = MagicMock()
    with patch.object(axis, "create_api") as mock_create_api:
        mock_create_api.return_value = mock_api
        edition, lp = db.edition(with_license_pool=True)

        mock_api.availability_by_title_ids.return_value = [({}, {})]

        mock_api.recent.side_effect = [
            ObjectDeletedError({}, "object deleted"),
            StaleDataError("stale data"),
            (edition, False, licensepool, False),
        ]

        mock_api.update_book.side_effect = [
            ObjectDeletedError({}, "object deleted"),
            StaleDataError("stale data"),
            (edition, False, licensepool, False),
        ]

        import_identifiers.delay(
            collection.id, identifiers=[edition.primary_identifier.identifier]
        ).wait()

        assert mock_api.update_book.call_count == 3
