import logging
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy.orm.exc import ObjectDeletedError

from palace.manager.api.axis import Axis360API
from palace.manager.celery.tasks import axis
from palace.manager.celery.tasks.axis import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_START_TIME,
    _redis_lock_list_identifiers_for_import,
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
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.redis import RedisFixture
from tests.manager.api.test_axis import axis_files_fixture  # noqa: autoflake
from tests.manager.api.test_axis import AxisFilesFixture
from tests.mocks.axis import MockAxis360API


class QueueCollectionImportLockFixture:
    def __init__(self, redis_fixture: RedisFixture, db: DatabaseTransactionFixture):
        self.redis_fixture = redis_fixture
        self.redis_client = redis_fixture.client
        self.task = MagicMock()
        self.task.request.root_id = "fake"
        self.collection = db.collection(protocol=Axis360API)
        self.task_lock = _redis_lock_list_identifiers_for_import(
            self.redis_client, collection_id=self.collection.id
        )


@pytest.fixture
def queue_collection_import_lock_fixture(
    redis_fixture: RedisFixture, db: DatabaseTransactionFixture
):
    return QueueCollectionImportLockFixture(redis_fixture, db)


def test_queue_collection_import_lock(
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    queue_collection_import_lock_fixture.task_lock.acquire()
    list_identifiers_for_import.delay(
        collection_id=queue_collection_import_lock_fixture.collection.id
    ).wait()
    assert "Skipping list_identifiers_for_import" in caplog.text


def set_caplog_level_to_info(caplog):
    caplog.set_level(
        logging.INFO,
        "palace.manager.celery.tasks.axis",
    )


def test_import_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    db.default_collection()
    collection2 = db.collection(name="test_collection", protocol=Axis360API.label())
    with patch.object(
        axis, "list_identifiers_for_import"
    ) as mock_list_identifiers_for_import:
        # turn off subtask execution interval.
        import_all_collections.apply_async(
            kwargs={"sub_task_execution_interval_in_secs": 0}
        ).wait()

        assert mock_list_identifiers_for_import.apply_async.call_count == 1
        assert (
            mock_list_identifiers_for_import.apply_async.call_args_list[0].kwargs[
                "kwargs"
            ]["collection_id"]
            == collection2.id
        )
        assert mock_list_identifiers_for_import.apply_async.call_args_list[0].kwargs[
            "link"
        ] == import_identifiers.s(
            collection_id=collection2.id,
            batch_size=DEFAULT_BATCH_SIZE,
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

    assert ts.start and ts.start > current_time
    assert not queue_collection_import_lock_fixture.task_lock.locked()
    assert mock_api.recent_activity.call_count == 1
    assert mock_api.recent_activity.call_args[0][0] == axis.DEFAULT_START_TIME
    assert "Finished listing identifiers in collection" in caplog.text


def generate_test_metadata_and_circulation_objects(test_ids: list[str]):
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
    with (patch.object(axis, "create_api") as mock_create_api,):
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
            collection_id=collection.id, identifiers=identifiers, batch_size=25
        ).wait()

    assert mock_api.availability_by_title_ids.call_count == 1
    assert mock_api.availability_by_title_ids.call_args.kwargs["title_ids"] == title_ids
    assert mock_api.update_book.call_count == 2

    assert f"Edition (id={edition_1.id}" in caplog.text
    assert (
        f"Finished importing identifiers for collection ({collection.name}, id={collection.id})"
        in caplog.text
    )
    assert (
        f'Batch of 2 identifiers for collection (name="{collection.name}", '
        f"id={collection.id}) imported"
    ) in caplog.text


def test_import_identifiers_with_requeue(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    queue_collection_import_lock_fixture: QueueCollectionImportLockFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    collection = db.collection(name="test_collection", protocol=Axis360API.label())

    mock_api = MagicMock()
    with (patch.object(axis, "create_api") as mock_create_api,):
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
            identifiers=identifiers,
            collection_id=collection.id,
            batch_size=1,
        ).wait()

    assert mock_api.availability_by_title_ids.call_count == 2
    assert (
        mock_api.availability_by_title_ids.call_args_list[0].kwargs["title_ids"]
        == title_ids[0:1]
    )
    assert (
        mock_api.availability_by_title_ids.call_args_list[1].kwargs["title_ids"]
        == title_ids[1:]
    )
    assert mock_api.update_book.call_count == 2

    assert f"Edition (id={edition_1.id}" in caplog.text
    assert f"Finished run" not in caplog.text
    assert f"Imported {2} identifiers"
    assert f"Replacing task to continue importing remaining 1 identifier" in caplog.text


def test_reap_all_collections(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    caplog: pytest.LogCaptureFixture,
):
    set_caplog_level_to_info(caplog)
    db.default_collection()
    collection2 = db.collection(name="test_collection", protocol=Axis360API.label())
    with patch.object(axis, "reap_collection") as mock_reap_collection:
        reap_all_collections.apply_async(
            kwargs={"sub_task_execution_interval_in_secs": 0}
        ).wait()

        assert mock_reap_collection.apply_async.call_count == 1
        assert (
            mock_reap_collection.apply_async.call_args_list[0].kwargs["kwargs"][
                "collection_id"
            ]
            == collection2.id
        )
        assert "Finished queuing reap collection tasks" in caplog.text


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

    identifiers = [x.primary_identifier for x in editions]
    mock_api = MagicMock()
    with patch.object(axis, "create_api") as mock_create_api:
        mock_create_api.return_value = mock_api

        reap_collection.delay(collection_id=collection.id, batch_size=2).wait()

        update_license_pools = mock_api.update_licensepools_for_identifiers
        assert update_license_pools.call_count == 2

        assert update_license_pools.call_args_list[0].kwargs == {
            "identifiers": identifiers[0:2]
        }
        assert update_license_pools.call_args_list[1].kwargs == {
            "identifiers": identifiers[2:]
        }
        assert f"Re-queuing reap_collection task at offset=2" in caplog.text
        assert (
            f'Reaping of collection (name="{collection.name}", id={collection.id}) complete.'
            in caplog.text
        )


def test_retry_import_identifiers(
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

        mock_api.update_book.side_effect = [
            ObjectDeletedError(None),
            (edition, False, licensepool, False),
        ]

        import_identifiers.delay(
            collection_id=collection.id,
            identifiers=[edition.primary_identifier.identifier],
        ).wait()

        assert mock_api.update_book.call_count == 2


def test_process_item_creates_presentation_ready_work(
    axis_files_fixture: AxisFilesFixture,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    """Test the normal workflow where we ask Axis for data,
    Axis provides it, and we create a presentation-ready work.
    """
    library = db.default_library()
    collection = MockAxis360API.mock_collection(db.session, library=library)
    data = axis_files_fixture.sample_data("single_item.xml")

    with (patch.object(axis, "create_api") as mock_create_api,):
        api = MockAxis360API(_db=db.session, collection=collection)
        mock_create_api.return_value = api
        api.queue_response(200, content=data)

        # Here's the book mentioned in single_item.xml.
        identifier = db.identifier(identifier_type=Identifier.AXIS_360_ID)
        identifier.identifier = "0003642860"

        # This book has no LicensePool.
        assert [] == identifier.licensed_through

        import_identifiers.delay(
            collection_id=collection.id, identifiers=[identifier.identifier]
        ).wait()

        # A LicensePool was created. We know both how many copies of this
        # book are available, and what formats it's available in.
        [pool] = identifier.licensed_through
        assert 9 == pool.licenses_owned
        [lpdm] = pool.delivery_mechanisms
        assert (
            "application/epub+zip (application/vnd.adobe.adept+xml)"
            == lpdm.delivery_mechanism.name
        )

        # A Work was created and made presentation ready.
        assert "Faith of My Fathers : A Family Memoir" == pool.work.title
        assert pool.work.presentation_ready is True


def test_transient_failure_if_requested_book_not_mentioned(
    axis_files_fixture: AxisFilesFixture,
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
):
    """Test an unrealistic case where we ask Axis 360 about one book and
    it tells us about a totally different book.
    """
    library = db.default_library()
    collection = MockAxis360API.mock_collection(db.session, library=library)

    with (patch.object(axis, "create_api") as mock_create_api,):
        api = MockAxis360API(_db=db.session, collection=collection)
        mock_create_api.return_value = api

        # We're going to ask about abcdef
        identifier = db.identifier(identifier_type=Identifier.AXIS_360_ID)
        identifier.identifier = "abcdef"

        # But we're going to get told about 0003642860.
        data = axis_files_fixture.sample_data("single_item.xml")
        api.queue_response(200, content=data)

        import_identifiers.delay(
            collection_id=collection.id, identifiers=[identifier.identifier]
        ).wait()

        # And nothing major was done about the book we were told
        # about. We created an Identifier record for its identifier,
        # but no LicensePool or Edition.
        wrong_identifier = Identifier.for_foreign_id(
            db.session, Identifier.AXIS_360_ID, "0003642860"
        )
        assert [] == identifier.licensed_through
        assert [] == identifier.primarily_identifies
