import datetime
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from botocore.exceptions import ClientError
from pymarc import MARCReader
from pytest import MonkeyPatch
from sqlalchemy import select

from palace.util.datetime_helpers import utc_now

from palace.manager.celery.tasks import marc
from palace.manager.celery.tasks.marc import marc_export_collection_lock
from palace.manager.integration.catalog.marc.exporter import MARC_EPOCH, MarcExporter
from palace.manager.integration.catalog.marc.uploader import MarcUploadManager
from palace.manager.service.redis.models.lock import LockNotAcquired, RedisLock
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.model.work import Work
from tests.fixtures.celery import CeleryFixture
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.marc import MarcExporterFixture
from tests.fixtures.redis import RedisFixture
from tests.fixtures.s3 import S3ServiceFixture, S3ServiceIntegrationFixture
from tests.fixtures.services import ServicesFixture


class TestMarcExport:
    def test_no_works(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        celery_fixture: CeleryFixture,
    ):
        marc_exporter_fixture.configure_export()
        with patch.object(marc, "marc_export_collection") as marc_export_collection:
            # Because none of the collections have works, we should skip all of them.
            marc.marc_export.delay().wait()
            marc_export_collection.delay.assert_not_called()

    def test_normal_run(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        celery_fixture: CeleryFixture,
    ):
        marc_exporter_fixture.configure_export()
        marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection1,
            library=marc_exporter_fixture.library1,
            created=utc_now() - datetime.timedelta(days=7),
        )
        with patch.object(marc, "marc_export_collection") as marc_export_collection:
            # Runs against all the expected collections
            collections = [
                marc_exporter_fixture.collection1,
                marc_exporter_fixture.collection2,
                marc_exporter_fixture.collection3,
            ]
            for collection in collections:
                marc_exporter_fixture.work(collection)
            marc.marc_export.delay().wait()

            # We make the calls to generate a full export for every collection
            marc_export_collection.delay.assert_has_calls(
                [
                    call(
                        collection_id=collection.id,
                        collection_name=collection.name,
                        start_time=ANY,
                        libraries=ANY,
                    )
                    for collection in collections
                ],
                any_order=True,
            )

            # We make the calls to generate a delta export only for collection1
            marc_export_collection.delay.assert_any_call(
                collection_id=marc_exporter_fixture.collection1.id,
                collection_name=marc_exporter_fixture.collection1.name,
                start_time=ANY,
                libraries=ANY,
                delta=True,
            )

            # Make sure the call was made with the correct library set
            [delta_call] = [
                c
                for c in marc_export_collection.delay.mock_calls
                if "delta" in c.kwargs
            ]
            libraries_kwarg = delta_call.kwargs["libraries"]
            assert len(libraries_kwarg) == 1
            assert libraries_kwarg[0].library_id == marc_exporter_fixture.library1.id

    def test_skip_collections(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        celery_fixture: CeleryFixture,
    ):
        marc_exporter_fixture.configure_export()
        with patch.object(marc, "marc_export_collection") as marc_export_collection:
            # Collection 1 should be skipped because it has no works

            # Collection 2 should be skipped because it was updated recently
            marc_exporter_fixture.work(marc_exporter_fixture.collection2)
            marc_exporter_fixture.marc_file(
                collection=marc_exporter_fixture.collection2,
                library=marc_exporter_fixture.library1,
            )

            # Collection 3 should get a full export, but not a delta, because
            # its work hasn't been updated since the last full export
            work = marc_exporter_fixture.work(marc_exporter_fixture.collection3)
            work.last_update_time = utc_now() - datetime.timedelta(days=50)
            marc_exporter_fixture.marc_file(
                collection=marc_exporter_fixture.collection3,
                library=marc_exporter_fixture.library2,
                created=utc_now() - datetime.timedelta(days=45),
            )

            marc.marc_export.delay().wait()

            marc_export_collection.delay.assert_called_once_with(
                collection_id=marc_exporter_fixture.collection3.id,
                collection_name=marc_exporter_fixture.collection3.name,
                start_time=ANY,
                libraries=ANY,
            )


class MarcExportCollectionFixture:
    def __init__(
        self,
        db: DatabaseTransactionFixture,
        celery_fixture: CeleryFixture,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
        s3_service_fixture: S3ServiceFixture,
        services_fixture: ServicesFixture,
    ):
        self.db = db
        self.celery_fixture = celery_fixture
        self.redis_fixture = redis_fixture
        self.marc_exporter_fixture = marc_exporter_fixture
        self.s3_service_integration_fixture = s3_service_integration_fixture
        self.s3_service_fixture = s3_service_fixture
        self.services_fixture = services_fixture

        self.mock_s3 = self.s3_service_fixture.mock_service()
        self.mock_s3.MINIMUM_MULTIPART_UPLOAD_SIZE = 10
        marc_exporter_fixture.configure_export()

        self.start_time = utc_now()

    def marc_files(self) -> list[MarcFile]:
        return self.db.session.execute(select(MarcFile)).scalars().all()

    def setup_minio_storage(self) -> None:
        self.services_fixture.services.storage.override(
            self.s3_service_integration_fixture.container
        )

    def setup_mock_storage(self) -> None:
        self.services_fixture.services.storage.public.override(self.mock_s3)

    def works(self, collection: Collection) -> list[Work]:
        return [self.marc_exporter_fixture.work(collection) for _ in range(15)]

    def export_collection(self, collection: Collection, delta: bool = False) -> None:
        service = self.services_fixture.services.integration_registry.catalog_services()
        assert collection.id is not None
        libraries = MarcExporter.enabled_libraries(
            self.db.session, service, collection.id
        )
        marc.marc_export_collection.delay(
            collection.id,
            collection_name=collection.name,
            batch_size=5,
            start_time=self.start_time,
            libraries=libraries,
            delta=delta,
        ).wait()

    def redis_lock(self, collection: Collection, delta: bool = False) -> RedisLock:
        assert collection.id is not None
        return marc_export_collection_lock(
            self.redis_fixture.client, collection.id, delta=delta
        )


@pytest.fixture
def marc_export_collection_fixture(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    redis_fixture: RedisFixture,
    marc_exporter_fixture: MarcExporterFixture,
    s3_service_integration_fixture: S3ServiceIntegrationFixture,
    s3_service_fixture: S3ServiceFixture,
    services_fixture: ServicesFixture,
) -> MarcExportCollectionFixture:
    return MarcExportCollectionFixture(
        db,
        celery_fixture,
        redis_fixture,
        marc_exporter_fixture,
        s3_service_integration_fixture,
        s3_service_fixture,
        services_fixture,
    )


class TestMarcExportCollection:
    def test_normal_run(
        self,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
        marc_exporter_fixture: MarcExporterFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
    ):
        marc_export_collection_fixture.setup_minio_storage()
        collection = marc_exporter_fixture.collection1
        works = marc_export_collection_fixture.works(collection)
        work_uris = [work.license_pools[0].identifier.urn for work in works]

        # Run the full end-to-end process for exporting a collection, this should generate
        # 3 batches of 5 works each, putting the results into minio.
        marc_export_collection_fixture.export_collection(collection)

        # Lock is released
        assert not marc_export_collection_fixture.redis_lock(collection).locked()

        # Verify that the expected number of files were uploaded to minio.
        uploaded_files = s3_service_integration_fixture.list_objects("public")
        assert len(uploaded_files) == 2

        # Verify that the expected number of marc files were created in the database.
        # A first-run full export creates both a full record and a full-content delta
        # (MARC_EPOCH) pointing to the same S3 object; 2 libraries → 4 records, 2 S3 objects.
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 4
        full_files = [mf for mf in marc_files if mf.since is None]
        epoch_delta_files = [mf for mf in marc_files if mf.since == MARC_EPOCH]
        assert len(full_files) == 2
        assert len(epoch_delta_files) == 2
        assert {mf.key for mf in full_files} == {mf.key for mf in epoch_delta_files}
        filenames = [marc_file.key for marc_file in marc_files]

        # Verify that the uploaded files are the expected ones.
        assert set(uploaded_files) == set(filenames)

        # Verify that the marc files contain the expected works.
        for file in uploaded_files:
            data = s3_service_integration_fixture.get_object("public", file)
            records = list(MARCReader(data))
            assert len(records) == len(work_uris)
            marc_uris = [record["001"].data for record in records]
            assert set(marc_uris) == set(work_uris)

            # Make sure the records have the correct organization code.
            expected_org = "library1-org" if "library1" in file else "library2-org"
            assert all(record["003"].data == expected_org for record in records)

            # Make sure records have the correct status
            assert all(record.leader.record_status == "n" for record in records)

        # Try running a delta export now
        marc_export_collection_fixture.export_collection(collection, delta=True)

        # Because no works have been updated since the last run, no delta exports are generated
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 4

        # Update a couple works last_updated_time
        updated_works = [works[0], works[1]]
        for work in updated_works:
            work.last_update_time = utc_now()

        marc_export_collection_fixture.export_collection(collection, delta=True)

        # Now we generate marc files (4 from the first full run + 2 new delta records).
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 6
        delta_marc_files = [
            marc_file
            for marc_file in marc_files
            if marc_file.key and "delta" in marc_file.key
        ]
        assert len(delta_marc_files) == 2

        # Verify that the marc files contain the expected works.
        for marc_file in delta_marc_files:
            assert marc_file.key is not None
            data = s3_service_integration_fixture.get_object("public", marc_file.key)
            records = list(MARCReader(data))
            assert len(records) == 2
            marc_uris = [record["001"].data for record in records]
            assert set(marc_uris) == {
                work.license_pools[0].identifier.urn for work in updated_works
            }

            # Make sure the records have the correct organization code.
            expected_org = (
                "library1-org" if "library1" in marc_file.key else "library2-org"
            )
            assert all(record["003"].data == expected_org for record in records)

            # Make sure records have the correct status
            assert all(record.leader.record_status == "c" for record in records)

        # A subsequent full export creates only full records. The epoch delta
        # pair is a first-run behavior and must not repeat.
        marc_export_collection_fixture.export_collection(collection)
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 8
        assert len([mf for mf in marc_files if mf.since == MARC_EPOCH]) == 2

    @pytest.mark.parametrize(
        "delta", [pytest.param(False, id="full"), pytest.param(True, id="delta")]
    )
    def test_reset_race(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
        delta: bool,
    ):
        """An export whose records were reset mid-flight is discarded and re-run."""
        marc_export_collection_fixture.setup_minio_storage()
        collection = marc_exporter_fixture.collection1
        assert collection.id is not None
        marc_export_collection_fixture.works(collection)

        # Craft the raced state: the scheduling-time snapshot says the libraries
        # have prior exports, but no MarcFile records exist by completion time
        # (as if marc_export_reset ran while the export was in flight).
        libraries = MarcExporter.enabled_libraries(
            db.session, marc_exporter_fixture.registry, collection.id
        )
        stale_libraries = [
            info.model_copy(
                update={"last_updated": utc_now() - datetime.timedelta(days=2)}
            )
            for info in libraries
        ]
        marc.marc_export_collection.delay(
            collection.id,
            collection_name=collection.name,
            batch_size=5,
            start_time=marc_export_collection_fixture.start_time,
            libraries=stale_libraries,
            delta=delta,
        ).wait()

        # The stale uploads were discarded and a fresh full export ran, creating
        # the first-run full + epoch-delta pair for each library.
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 4
        assert len([mf for mf in marc_files if mf.since is None]) == 2
        assert len([mf for mf in marc_files if mf.since == MARC_EPOCH]) == 2

        # Only the fresh exports' objects exist in storage.
        uploaded = s3_service_integration_fixture.list_objects("public")
        assert set(uploaded) == {mf.key for mf in marc_files}

    def test_reset_race_first_run_settings_drift(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
    ):
        """A settings change during a first-run export (which the record-count
        check can't see) is caught by the settings-drift check and re-run."""
        marc_export_collection_fixture.setup_minio_storage()
        collection = marc_exporter_fixture.collection1
        assert collection.id is not None
        marc_export_collection_fixture.works(collection)

        # The export runs with a settings snapshot that no longer matches the
        # current configuration (as if the settings changed mid-flight).
        libraries = MarcExporter.enabled_libraries(
            db.session, marc_exporter_fixture.registry, collection.id
        )
        stale_libraries = [
            info.model_copy(update={"organization_code": "stale-org"})
            for info in libraries
        ]
        marc.marc_export_collection.delay(
            collection.id,
            collection_name=collection.name,
            batch_size=5,
            start_time=marc_export_collection_fixture.start_time,
            libraries=stale_libraries,
        ).wait()

        # The stale uploads were discarded and the re-run used current settings.
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 4
        uploaded = s3_service_integration_fixture.list_objects("public")
        assert set(uploaded) == {mf.key for mf in marc_files}
        for file in uploaded:
            data = s3_service_integration_fixture.get_object("public", file)
            for record in MARCReader(data):
                assert record["003"].data in {"library1-org", "library2-org"}

    def test_reset_race_after_finalize_check(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
        monkeypatch: MonkeyPatch,
    ):
        """A configuration change landing between the finalize check and the
        export's commit is detected after the commit and queues a reset."""
        marc_export_collection_fixture.setup_mock_storage()
        collection = marc_exporter_fixture.collection1
        marc_export_collection_fixture.works(collection)

        mock_delay = MagicMock()
        monkeypatch.setattr(marc.marc_export_reset, "delay", mock_delay)

        # The finalize check (one call per library) sees unchanged settings; the
        # post-commit verification sees changed ones.
        monkeypatch.setattr(
            marc,
            "_export_settings_changed",
            MagicMock(side_effect=[False, False, True, True]),
        )

        marc_export_collection_fixture.export_collection(collection)

        # A reset was queued for both libraries whose stale exports were recorded.
        assert mock_delay.call_count == 2
        assert {call_args.args[0] for call_args in mock_delay.call_args_list} == {
            marc_exporter_fixture.library1.id,
            marc_exporter_fixture.library2.id,
        }

    def test_reset_race_library_disabled(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
    ):
        """A raced library that is no longer MARC-enabled is discarded without a re-run."""
        marc_export_collection_fixture.setup_minio_storage()
        collection = marc_exporter_fixture.collection1
        assert collection.id is not None
        marc_export_collection_fixture.works(collection)

        libraries = MarcExporter.enabled_libraries(
            db.session, marc_exporter_fixture.registry, collection.id
        )
        stale_libraries = [
            info.model_copy(
                update={"last_updated": utc_now() - datetime.timedelta(days=2)}
            )
            for info in libraries
        ]

        # Remove library2's MARC exporter configuration mid-flight.
        assert marc_exporter_fixture.marc_integration is not None
        lc = marc_exporter_fixture.marc_integration.for_library(
            marc_exporter_fixture.library2
        )
        assert lc is not None
        db.session.delete(lc)

        marc.marc_export_collection.delay(
            collection.id,
            collection_name=collection.name,
            batch_size=5,
            start_time=marc_export_collection_fixture.start_time,
            libraries=stale_libraries,
        ).wait()

        # Library1 was re-run (first-run pair); library2 was discarded silently.
        marc_files = marc_export_collection_fixture.marc_files()
        assert {mf.library_id for mf in marc_files} == {
            marc_exporter_fixture.library1.id
        }
        assert len(marc_files) == 2
        uploaded = s3_service_integration_fixture.list_objects("public")
        assert set(uploaded) == {mf.key for mf in marc_files}

    def test_collection_no_works(
        self,
        marc_exporter_fixture: MarcExporterFixture,
        s3_service_integration_fixture: S3ServiceIntegrationFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
    ):
        marc_export_collection_fixture.setup_minio_storage()
        collection = marc_exporter_fixture.collection2
        marc_export_collection_fixture.export_collection(collection)

        assert marc_export_collection_fixture.marc_files() == []
        assert s3_service_integration_fixture.list_objects("public") == []
        assert not marc_export_collection_fixture.redis_lock(collection).locked()

    def test_exception_handled(
        self,
        marc_exporter_fixture: MarcExporterFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
    ):
        marc_export_collection_fixture.setup_mock_storage()
        collection = marc_exporter_fixture.collection1
        marc_export_collection_fixture.works(collection)

        with patch.object(MarcUploadManager, "complete") as complete:
            complete.side_effect = Exception("Test Exception")
            with pytest.raises(Exception, match="Test Exception"):
                marc_export_collection_fixture.export_collection(collection)

        # After the exception, we should have aborted the multipart uploads and released the lock
        assert marc_export_collection_fixture.marc_files() == []
        assert len(marc_export_collection_fixture.mock_s3.aborted) == 2
        assert not marc_export_collection_fixture.redis_lock(collection).locked()

    def test_locked(
        self,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
    ):
        collection = marc_exporter_fixture.collection1
        marc_export_collection_fixture.redis_lock(collection).acquire()
        marc_export_collection_fixture.setup_mock_storage()
        with pytest.raises(LockNotAcquired):
            marc_export_collection_fixture.export_collection(collection)


def test_marc_export_cleanup(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    s3_service_fixture: S3ServiceFixture,
    marc_exporter_fixture: MarcExporterFixture,
    services_fixture: ServicesFixture,
):
    marc_exporter_fixture.configure_export()
    mock_s3 = s3_service_fixture.mock_service()
    services_fixture.services.storage.public.override(mock_s3)

    not_deleted_id = marc_exporter_fixture.marc_file(created=utc_now()).id
    deleted_keys = [
        marc_exporter_fixture.marc_file(
            created=utc_now() - datetime.timedelta(days=d + 1)
        ).key
        for d in range(20)
    ]

    marc.marc_export_cleanup.delay(batch_size=5).wait()

    [not_deleted] = db.session.execute(select(MarcFile)).scalars().all()
    assert not_deleted.id == not_deleted_id
    assert mock_s3.deleted == deleted_keys


def test_marc_export_cleanup_shared_key(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    s3_service_fixture: S3ServiceFixture,
    marc_exporter_fixture: MarcExporterFixture,
    services_fixture: ServicesFixture,
):
    """S3 object shared by two MarcFile records is deleted only once, not twice."""
    marc_exporter_fixture.configure_export()
    mock_s3 = s3_service_fixture.mock_service()
    services_fixture.services.storage.public.override(mock_s3)

    shared_key = db.fresh_str()

    # Simulate a first-run pair: full record + MARC_EPOCH delta sharing the same S3 key.
    # Both belong to a collection not in the MARC-enabled set so they are cleaned up via
    # the "disabled pair" branch of files_for_cleanup.
    other_collection = db.collection()
    marc_exporter_fixture.marc_file(
        key=shared_key,
        collection=other_collection,
        library=marc_exporter_fixture.library1,
    )
    marc_exporter_fixture.marc_file(
        key=shared_key,
        collection=other_collection,
        library=marc_exporter_fixture.library1,
        since=MARC_EPOCH,
    )

    marc.marc_export_cleanup.delay(batch_size=100).wait()

    # Both MarcFile records removed from the database.
    remaining = db.session.execute(select(MarcFile)).scalars().all()
    assert all(mf.key != shared_key for mf in remaining)

    # S3 object deleted exactly once despite two records sharing the key.
    assert mock_s3.deleted.count(shared_key) == 1


def test_marc_export_reset(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    s3_service_fixture: S3ServiceFixture,
    marc_exporter_fixture: MarcExporterFixture,
    services_fixture: ServicesFixture,
):
    marc_exporter_fixture.configure_export()
    mock_s3 = s3_service_fixture.mock_service()
    services_fixture.services.storage.public.override(mock_s3)

    library1 = marc_exporter_fixture.library1
    library2 = marc_exporter_fixture.library2
    assert library1.id is not None

    # A nonexistent library is a no-op.
    marc.marc_export_reset.delay(library_id=-1).wait()

    # First-run pair for library1 / collection1 sharing one S3 object, plus a
    # record in another collection. Library2's record must not be affected.
    shared_key = db.fresh_str()
    marc_exporter_fixture.marc_file(key=shared_key)
    marc_exporter_fixture.marc_file(key=shared_key, since=MARC_EPOCH)
    collection2_key = marc_exporter_fixture.marc_file(
        collection=marc_exporter_fixture.collection2
    ).key
    library2_file = marc_exporter_fixture.marc_file(library=library2)

    marc.marc_export_reset.delay(library_id=library1.id).wait()

    # Only library2's record remains.
    remaining = db.session.execute(select(MarcFile)).scalars().all()
    assert remaining == [library2_file]

    # The shared S3 object was deleted exactly once; the other object once.
    assert mock_s3.deleted.count(shared_key) == 1
    assert mock_s3.deleted.count(collection2_key) == 1


def test_marc_export_reset_s3_failure(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    s3_service_fixture: S3ServiceFixture,
    marc_exporter_fixture: MarcExporterFixture,
    services_fixture: ServicesFixture,
):
    """A failed S3 deletion orphans the object but never leaves the reset half-applied."""
    marc_exporter_fixture.configure_export()
    mock_s3 = s3_service_fixture.mock_service()
    services_fixture.services.storage.public.override(mock_s3)

    library1 = marc_exporter_fixture.library1
    assert library1.id is not None

    marc_exporter_fixture.marc_file()
    marc_exporter_fixture.marc_file(collection=marc_exporter_fixture.collection2)

    with patch.object(
        mock_s3,
        "delete",
        side_effect=ClientError({"Error": {}}, "DeleteObject"),
    ):
        marc.marc_export_reset.delay(library_id=library1.id).wait()

    # All MarcFile records were deleted despite the S3 failures.
    assert db.session.execute(select(MarcFile)).scalars().all() == []
