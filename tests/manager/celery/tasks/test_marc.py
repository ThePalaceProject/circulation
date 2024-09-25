import datetime
from typing import Any
from unittest.mock import ANY, call, patch

import pytest
from pymarc import MARCReader
from sqlalchemy import select

from palace.manager.celery.tasks import marc
from palace.manager.marc.exporter import MarcExporter
from palace.manager.marc.uploader import MarcUploadManager
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.service.redis.models.marc import (
    MarcFileUploadSession,
    MarcFileUploadSessionError,
    MarcFileUploadState,
)
from palace.manager.sqlalchemy.model.collection import Collection
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.datetime_helpers import utc_now
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
            marc_export_collection.delay.assert_has_calls(
                [
                    call(collection_id=collection.id, start_time=ANY, libraries=ANY)
                    for collection in collections
                ],
                any_order=True,
            )

    def test_skip_collections(
        self,
        db: DatabaseTransactionFixture,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        celery_fixture: CeleryFixture,
    ):
        marc_exporter_fixture.configure_export()
        collections = [
            marc_exporter_fixture.collection1,
            marc_exporter_fixture.collection2,
            marc_exporter_fixture.collection3,
        ]
        for collection in collections:
            marc_exporter_fixture.work(collection)
        with patch.object(marc, "marc_export_collection") as marc_export_collection:
            # Collection 1 should be skipped because it is locked
            assert marc_exporter_fixture.collection1.id is not None
            MarcFileUploadSession(
                redis_fixture.client, marc_exporter_fixture.collection1.id
            ).acquire()

            # Collection 2 should be skipped because it was updated recently
            marc_exporter_fixture.marc_file(
                collection=marc_exporter_fixture.collection2
            )

            # Collection 3 should be skipped because its state is not INITIAL
            assert marc_exporter_fixture.collection3.id is not None
            upload_session = MarcFileUploadSession(
                redis_fixture.client, marc_exporter_fixture.collection3.id
            )
            with upload_session.lock() as acquired:
                assert acquired
                upload_session.set_state(MarcFileUploadState.QUEUED)

            marc.marc_export.delay().wait()
            marc_export_collection.delay.assert_not_called()


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
        # We need to ignore the test-file-1.mrc file, which is created by our call to configure_export.
        return [
            f
            for f in self.db.session.execute(select(MarcFile)).scalars().all()
            if f.key != self.marc_exporter_fixture.test_marc_file_key
        ]

    def redis_data(self, collection: Collection) -> dict[str, Any] | None:
        assert collection.id is not None
        uploads = MarcFileUploadSession(self.redis_fixture.client, collection.id)
        return self.redis_fixture.client.json().get(uploads.key)

    def setup_minio_storage(self) -> None:
        self.services_fixture.services.storage.override(
            self.s3_service_integration_fixture.container
        )

    def setup_mock_storage(self) -> None:
        self.services_fixture.services.storage.public.override(self.mock_s3)

    def works(self, collection: Collection) -> list[Work]:
        return [self.marc_exporter_fixture.work(collection) for _ in range(15)]

    def export_collection(self, collection: Collection) -> None:
        service = self.services_fixture.services.integration_registry.catalog_services()
        assert collection.id is not None
        info = MarcExporter.enabled_libraries(self.db.session, service, collection.id)
        libraries = [l.model_dump() for l in info]
        marc.marc_export_collection.delay(
            collection.id, batch_size=5, start_time=self.start_time, libraries=libraries
        ).wait()


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
        work_uris = [
            work.license_pools[0].identifier.urn
            for work in marc_export_collection_fixture.works(collection)
        ]

        # Run the full end-to-end process for exporting a collection, this should generate
        # 3 batches of 5 works each, putting the results into minio.
        marc_export_collection_fixture.export_collection(collection)

        # Verify that we didn't leave anything in the redis cache.
        assert marc_export_collection_fixture.redis_data(collection) is None

        # Verify that the expected number of files were uploaded to minio.
        uploaded_files = s3_service_integration_fixture.list_objects("public")
        assert len(uploaded_files) == 3

        # Verify that the expected number of marc files were created in the database.
        marc_files = marc_export_collection_fixture.marc_files()
        assert len(marc_files) == 3
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
            expected_status = "c" if "delta" in file else "n"
            assert all(
                record.leader.record_status == expected_status for record in records
            )

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
        assert marc_export_collection_fixture.redis_data(collection) is None

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

        # After the exception, we should have aborted the multipart uploads and deleted the redis data.
        assert marc_export_collection_fixture.marc_files() == []
        assert marc_export_collection_fixture.redis_data(collection) is None
        assert len(marc_export_collection_fixture.mock_s3.aborted) == 3

    def test_locked(
        self,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.info)
        collection = marc_exporter_fixture.collection1
        assert collection.id is not None
        MarcFileUploadSession(redis_fixture.client, collection.id).acquire()
        marc_export_collection_fixture.setup_mock_storage()
        with patch.object(MarcExporter, "query_works") as query:
            marc_export_collection_fixture.export_collection(collection)
        query.assert_not_called()
        assert "another task is already processing it" in caplog.text

    def test_outdated_task_run(
        self,
        redis_fixture: RedisFixture,
        marc_exporter_fixture: MarcExporterFixture,
        marc_export_collection_fixture: MarcExportCollectionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # In the case that an old task is run again for some reason, it should
        # detect that its update number is incorrect and exit.
        caplog.set_level(LogLevel.info)
        collection = marc_exporter_fixture.collection1
        marc_export_collection_fixture.setup_mock_storage()
        assert collection.id is not None

        # Acquire the lock and start an upload, this simulates another task having done work
        # that the current task doesn't know about.
        uploads = MarcFileUploadSession(redis_fixture.client, collection.id)
        with uploads.lock() as locked:
            assert locked
            uploads.append_buffers({"test": "data"})

        with pytest.raises(MarcFileUploadSessionError, match="Update number mismatch"):
            marc_export_collection_fixture.export_collection(collection)

        assert marc_export_collection_fixture.marc_files() == []
        assert marc_export_collection_fixture.redis_data(collection) is None


def test_marc_export_cleanup(
    db: DatabaseTransactionFixture,
    celery_fixture: CeleryFixture,
    s3_service_fixture: S3ServiceFixture,
    marc_exporter_fixture: MarcExporterFixture,
    services_fixture: ServicesFixture,
):
    marc_exporter_fixture.configure_export(marc_file=False)
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
