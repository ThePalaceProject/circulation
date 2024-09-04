import datetime
from functools import partial
from unittest.mock import ANY, call, create_autospec
from uuid import UUID

import pytest
from freezegun import freeze_time

from palace.manager.marc.exporter import LibraryInfo, MarcExporter
from palace.manager.marc.settings import MarcExporterLibrarySettings
from palace.manager.marc.uploader import MarcUploadManager
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.util import create, get_one
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.marc import MarcExporterFixture


class TestMarcExporter:
    def test__s3_key(self, marc_exporter_fixture: MarcExporterFixture) -> None:
        library = marc_exporter_fixture.library1
        collection = marc_exporter_fixture.collection1

        uuid = UUID("c2370bf2-28e1-40ff-9f04-4864306bd11c")
        now = datetime_utc(2024, 8, 27)
        since = datetime_utc(2024, 8, 20)

        s3_key = partial(MarcExporter._s3_key, library, collection, now, uuid)

        assert (
            s3_key()
            == f"marc/{library.short_name}/{collection.name}.full.2024-08-27.wjcL8ijhQP-fBEhkMGvRHA.mrc"
        )

        assert (
            s3_key(since_time=since)
            == f"marc/{library.short_name}/{collection.name}.delta.2024-08-20.2024-08-27.wjcL8ijhQP-fBEhkMGvRHA.mrc"
        )

    @freeze_time("2020-02-20T10:00:00Z")
    @pytest.mark.parametrize(
        "last_updated_time, update_frequency, expected",
        [
            (None, 60, True),
            (None, 1, True),
            (datetime.datetime.fromisoformat("2020-02-20T09:00:00"), 1, False),
            (datetime.datetime.fromisoformat("2020-02-19T10:02:00"), 1, True),
            (datetime.datetime.fromisoformat("2020-01-31T10:02:00"), 20, True),
            (datetime.datetime.fromisoformat("2020-02-01T10:00:00"), 20, False),
        ],
    )
    def test__needs_update(
        self,
        last_updated_time: datetime.datetime,
        update_frequency: int,
        expected: bool,
    ):
        assert (
            MarcExporter._needs_update(last_updated_time, update_frequency) == expected
        )

    def test__web_client_urls(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        library = marc_exporter_fixture.library1
        web_client_urls = partial(MarcExporter._web_client_urls, db.session, library)

        # No web client URLs are returned if there are no discovery service registrations.
        assert web_client_urls() == ()

        # If we pass in a configured web client URL, that URL is returned.
        assert web_client_urls(url="http://web-client") == ("http://web-client",)

        # Add a URL from a library registry.
        registry = db.discovery_service_integration()
        create(
            db.session,
            DiscoveryServiceRegistration,
            library=library,
            integration=registry,
            web_client="http://web-client/registry",
        )
        assert web_client_urls() == ("http://web-client/registry",)

        # URL from library registry and configured URL are both returned.
        assert web_client_urls(url="http://web-client") == (
            "http://web-client/registry",
            "http://web-client",
        )

    def test__enabled_collections_and_libraries(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ) -> None:
        enabled_collections_and_libraries = partial(
            MarcExporter._enabled_collections_and_libraries,
            db.session,
            marc_exporter_fixture.registry,
        )

        assert enabled_collections_and_libraries() == set()

        # Marc export is enabled on the collections, but since the libraries don't have a marc exporter, they are
        # not included.
        marc_exporter_fixture.collection1.export_marc_records = True
        marc_exporter_fixture.collection2.export_marc_records = True
        assert enabled_collections_and_libraries() == set()

        # Marc export is enabled, but no libraries are added to it
        marc_integration = marc_exporter_fixture.integration()
        assert enabled_collections_and_libraries() == set()

        # Add a marc exporter to library1
        marc_l1_config = db.integration_library_configuration(
            marc_integration, marc_exporter_fixture.library1
        )
        assert enabled_collections_and_libraries() == {
            (marc_exporter_fixture.collection1, marc_l1_config),
            (marc_exporter_fixture.collection2, marc_l1_config),
        }

        # Add a marc exporter to library2
        marc_l2_config = db.integration_library_configuration(
            marc_integration, marc_exporter_fixture.library2
        )
        assert enabled_collections_and_libraries() == {
            (marc_exporter_fixture.collection1, marc_l1_config),
            (marc_exporter_fixture.collection1, marc_l2_config),
            (marc_exporter_fixture.collection2, marc_l1_config),
        }

        # Enable marc export on collection3
        marc_exporter_fixture.collection3.export_marc_records = True
        assert enabled_collections_and_libraries() == {
            (marc_exporter_fixture.collection1, marc_l1_config),
            (marc_exporter_fixture.collection1, marc_l2_config),
            (marc_exporter_fixture.collection2, marc_l1_config),
            (marc_exporter_fixture.collection3, marc_l2_config),
        }

        # We can also filter by a collection id
        assert enabled_collections_and_libraries(
            collection_id=marc_exporter_fixture.collection1.id
        ) == {
            (marc_exporter_fixture.collection1, marc_l1_config),
            (marc_exporter_fixture.collection1, marc_l2_config),
        }

    def test__last_updated(self, marc_exporter_fixture: MarcExporterFixture) -> None:
        library = marc_exporter_fixture.library1
        collection = marc_exporter_fixture.collection1

        last_updated = partial(
            MarcExporter._last_updated,
            marc_exporter_fixture.session,
            library,
            collection,
        )

        # If there is no cached file, we return None.
        assert last_updated() is None

        # If there is a cached file, we return the time it was created.
        file1 = MarcFile(
            library=library,
            collection=collection,
            created=datetime_utc(1984, 5, 8),
            key="file1",
        )
        marc_exporter_fixture.session.add(file1)
        assert last_updated() == file1.created

        # If there are multiple cached files, we return the time of the most recent one.
        file2 = MarcFile(
            library=library,
            collection=collection,
            created=utc_now(),
            key="file2",
        )
        marc_exporter_fixture.session.add(file2)
        assert last_updated() == file2.created

    def test_enabled_collections(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        enabled_collections = partial(
            MarcExporter.enabled_collections,
            db.session,
            marc_exporter_fixture.registry,
        )

        assert enabled_collections() == set()

        # Marc export is enabled on the collections, but since the libraries don't have a marc exporter, they are
        # not included.
        marc_exporter_fixture.collection1.export_marc_records = True
        marc_exporter_fixture.collection2.export_marc_records = True
        assert enabled_collections() == set()

        # Marc export is enabled, but no libraries are added to it
        marc_integration = marc_exporter_fixture.integration()
        assert enabled_collections() == set()

        # Add a marc exporter to library2
        db.integration_library_configuration(
            marc_integration, marc_exporter_fixture.library2
        )
        assert enabled_collections() == {marc_exporter_fixture.collection1}

        # Enable marc export on collection3
        marc_exporter_fixture.collection3.export_marc_records = True
        assert enabled_collections() == {
            marc_exporter_fixture.collection1,
            marc_exporter_fixture.collection3,
        }

    def test_enabled_libraries(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ):
        assert marc_exporter_fixture.collection1.id is not None
        enabled_libraries = partial(
            MarcExporter.enabled_libraries,
            db.session,
            marc_exporter_fixture.registry,
            collection_id=marc_exporter_fixture.collection1.id,
        )

        assert enabled_libraries() == []

        # Collections have marc export enabled, and the marc exporter integration is setup, but
        # no libraries are configured to use it.
        marc_exporter_fixture.collection1.export_marc_records = True
        marc_exporter_fixture.collection2.export_marc_records = True
        marc_integration = marc_exporter_fixture.integration()
        assert enabled_libraries() == []

        # Add a marc exporter to library2
        db.integration_library_configuration(
            marc_integration,
            marc_exporter_fixture.library2,
            MarcExporterLibrarySettings(
                organization_code="org", web_client_url="http://web-client"
            ),
        )
        [library_2_info] = enabled_libraries()

        def assert_library_2(library_info: LibraryInfo) -> None:
            assert library_info.library_id == marc_exporter_fixture.library2.id
            assert (
                library_info.library_short_name
                == marc_exporter_fixture.library2.short_name
            )
            assert library_info.last_updated is None
            assert library_info.needs_update
            assert library_info.organization_code == "org"
            assert library_info.include_summary is False
            assert library_info.include_genres is False
            assert library_info.web_client_urls == ("http://web-client",)
            assert library_info.s3_key_full.startswith("marc/library2/collection1.full")
            assert library_info.s3_key_delta is None

        assert_library_2(library_2_info)

        # Add a marc exporter to library1
        db.integration_library_configuration(
            marc_integration,
            marc_exporter_fixture.library1,
            MarcExporterLibrarySettings(
                organization_code="org2", include_summary=True, include_genres=True
            ),
        )
        [library_1_info, library_2_info] = enabled_libraries()
        assert_library_2(library_2_info)

        assert library_1_info.library_id == marc_exporter_fixture.library1.id
        assert (
            library_1_info.library_short_name
            == marc_exporter_fixture.library1.short_name
        )
        assert library_1_info.last_updated is None
        assert library_1_info.needs_update
        assert library_1_info.organization_code == "org2"
        assert library_1_info.include_summary is True
        assert library_1_info.include_genres is True
        assert library_1_info.web_client_urls == ()
        assert library_1_info.s3_key_full.startswith("marc/library1/collection1.full")
        assert library_1_info.s3_key_delta is None

    def test_query_works(self, marc_exporter_fixture: MarcExporterFixture) -> None:
        assert marc_exporter_fixture.collection1.id is not None
        query_works = partial(
            MarcExporter.query_works,
            marc_exporter_fixture.session,
            collection_id=marc_exporter_fixture.collection1.id,
            work_id_offset=None,
            batch_size=3,
        )

        assert query_works() == []

        works = marc_exporter_fixture.works()

        assert query_works() == works[:3]
        assert query_works(work_id_offset=works[3].id) == works[4:]

    def test_collection(self, marc_exporter_fixture: MarcExporterFixture) -> None:
        collection_id = marc_exporter_fixture.collection1.id
        assert collection_id is not None
        collection = MarcExporter.collection(
            marc_exporter_fixture.session, collection_id
        )
        assert collection == marc_exporter_fixture.collection1

        marc_exporter_fixture.session.delete(collection)
        collection = MarcExporter.collection(
            marc_exporter_fixture.session, collection_id
        )
        assert collection is None

    def test_process_work(self, marc_exporter_fixture: MarcExporterFixture) -> None:
        marc_exporter_fixture.configure_export()

        collection = marc_exporter_fixture.collection1
        work = marc_exporter_fixture.work(collection)
        enabled_libraries = marc_exporter_fixture.enabled_libraries(collection)

        mock_upload_manager = create_autospec(MarcUploadManager)

        process_work = partial(
            MarcExporter.process_work,
            work,
            enabled_libraries,
            "http://base.url",
            upload_manager=mock_upload_manager,
        )

        process_work()
        mock_upload_manager.add_record.assert_has_calls(
            [
                call(enabled_libraries[0].s3_key_full, ANY),
                call(enabled_libraries[0].s3_key_delta, ANY),
                call(enabled_libraries[1].s3_key_full, ANY),
            ]
        )

        # If the work has no license pools, it is skipped.
        mock_upload_manager.reset_mock()
        work.license_pools = []
        process_work()
        mock_upload_manager.add_record.assert_not_called()

    def test_create_marc_upload_records(
        self, marc_exporter_fixture: MarcExporterFixture
    ) -> None:
        marc_exporter_fixture.configure_export()

        collection = marc_exporter_fixture.collection1
        assert collection.id is not None
        enabled_libraries = marc_exporter_fixture.enabled_libraries(collection)

        marc_exporter_fixture.session.query(MarcFile).delete()

        start_time = utc_now()

        # If there are no uploads, then no records are created.
        MarcExporter.create_marc_upload_records(
            marc_exporter_fixture.session,
            start_time,
            collection.id,
            enabled_libraries,
            set(),
        )

        assert len(marc_exporter_fixture.session.query(MarcFile).all()) == 0

        # If there are uploads, then records are created.
        assert enabled_libraries[0].s3_key_delta is not None
        MarcExporter.create_marc_upload_records(
            marc_exporter_fixture.session,
            start_time,
            collection.id,
            enabled_libraries,
            {
                enabled_libraries[0].s3_key_full,
                enabled_libraries[1].s3_key_full,
                enabled_libraries[0].s3_key_delta,
            },
        )

        assert len(marc_exporter_fixture.session.query(MarcFile).all()) == 3

        assert get_one(
            marc_exporter_fixture.session,
            MarcFile,
            collection=collection,
            library_id=enabled_libraries[0].library_id,
            key=enabled_libraries[0].s3_key_full,
        )

        assert get_one(
            marc_exporter_fixture.session,
            MarcFile,
            collection=collection,
            library_id=enabled_libraries[1].library_id,
            key=enabled_libraries[1].s3_key_full,
        )

        assert get_one(
            marc_exporter_fixture.session,
            MarcFile,
            collection=collection,
            library_id=enabled_libraries[0].library_id,
            key=enabled_libraries[0].s3_key_delta,
            since=enabled_libraries[0].last_updated,
        )
