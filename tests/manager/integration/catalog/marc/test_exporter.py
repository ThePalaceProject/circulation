import datetime
from functools import partial

import pytest
from freezegun import freeze_time
from sqlalchemy.exc import InvalidRequestError

from palace.manager.core.classifier import Classifier
from palace.manager.integration.catalog.marc.exporter import LibraryInfo, MarcExporter
from palace.manager.integration.catalog.marc.settings import MarcExporterLibrarySettings
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.marc import MarcExporterFixture


class TestMarcExporter:
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

        assert enabled_libraries(collection_id=None) == []
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

    def test_query_works(self, marc_exporter_fixture: MarcExporterFixture) -> None:
        assert marc_exporter_fixture.collection1.id is not None
        query_works = partial(
            MarcExporter.query_works,
            marc_exporter_fixture.session,
            collection_id=marc_exporter_fixture.collection1.id,
            work_id_offset=None,
            batch_size=3,
        )

        assert query_works(collection_id=None) == []
        assert query_works() == []

        works = marc_exporter_fixture.works()

        result = query_works()
        assert result == works[:3]

        # Make sure the loader options are correctly set on the results, this will cause an InvalidRequestError
        # to be raised on any attribute access that doesn't have a loader setup. LicensePool.loans is an example
        # of an unconfigured attribute.
        with pytest.raises(
            InvalidRequestError,
            match="'LicensePool.loans' is not available due to lazy='raise'",
        ):
            _ = result[0].license_pools[0].loans

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
        marc_exporter_fixture.marc_file(
            library=marc_exporter_fixture.library1,
            created=utc_now() - datetime.timedelta(days=14),
        )

        collection = marc_exporter_fixture.collection1
        work = marc_exporter_fixture.work(collection)
        pool = work.license_pools[0]
        enabled_libraries = marc_exporter_fixture.enabled_libraries(collection)

        process_work = partial(
            MarcExporter.process_work,
            work,
            pool,
            None,
            enabled_libraries,
            "http://base.url",
        )

        # We get both libraries included in a full record
        processed_works = process_work(False)
        assert list(processed_works.keys()) == enabled_libraries

        # But we only get library1 in a delta record, since this is the first full marc export
        # for library2, so there is no timestamp to create a delta record against.
        [processed_work] = process_work(True).keys()
        assert processed_work.library_id == marc_exporter_fixture.library1.id

    def test_files_for_cleanup_deleted_disabled(
        self, marc_exporter_fixture: MarcExporterFixture
    ) -> None:
        marc_exporter_fixture.configure_export()
        files_for_cleanup = partial(
            MarcExporter.files_for_cleanup,
            marc_exporter_fixture.session,
            marc_exporter_fixture.registry,
        )

        # If there are no files, then no files are returned.
        assert set(files_for_cleanup()) == set()

        # Files created for libraries or collections that have been deleted are returned.
        collection1_library1 = marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection1,
            library=marc_exporter_fixture.library1,
        )
        collection1_library2 = marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection1,
            library=marc_exporter_fixture.library2,
        )
        collection2_library1 = marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection2,
            library=marc_exporter_fixture.library1,
        )
        collection3_library2 = marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection3,
            library=marc_exporter_fixture.library2,
        )
        deleted_collection = marc_exporter_fixture.marc_file(collection=None)
        deleted_library = marc_exporter_fixture.marc_file(library=None)

        assert set(files_for_cleanup()) == {deleted_collection, deleted_library}

        # If a collection has export_marc_records set to False, then the files for that collection are returned.
        marc_exporter_fixture.collection1.export_marc_records = False
        assert set(files_for_cleanup()) == {
            deleted_collection,
            deleted_library,
            collection1_library1,
            collection1_library2,
        }

        # If a library has its marc exporter integration disabled, then the files for that library are returned.
        library2_marc_integration = marc_exporter_fixture.integration().for_library(
            marc_exporter_fixture.library2
        )
        assert library2_marc_integration is not None
        marc_exporter_fixture.session.delete(library2_marc_integration)
        assert set(files_for_cleanup()) == {
            deleted_collection,
            deleted_library,
            collection1_library1,
            collection1_library2,
            collection3_library2,
        }

    def test_files_for_cleanup_outdated_full(
        self, marc_exporter_fixture: MarcExporterFixture
    ) -> None:
        marc_exporter_fixture.configure_export()
        files_for_cleanup = partial(
            MarcExporter.files_for_cleanup,
            marc_exporter_fixture.session,
            marc_exporter_fixture.registry,
        )

        # Only a single full file is needed, the most recent, all other files are returned.
        decoy = marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection2,
            created=utc_now() - datetime.timedelta(days=15),
        )
        newest = marc_exporter_fixture.marc_file(created=utc_now())
        outdated = {
            marc_exporter_fixture.marc_file(
                created=utc_now() - datetime.timedelta(days=d + 1)
            )
            for d in range(5)
        }
        assert set(files_for_cleanup()) == outdated

    def test_files_for_cleanup_outdated_delta(
        self, marc_exporter_fixture: MarcExporterFixture
    ) -> None:
        marc_exporter_fixture.configure_export()
        files_for_cleanup = partial(
            MarcExporter.files_for_cleanup,
            marc_exporter_fixture.session,
            marc_exporter_fixture.registry,
        )

        # The most recent 12 delta files are kept, all others are returned
        last_week = utc_now() - datetime.timedelta(days=7)
        decoy = marc_exporter_fixture.marc_file(
            collection=marc_exporter_fixture.collection2,
            created=utc_now() - datetime.timedelta(days=15),
            since=last_week - datetime.timedelta(days=15),
        )
        kept = {
            marc_exporter_fixture.marc_file(created=utc_now(), since=last_week)
            for _ in range(12)
        }
        outdated = {
            marc_exporter_fixture.marc_file(
                created=last_week - datetime.timedelta(days=d),
                since=last_week - datetime.timedelta(days=d + 1),
            )
            for d in range(20)
        }
        assert set(files_for_cleanup()) == outdated

    def test_process_work_with_filtering(
        self, marc_exporter_fixture: MarcExporterFixture
    ) -> None:
        """Test that process_work excludes works based on library content filtering."""
        marc_exporter_fixture.configure_export()

        collection = marc_exporter_fixture.collection1
        work = marc_exporter_fixture.work(collection)
        work.audience = Classifier.AUDIENCE_ADULT
        pool = work.license_pools[0]

        # Get enabled libraries - these won't have filtering configured yet
        enabled_libraries = list(marc_exporter_fixture.enabled_libraries(collection))
        assert len(enabled_libraries) == 2

        # Process work without filtering - both libraries should get the work
        processed_works = MarcExporter.process_work(
            work, pool, None, enabled_libraries, "http://base.url", False
        )
        assert len(processed_works) == 2

        # Now create library info with filtering for one library
        library1_info = enabled_libraries[0]
        library2_info = enabled_libraries[1]

        # Create a filtered version of library1 that filters Adult content
        library1_filtered = LibraryInfo(
            library_id=library1_info.library_id,
            library_short_name=library1_info.library_short_name,
            last_updated=library1_info.last_updated,
            needs_update=library1_info.needs_update,
            organization_code=library1_info.organization_code,
            include_summary=library1_info.include_summary,
            include_genres=library1_info.include_genres,
            web_client_urls=library1_info.web_client_urls,
            filtered_audiences=("Adult",),
            filtered_genres=(),
        )

        # Process work with one filtered library - only unfiltered library should get the work
        processed_works = MarcExporter.process_work(
            work,
            pool,
            None,
            [library1_filtered, library2_info],
            "http://base.url",
            False,
        )
        assert len(processed_works) == 1
        assert list(processed_works.keys())[0].library_id == library2_info.library_id

        # If both libraries filter the work, no records should be generated
        library2_filtered = LibraryInfo(
            library_id=library2_info.library_id,
            library_short_name=library2_info.library_short_name,
            last_updated=library2_info.last_updated,
            needs_update=library2_info.needs_update,
            organization_code=library2_info.organization_code,
            include_summary=library2_info.include_summary,
            include_genres=library2_info.include_genres,
            web_client_urls=library2_info.web_client_urls,
            filtered_audiences=("Adult",),
            filtered_genres=(),
        )
        processed_works = MarcExporter.process_work(
            work,
            pool,
            None,
            [library1_filtered, library2_filtered],
            "http://base.url",
            False,
        )
        assert len(processed_works) == 0

    def test_enabled_libraries_includes_filtering_settings(
        self,
        db: DatabaseTransactionFixture,
        marc_exporter_fixture: MarcExporterFixture,
    ) -> None:
        """Test that enabled_libraries includes the library's content filtering settings."""
        marc_exporter_fixture.configure_export()

        # Set up filtering on library1
        marc_exporter_fixture.library1.settings_dict["filtered_audiences"] = ["Adult"]
        marc_exporter_fixture.library1.settings_dict["filtered_genres"] = [
            "Romance",
            "Horror",
        ]
        if hasattr(marc_exporter_fixture.library1, "_settings"):
            delattr(marc_exporter_fixture.library1, "_settings")

        enabled_libraries = marc_exporter_fixture.enabled_libraries(
            marc_exporter_fixture.collection1
        )

        # Find library1 in the results
        library1_info = next(
            lib
            for lib in enabled_libraries
            if lib.library_id == marc_exporter_fixture.library1.id
        )
        library2_info = next(
            lib
            for lib in enabled_libraries
            if lib.library_id == marc_exporter_fixture.library2.id
        )

        # Library1 should have filtering settings
        assert library1_info.filtered_audiences == ("Adult",)
        assert library1_info.filtered_genres == ("Romance", "Horror")

        # Library2 should have no filtering
        assert library2_info.filtered_audiences == ()
        assert library2_info.filtered_genres == ()
