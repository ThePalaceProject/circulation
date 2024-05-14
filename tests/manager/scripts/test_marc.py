from __future__ import annotations

import datetime
import logging
from unittest.mock import MagicMock, call, create_autospec

import pytest
from _pytest.logging import LogCaptureFixture
from sqlalchemy.exc import NoResultFound

from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.marc import (
    MARCExporter,
    MarcExporterLibrarySettings,
    MarcExporterSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.scripts.marc import CacheMARCFiles
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.util import create
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.database import (
    DatabaseTransactionFixture,
    IntegrationConfigurationFixture,
)
from tests.fixtures.services import ServicesFixture


class CacheMARCFilesFixture:
    def __init__(
        self, db: DatabaseTransactionFixture, services_fixture: ServicesFixture
    ):
        self.db = db
        self.services_fixture = services_fixture
        self.base_url = "http://test-circulation-manager"
        services_fixture.set_base_url(self.base_url)
        self.exporter = MagicMock(spec=MARCExporter)
        self.library = self.db.default_library()
        self.collection = self.db.collection()
        self.collection.export_marc_records = True
        self.collection.libraries += [self.library]

    def integration(self, library: Library | None = None) -> IntegrationConfiguration:
        if library is None:
            library = self.library

        return self.db.integration_configuration(
            protocol=MARCExporter.__name__,
            goal=Goals.CATALOG_GOAL,
            libraries=[library],
        )

    def script(self, cmd_args: list[str] | None = None) -> CacheMARCFiles:
        cmd_args = cmd_args or []
        return CacheMARCFiles(
            self.db.session,
            exporter=self.exporter,
            services=self.services_fixture.services,
            cmd_args=cmd_args,
        )


@pytest.fixture
def cache_marc_files(
    db: DatabaseTransactionFixture, services_fixture: ServicesFixture
) -> CacheMARCFilesFixture:
    return CacheMARCFilesFixture(db, services_fixture)


class TestCacheMARCFiles:
    def test_constructor(self, cache_marc_files: CacheMARCFilesFixture):
        cache_marc_files.services_fixture.set_base_url(None)
        with pytest.raises(CannotLoadConfiguration):
            cache_marc_files.script()

        cache_marc_files.services_fixture.set_base_url("http://test.com")
        script = cache_marc_files.script()
        assert script.base_url == "http://test.com"

    def test_settings(self, cache_marc_files: CacheMARCFilesFixture):
        # Test that the script gets the correct settings.
        test_library = cache_marc_files.library
        other_library = cache_marc_files.db.library()

        expected_settings = MarcExporterSettings(update_frequency=3)
        expected_library_settings = MarcExporterLibrarySettings(
            organization_code="test",
            include_summary=True,
            include_genres=True,
        )

        other_library_settings = MarcExporterLibrarySettings(
            organization_code="other",
        )

        integration = cache_marc_files.integration(test_library)
        integration.libraries += [other_library]

        test_library_integration = integration.for_library(test_library)
        assert test_library_integration is not None
        other_library_integration = integration.for_library(other_library)
        assert other_library_integration is not None
        MARCExporter.settings_update(integration, expected_settings)
        MARCExporter.library_settings_update(
            test_library_integration, expected_library_settings
        )
        MARCExporter.library_settings_update(
            other_library_integration, other_library_settings
        )

        script = cache_marc_files.script()
        actual_settings, actual_library_settings = script.settings(test_library)

        assert actual_settings == expected_settings
        assert actual_library_settings == expected_library_settings

    def test_settings_none(self, cache_marc_files: CacheMARCFilesFixture):
        # If there are no settings, the setting function raises an exception.
        test_library = cache_marc_files.library
        script = cache_marc_files.script()
        with pytest.raises(NoResultFound):
            script.settings(test_library)

    def test_process_libraries_no_storage(
        self, cache_marc_files: CacheMARCFilesFixture, caplog: LogCaptureFixture
    ):
        # If there is no storage integration, the script logs an error and returns.
        script = cache_marc_files.script()
        script.storage_service = None
        caplog.set_level(logging.INFO)
        script.process_libraries([MagicMock(), MagicMock()])
        assert "No storage service was found" in caplog.text

    def test_get_collections(self, cache_marc_files: CacheMARCFilesFixture):
        # Test that the script gets the correct collections.
        test_library = cache_marc_files.library
        collection1 = cache_marc_files.collection

        # Second collection is configured to export MARC records.
        collection2 = cache_marc_files.db.collection()
        collection2.export_marc_records = True
        collection2.libraries += [test_library]

        # Third collection is not configured to export MARC records.
        collection3 = cache_marc_files.db.collection()
        collection3.export_marc_records = False
        collection3.libraries += [test_library]

        # Fourth collection is configured to export MARC records, but is
        # configured to export only to a different library.
        other_library = cache_marc_files.db.library()
        other_collection = cache_marc_files.db.collection()
        other_collection.export_marc_records = True
        other_collection.libraries += [other_library]

        script = cache_marc_files.script()

        # We should get back the two collections that are configured to export
        # MARC records to this library.
        collections = script.get_collections(test_library)
        assert set(collections) == {collection1, collection2}

        # Set collection3 to export MARC records to this library.
        collection3.export_marc_records = True

        # We should get back all three collections that are configured to export
        # MARC records to this library.
        collections = script.get_collections(test_library)
        assert set(collections) == {collection1, collection2, collection3}

    def test_get_web_client_urls(
        self,
        db: DatabaseTransactionFixture,
        cache_marc_files: CacheMARCFilesFixture,
        create_integration_configuration: IntegrationConfigurationFixture,
    ):
        # No web client URLs are returned if there are no discovery service registrations.
        script = cache_marc_files.script()
        assert script.get_web_client_urls(cache_marc_files.library) == []

        # If we pass in a configured web client URL, that URL is returned.
        assert script.get_web_client_urls(
            cache_marc_files.library, "http://web-client"
        ) == ["http://web-client"]

        # Add a URL from a library registry.
        registry = create_integration_configuration.discovery_service()
        create(
            db.session,
            DiscoveryServiceRegistration,
            library=cache_marc_files.library,
            integration=registry,
            web_client="http://web-client-url/",
        )
        assert script.get_web_client_urls(cache_marc_files.library) == [
            "http://web-client-url/"
        ]

        # URL from library registry and configured URL are both returned.
        assert script.get_web_client_urls(
            cache_marc_files.library, "http://web-client"
        ) == [
            "http://web-client-url/",
            "http://web-client",
        ]

    def test_process_library_not_configured(
        self,
        cache_marc_files: CacheMARCFilesFixture,
    ):
        script = cache_marc_files.script()
        mock_process_collection = create_autospec(script.process_collection)
        script.process_collection = mock_process_collection
        mock_settings = create_autospec(script.settings)
        script.settings = mock_settings
        mock_settings.side_effect = NoResultFound

        # If there is no integration configuration for the library, the script
        # does nothing.
        script.process_library(cache_marc_files.library)
        mock_process_collection.assert_not_called()

    def test_process_library(self, cache_marc_files: CacheMARCFilesFixture):
        script = cache_marc_files.script()
        mock_annotator_cls = MagicMock()
        mock_process_collection = create_autospec(script.process_collection)
        script.process_collection = mock_process_collection
        mock_settings = create_autospec(script.settings)
        script.settings = mock_settings
        settings = MarcExporterSettings(update_frequency=3)
        library_settings = MarcExporterLibrarySettings(
            organization_code="test",
            web_client_url="http://web-client-url/",
            include_summary=True,
            include_genres=False,
        )
        mock_settings.return_value = (
            settings,
            library_settings,
        )

        before_call_time = utc_now()

        # If there is an integration configuration for the library, the script
        # processes all the collections for that library.
        script.process_library(
            cache_marc_files.library, annotator_cls=mock_annotator_cls
        )

        after_call_time = utc_now()

        mock_annotator_cls.assert_called_once_with(
            cache_marc_files.base_url,
            cache_marc_files.library.short_name,
            [library_settings.web_client_url],
            library_settings.organization_code,
            library_settings.include_summary,
            library_settings.include_genres,
        )

        assert mock_process_collection.call_count == 1
        (
            library,
            collection,
            annotator,
            update_frequency,
            creation_time,
        ) = mock_process_collection.call_args.args
        assert library == cache_marc_files.library
        assert collection == cache_marc_files.collection
        assert annotator == mock_annotator_cls.return_value
        assert update_frequency == settings.update_frequency
        assert creation_time > before_call_time
        assert creation_time < after_call_time

    def test_last_updated(
        self, db: DatabaseTransactionFixture, cache_marc_files: CacheMARCFilesFixture
    ):
        script = cache_marc_files.script()

        # If there is no cached file, we return None.
        assert (
            script.last_updated(cache_marc_files.library, cache_marc_files.collection)
            is None
        )

        # If there is a cached file, we return the time it was created.
        file1 = MarcFile(
            library=cache_marc_files.library,
            collection=cache_marc_files.collection,
            created=datetime_utc(1984, 5, 8),
            key="file1",
        )
        db.session.add(file1)
        assert (
            script.last_updated(cache_marc_files.library, cache_marc_files.collection)
            == file1.created
        )

        # If there are multiple cached files, we return the time of the most recent one.
        file2 = MarcFile(
            library=cache_marc_files.library,
            collection=cache_marc_files.collection,
            created=utc_now(),
            key="file2",
        )
        db.session.add(file2)
        assert (
            script.last_updated(cache_marc_files.library, cache_marc_files.collection)
            == file2.created
        )

    def test_force(self, cache_marc_files: CacheMARCFilesFixture):
        script = cache_marc_files.script()
        assert script.force is False

        script = cache_marc_files.script(cmd_args=["--force"])
        assert script.force is True

    @pytest.mark.parametrize(
        "last_updated, force, update_frequency, run_exporter",
        [
            pytest.param(None, False, 10, True, id="never_run_before"),
            pytest.param(None, False, 10, True, id="never_run_before_w_force"),
            pytest.param(
                utc_now() - datetime.timedelta(days=5),
                False,
                10,
                False,
                id="recently_run",
            ),
            pytest.param(
                utc_now() - datetime.timedelta(days=5),
                True,
                10,
                True,
                id="recently_run_w_force",
            ),
            pytest.param(
                utc_now() - datetime.timedelta(days=5),
                False,
                0,
                True,
                id="recently_run_w_frequency_0",
            ),
            pytest.param(
                utc_now() - datetime.timedelta(days=15),
                False,
                10,
                True,
                id="not_recently_run",
            ),
            pytest.param(
                utc_now() - datetime.timedelta(days=15),
                True,
                10,
                True,
                id="not_recently_run_w_force",
            ),
            pytest.param(
                utc_now() - datetime.timedelta(days=15),
                False,
                0,
                True,
                id="not_recently_run_w_frequency_0",
            ),
        ],
    )
    def test_process_collection_skip(
        self,
        cache_marc_files: CacheMARCFilesFixture,
        caplog: LogCaptureFixture,
        last_updated: datetime.datetime | None,
        force: bool,
        update_frequency: int,
        run_exporter: bool,
    ):
        script = cache_marc_files.script()
        script.exporter = MagicMock()
        now = utc_now()
        caplog.set_level(logging.INFO)

        script.force = force
        script.last_updated = MagicMock(return_value=last_updated)
        script.process_collection(
            cache_marc_files.library,
            cache_marc_files.collection,
            MagicMock(),
            update_frequency,
            now,
        )

        if run_exporter:
            assert script.exporter.records.call_count > 0
            assert "Processed collection" in caplog.text
        else:
            assert script.exporter.records.call_count == 0
            assert "Skipping collection" in caplog.text

    def test_process_collection_never_called(
        self, cache_marc_files: CacheMARCFilesFixture, caplog: LogCaptureFixture
    ):
        # If the collection has not been processed before, the script processes
        # the collection and created a full export.
        caplog.set_level(logging.INFO)
        script = cache_marc_files.script()
        mock_exporter = MagicMock(spec=MARCExporter)
        script.exporter = mock_exporter
        script.last_updated = MagicMock(return_value=None)
        mock_annotator = MagicMock()
        creation_time = utc_now()
        script.process_collection(
            cache_marc_files.library,
            cache_marc_files.collection,
            mock_annotator,
            10,
            creation_time,
        )
        mock_exporter.records.assert_called_once_with(
            cache_marc_files.library,
            cache_marc_files.collection,
            mock_annotator,
            creation_time=creation_time,
        )
        assert "Processed collection" in caplog.text

    def test_process_collection_with_last_updated(
        self, cache_marc_files: CacheMARCFilesFixture, caplog: LogCaptureFixture
    ):
        # If the collection has been processed before, the script processes
        # the collection, created a full export and a delta export.
        caplog.set_level(logging.INFO)
        script = cache_marc_files.script()
        mock_exporter = MagicMock(spec=MARCExporter)
        script.exporter = mock_exporter
        last_updated = utc_now() - datetime.timedelta(days=20)
        script.last_updated = MagicMock(return_value=last_updated)
        mock_annotator = MagicMock()
        creation_time = utc_now()
        script.process_collection(
            cache_marc_files.library,
            cache_marc_files.collection,
            mock_annotator,
            10,
            creation_time,
        )
        assert "Processed collection" in caplog.text
        assert mock_exporter.records.call_count == 2

        full_call = call(
            cache_marc_files.library,
            cache_marc_files.collection,
            mock_annotator,
            creation_time=creation_time,
        )

        delta_call = call(
            cache_marc_files.library,
            cache_marc_files.collection,
            mock_annotator,
            creation_time=creation_time,
            since_time=last_updated,
        )

        mock_exporter.records.assert_has_calls([full_call, delta_call])
