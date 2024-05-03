from __future__ import annotations

import datetime
import logging
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest
from alembic.util import CommandError
from pytest import LogCaptureFixture
from sqlalchemy.exc import NoResultFound

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.metadata.novelist import NoveListAPI
from palace.manager.core.config import CannotLoadConfiguration
from palace.manager.core.marc import (
    MARCExporter,
    MarcExporterLibrarySettings,
    MarcExporterSettings,
)
from palace.manager.integration.goals import Goals
from palace.manager.scripts import (
    AdobeAccountIDResetScript,
    CacheMARCFiles,
    GenerateShortTokenScript,
    InstanceInitializationScript,
    LanguageListScript,
    LocalAnalyticsExportScript,
    NovelistSnapshotScript,
)
from palace.manager.sqlalchemy.model.credential import Credential
from palace.manager.sqlalchemy.model.datasource import DataSource
from palace.manager.sqlalchemy.model.discovery_service_registration import (
    DiscoveryServiceRegistration,
)
from palace.manager.sqlalchemy.model.integration import IntegrationConfiguration
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.marcfile import MarcFile
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.sqlalchemy.util import LOCK_ID_DB_INIT, create
from palace.manager.util.datetime_helpers import datetime_utc, utc_now
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake
from tests.fixtures.services import ServicesFixture

if TYPE_CHECKING:
    from tests.fixtures.authenticator import SimpleAuthIntegrationFixture
    from tests.fixtures.database import (
        DatabaseTransactionFixture,
        IntegrationConfigurationFixture,
    )


class TestAdobeAccountIDResetScript:
    def test_process_patron(self, db: DatabaseTransactionFixture):
        patron = db.patron()

        # This patron has a credential that links them to a Adobe account ID
        def set_value(credential):
            credential.value = "a credential"

        # Data source doesn't matter -- even if it's incorrect, a Credential
        # of the appropriate type will be deleted.
        data_source = DataSource.lookup(db.session, DataSource.OVERDRIVE)

        # Create one Credential that will be deleted and one that will be
        # left alone.
        for type in (
            AuthdataUtility.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
            "Some other type",
        ):
            credential = Credential.lookup(
                db.session, data_source, type, patron, set_value, True
            )

        assert 2 == len(patron.credentials)

        # Run the patron through the script.
        script = AdobeAccountIDResetScript(db.session)

        # A dry run does nothing.
        script.delete = False
        script.process_patron(patron)
        db.session.commit()
        assert 2 == len(patron.credentials)

        # Now try it for real.
        script.delete = True
        script.process_patron(patron)
        db.session.commit()

        # The Adobe-related credential is gone. The other one remains.
        [credential] = patron.credentials
        assert "Some other type" == credential.type


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


class TestInstanceInitializationScript:
    # These are some basic tests for the instance initialization script. It is tested
    # more thoroughly as part of the migration tests, since migration tests are able
    # to test the script's interaction with the database.

    def test_run_locks_database(self, db: DatabaseTransactionFixture):
        # The script locks the database with a PostgreSQL advisory lock
        with patch("palace.manager.scripts.pg_advisory_lock") as advisory_lock:
            mock_engine_factory = MagicMock()
            script = InstanceInitializationScript(engine_factory=mock_engine_factory)
            script.initialize = MagicMock()
            script.run()

            advisory_lock.assert_called_once_with(
                mock_engine_factory().begin().__enter__(),
                LOCK_ID_DB_INIT,
            )
            advisory_lock().__enter__.assert_called_once()
            advisory_lock().__exit__.assert_called_once()

    def test_initialize(self, db: DatabaseTransactionFixture):
        # Test that the script inspects the database and initializes or migrates the database
        # as necessary.
        with patch("palace.manager.scripts.inspect") as inspect:
            script = InstanceInitializationScript()
            script.migrate_database = MagicMock()  # type: ignore[method-assign]
            script.initialize_database = MagicMock()  # type: ignore[method-assign]
            script.initialize_search_indexes = MagicMock()  # type: ignore[method-assign]

            # If the database is uninitialized, initialize_database() is called.
            inspect().has_table.return_value = False
            script.initialize(MagicMock())
            script.initialize_database.assert_called_once()
            script.migrate_database.assert_not_called()

            # If the database is initialized, migrate_database() is called.
            script.initialize_database.reset_mock()
            script.migrate_database.reset_mock()
            inspect().has_table.return_value = True
            script.initialize(MagicMock())
            script.initialize_database.assert_not_called()
            script.migrate_database.assert_called_once()

    def test_initialize_alembic_exception(self, caplog: LogCaptureFixture):
        # Test that we handle a CommandError exception being returned by Alembic.
        with patch("palace.manager.scripts.inspect") as inspect:
            with patch("palace.manager.scripts.container_instance"):
                script = InstanceInitializationScript()

            caplog.set_level(logging.ERROR)
            script.migrate_database = MagicMock(side_effect=CommandError("test"))
            script.initialize_database = MagicMock()
            script.initialize_search_indexes = MagicMock()

            # If the database is initialized, migrate_database() is called.
            inspect().has_table.return_value = True
            script.initialize(MagicMock())
            script.initialize_database.assert_not_called()
            script.migrate_database.assert_called_once()

            assert "Error running database migrations" in caplog.text

    def test_initialize_database(self, db: DatabaseTransactionFixture):
        # Test that the script initializes the database.
        script = InstanceInitializationScript()
        mock_db = MagicMock()

        with patch(
            "palace.manager.scripts.SessionManager", autospec=SessionManager
        ) as session_manager:
            with patch("palace.manager.scripts.command") as alemic_command:
                script.initialize_database(mock_db)

        session_manager.initialize_data.assert_called_once()
        session_manager.initialize_schema.assert_called_once()
        alemic_command.stamp.assert_called_once()

    def test_migrate_database(self, db: DatabaseTransactionFixture):
        script = InstanceInitializationScript()
        mock_db = MagicMock()

        with patch("palace.manager.scripts.command") as alemic_command:
            script.migrate_database(mock_db)

        alemic_command.upgrade.assert_called_once()

    def test__get_alembic_config(self, db: DatabaseTransactionFixture):
        # Make sure we find alembic.ini for script command
        mock_connection = MagicMock()
        conf = InstanceInitializationScript._get_alembic_config(mock_connection, None)
        assert conf.config_file_name == "alembic.ini"
        assert conf.attributes["connection"] == mock_connection.engine
        assert conf.attributes["configure_logger"] is False

        test_ini = Path("test.ini")
        conf = InstanceInitializationScript._get_alembic_config(
            mock_connection, test_ini
        )
        assert conf.config_file_name == str(test_ini.resolve())

    def test_initialize_search_indexes_mocked(
        self,
        external_search_fake_fixture: ExternalSearchFixtureFake,
        caplog: LogCaptureFixture,
    ):
        caplog.set_level(logging.WARNING)

        script = InstanceInitializationScript()

        search_service = external_search_fake_fixture.external_search
        search_service.start_migration = MagicMock()
        search_service.search_service = MagicMock()

        # To fake "no migration is available", mock all the values
        search_service.start_migration.return_value = None
        search_service.search_service().is_pointer_empty.return_value = True

        # Migration should fail
        assert script.initialize_search_indexes() is False

        # Logs were emitted
        record = caplog.records.pop()
        assert "WARNING" in record.levelname
        assert "no migration was available" in record.message

        search_service.search_service.reset_mock()
        search_service.start_migration.reset_mock()

        # In case there is no need for a migration, read pointer exists as a non-empty pointer
        search_service.search_service().is_pointer_empty.return_value = False

        # Initialization should pass, as a no-op
        assert script.initialize_search_indexes() is True
        assert search_service.start_migration.call_count == 0

    def test_initialize_search_indexes(
        self, end_to_end_search_fixture: EndToEndSearchFixture
    ):
        search = end_to_end_search_fixture.external_search_index
        base_name = end_to_end_search_fixture.external_search.service.base_revision_name
        script = InstanceInitializationScript()

        # Initially this should not exist, if InstanceInit has not been run
        assert search.search_service().read_pointer() is None

        # Initialization should work now
        assert script.initialize_search_indexes() is True
        # Then we have the latest version index
        assert (
            search.search_service().read_pointer()
            == search._revision.name_for_index(base_name)
        )


class TestLanguageListScript:
    def test_languages(self, db: DatabaseTransactionFixture):
        """Test the method that gives this script the bulk of its output."""
        english = db.work(language="eng", with_open_access_download=True)
        tagalog = db.work(language="tgl", with_license_pool=True)
        [pool] = tagalog.license_pools
        db.add_generic_delivery_mechanism(pool)
        script = LanguageListScript(db.session)
        output = list(script.languages(db.default_library()))

        # English is ignored because all its works are open-access.
        # Tagalog shows up with the correct estimate.
        assert ["tgl 1 (Tagalog)"] == output


class TestNovelistSnapshotScript:
    def mockNoveListAPI(self, *args, **kwargs):
        self.called_with = (args, kwargs)

    def test_do_run(self, db: DatabaseTransactionFixture):
        """Test that NovelistSnapshotScript.do_run() calls the NoveList api."""

        class MockNovelistSnapshotScript(NovelistSnapshotScript):
            pass

        oldNovelistConfig = NoveListAPI.from_config
        NoveListAPI.from_config = self.mockNoveListAPI

        l1 = db.library()
        cmd_args = [l1.name]
        script = MockNovelistSnapshotScript(db.session)
        script.do_run(cmd_args=cmd_args)

        (params, args) = self.called_with

        assert params[0] == l1

        NoveListAPI.from_config = oldNovelistConfig


class TestLocalAnalyticsExportScript:
    def test_do_run(self, db: DatabaseTransactionFixture):
        class MockLocalAnalyticsExporter:
            def export(self, _db, start, end):
                self.called_with = [start, end]
                return "test"

        output = StringIO()
        cmd_args = ["--start=20190820", "--end=20190827"]
        exporter = MockLocalAnalyticsExporter()
        script = LocalAnalyticsExportScript(_db=db.session)
        script.do_run(output=output, cmd_args=cmd_args, exporter=exporter)
        assert "test" == output.getvalue()
        assert ["20190820", "20190827"] == exporter.called_with


class TestGenerateShortTokenScript:
    @pytest.fixture
    def script(self, db: DatabaseTransactionFixture):
        return GenerateShortTokenScript(_db=db.session)

    @pytest.fixture
    def output(self):
        return StringIO()

    @pytest.fixture
    def authdata(self, monkeypatch):
        authdata = AuthdataUtility(
            vendor_id="The Vendor ID",
            library_uri="http://your-library.org/",
            library_short_name="you",
            secret="Your library secret",
        )
        test_date = datetime_utc(2021, 5, 5)
        monkeypatch.setattr(authdata, "_now", lambda: test_date)
        return authdata

    @pytest.fixture
    def patron(self, authdata, db: DatabaseTransactionFixture):
        patron = db.patron(external_identifier="test")
        patron.authorization_identifier = "test"
        adobe_credential = db.credential(
            data_source_name=DataSource.INTERNAL_PROCESSING,
            patron=patron,
            type=authdata.ADOBE_ACCOUNT_ID_PATRON_IDENTIFIER,
        )
        adobe_credential.credential = "1234567"
        return patron

    @pytest.fixture
    def authentication_provider(
        self,
        db: DatabaseTransactionFixture,
        create_simple_auth_integration: SimpleAuthIntegrationFixture,
    ):
        barcode = "12345"
        pin = "abcd"
        create_simple_auth_integration(db.default_library(), barcode, pin)
        return barcode, pin

    def test_run_days(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --days
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--days=2",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n") == [
            "Vendor ID: The Vendor ID",
            "Token: YOU|1620345600|1234567|ZP45vhpfs3fHREvFkDDVgDAmhoD699elFD3PGaZu7yo@",
            "Username: YOU|1620345600|1234567",
            "Password: ZP45vhpfs3fHREvFkDDVgDAmhoD699elFD3PGaZu7yo@",
            "",
        ]

    def test_run_minutes(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --minutes
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--minutes=20",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n")[2] == "Username: YOU|1620174000|1234567"

    def test_run_hours(
        self, script, output, authdata, patron, db: DatabaseTransactionFixture
    ):
        # Test with --hours
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--hours=4",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert output.getvalue().split("\n")[2] == "Username: YOU|1620187200|1234567"

    def test_no_registry(self, script, output, patron, db: DatabaseTransactionFixture):
        cmd_args = [
            f"--barcode={patron.authorization_identifier}",
            "--minutes=20",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(_db=db.session, output=output, cmd_args=cmd_args)
        assert pytest_exit.value.code == -1
        assert "Library not registered with library registry" in output.getvalue()

    def test_no_patron_auth_method(
        self, script, output, db: DatabaseTransactionFixture
    ):
        # Test running when the patron does not exist
        cmd_args = [
            "--barcode={}".format("1234567"),
            "--hours=4",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(_db=db.session, output=output, cmd_args=cmd_args)
        assert pytest_exit.value.code == -1
        assert "No methods to authenticate patron found" in output.getvalue()

    def test_patron_auth(
        self,
        script,
        output,
        authdata,
        authentication_provider,
        db: DatabaseTransactionFixture,
    ):
        barcode, pin = authentication_provider
        # Test running when the patron does not exist
        cmd_args = [
            f"--barcode={barcode}",
            f"--pin={pin}",
            "--hours=4",
            db.default_library().short_name,
        ]
        script.do_run(
            _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
        )
        assert "Token: YOU|1620187200" in output.getvalue()

    def test_patron_auth_no_patron(
        self,
        script,
        output,
        authdata,
        authentication_provider,
        db: DatabaseTransactionFixture,
    ):
        barcode = "nonexistent"
        # Test running when the patron does not exist
        cmd_args = [
            f"--barcode={barcode}",
            "--hours=4",
            db.default_library().short_name,
        ]
        with pytest.raises(SystemExit) as pytest_exit:
            script.do_run(
                _db=db.session, output=output, cmd_args=cmd_args, authdata=authdata
            )
        assert pytest_exit.value.code == -1
        assert "Patron not found" in output.getvalue()
