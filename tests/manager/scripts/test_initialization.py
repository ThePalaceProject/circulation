from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

from _pytest.logging import LogCaptureFixture
from alembic.util import CommandError

from palace.manager.scripts.initialization import InstanceInitializationScript
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.sqlalchemy.util import LOCK_ID_DB_INIT
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import EndToEndSearchFixture, ExternalSearchFixtureFake


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
