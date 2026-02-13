from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from _pytest.logging import LogCaptureFixture
from alembic.util import CommandError
from opensearchpy.exceptions import OpenSearchException

from palace.manager.scripts.initialization import InstanceInitializationScript
from palace.manager.search.revision_directory import SearchRevisionDirectory
from palace.manager.service.logging.configuration import LogLevel
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.sqlalchemy.util import LOCK_ID_DB_INIT
from tests.fixtures.database import DatabaseTransactionFixture
from tests.fixtures.search import ExternalSearchFixture
from tests.fixtures.services import ServicesFixture
from tests.mocks.search import MockSearchSchemaRevisionLatest


class TestInstanceInitializationScript:
    # These are some basic tests for the instance initialization script. It is tested
    # more thoroughly as part of the migration tests, since migration tests are able
    # to test the script's interaction with the database.

    def test_run_locks_database(self, services_fixture: ServicesFixture):
        # The script locks the database with a PostgreSQL advisory lock
        with patch(
            "palace.manager.scripts.initialization.pg_advisory_lock"
        ) as advisory_lock:
            mock_engine_factory = MagicMock()
            script = InstanceInitializationScript(engine_factory=mock_engine_factory)
            script.initialize_database = MagicMock()
            script.initialize_search = MagicMock()
            script.run([])

        advisory_lock.assert_called_once_with(
            mock_engine_factory.return_value,
            LOCK_ID_DB_INIT,
        )
        advisory_lock.return_value.__enter__.assert_called_once()
        advisory_lock.return_value.__exit__.assert_called_once()

        # Run called initialize_database and initialize_search while the lock was held
        script.initialize_database.assert_called_once_with(
            mock_engine_factory.return_value
        )
        script.initialize_search.assert_called_once()

    def test_initialize_database(self, services_fixture: ServicesFixture):
        # Test that the script initializes or migrates the database as necessary.
        script = InstanceInitializationScript()
        script.migrate_database = MagicMock()
        script.initialize_database_schema = MagicMock()
        mock_engine = MagicMock()

        with patch("palace.manager.scripts.initialization.inspect") as inspect:
            # If the database is uninitialized, initialize_database() is called.
            inspect().has_table.return_value = False
            script.initialize_database(mock_engine)
            script.initialize_database_schema.assert_called_once()
            script.migrate_database.assert_not_called()

            # If the database is initialized, migrate_database() is called.
            script.initialize_database_schema.reset_mock()
            script.migrate_database.reset_mock()
            inspect().has_table.return_value = True
            script.initialize_database(mock_engine)
            script.initialize_database_schema.assert_not_called()
            script.migrate_database.assert_called_once()

    def test_initialize_database_alembic_exception(
        self, caplog: LogCaptureFixture, services_fixture: ServicesFixture
    ):
        # Test that we handle a CommandError exception being returned by Alembic.
        with patch("palace.manager.scripts.initialization.inspect") as inspect:
            script = InstanceInitializationScript()

            caplog.set_level(logging.ERROR)
            script.migrate_database = MagicMock(side_effect=CommandError("test"))
            script.initialize_database_schema = MagicMock()

            # If the database is initialized, migrate_database() is called.
            inspect().has_table.return_value = True
            script.initialize_database(MagicMock())
            script.initialize_database_schema.assert_not_called()
            script.migrate_database.assert_called_once()

            assert "Error running database migrations" in caplog.text

    def test_initialize_database_schema(self, services_fixture: ServicesFixture):
        # Test that the script initializes the database.
        script = InstanceInitializationScript()
        mock_db = MagicMock()

        with (
            patch(
                "palace.manager.scripts.initialization.SessionManager",
                autospec=SessionManager,
            ) as session_manager,
            patch("palace.manager.scripts.initialization.command") as alemic_command,
        ):
            script.initialize_database_schema(mock_db)

        session_manager.initialize_data.assert_called_once()
        session_manager.initialize_schema.assert_called_once()
        alemic_command.stamp.assert_called_once()

    def test_migrate_database(self, services_fixture: ServicesFixture):
        script = InstanceInitializationScript()
        mock_db = MagicMock()

        with patch("palace.manager.scripts.initialization.command") as alemic_command:
            script.migrate_database(mock_db)

        alemic_command.upgrade.assert_called_once()

    def test__get_alembic_config(self, db: DatabaseTransactionFixture):
        # Make sure we find alembic.ini for script command
        mock_engine = MagicMock()
        conf = InstanceInitializationScript._get_alembic_config(mock_engine, None)
        assert conf.config_file_name == "alembic.ini"
        assert conf.attributes["connection"] == mock_engine
        assert conf.attributes["configure_logger"] is False

        test_ini = Path("test.ini")
        conf = InstanceInitializationScript._get_alembic_config(mock_engine, test_ini)
        assert conf.config_file_name == str(test_ini.resolve())

    def test_initialize_search(self, external_search_fixture: ExternalSearchFixture):
        service = external_search_fixture.service
        index = external_search_fixture.index
        revision = external_search_fixture.revision
        base_name = service.base_revision_name

        script = InstanceInitializationScript()
        script.create_search_index = MagicMock(wraps=script.create_search_index)
        script.migrate_search = MagicMock(wraps=script.migrate_search)

        # Initially this should not exist, if the search service hasn't been initialized
        assert service.read_pointer() is None
        assert service.write_pointer() is None

        # We cannot do make search requests before we initialize the search service
        with pytest.raises(OpenSearchException) as raised:
            index.query_works("")
        assert "index_not_found_exception" in str(raised.value)

        # Do the initial search index creation
        script.initialize_search()

        # We should have created the search index, but not migrated it since we
        # know it is a new freshly created index.
        script.create_search_index.assert_called_once()
        script.migrate_search.assert_not_called()

        # Then we have the latest version index
        read_pointer = service.read_pointer()
        assert read_pointer is not None
        assert read_pointer.index == revision.name_for_index(base_name)
        write_pointer = service.write_pointer()
        assert write_pointer is not None
        assert write_pointer.index == revision.name_for_index(base_name)

        # Now we try to initialize the search index again, and we should not create a new index
        script.create_search_index.reset_mock()
        script.initialize_search()
        script.create_search_index.assert_not_called()
        script.migrate_search.assert_not_called()

        # And because no new index was created, the read and write pointers should be the same
        assert service.read_pointer() == read_pointer
        assert service.write_pointer() == write_pointer

        # The same client should work without issue once the pointers are setup
        assert len(index.query_works("")) == 0

    def test_migrate_search(
        self,
        external_search_fixture: ExternalSearchFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        service = external_search_fixture.service
        index = external_search_fixture.index
        client = external_search_fixture.client
        revision = external_search_fixture.revision
        base_name = service.base_revision_name

        script = InstanceInitializationScript()

        work1 = db.work(title="Test work 1", with_open_access_download=True)
        work2 = db.work(title="Test work 2", with_open_access_download=True)

        # Do the initial search index creation
        script.initialize_search()

        # We should have migrated the search index to the latest revision
        read_pointer = service.read_pointer()
        assert read_pointer is not None
        assert read_pointer.version == revision.version
        write_pointer = service.write_pointer()
        assert write_pointer is not None
        assert write_pointer.version == revision.version

        new_revision = MockSearchSchemaRevisionLatest(1000001)
        new_revision_directory = SearchRevisionDirectory(
            {new_revision.version: new_revision}
        )
        new_revision_index_name = new_revision.name_for_index(base_name)

        # The new_revision_index_name does not exist yet
        with pytest.raises(OpenSearchException) as raised:
            client.indices.get(index=new_revision_index_name)
        assert "index_not_found" in str(raised.value)

        # The client can work without issue in this state
        assert len(index.query_works("")) == 0

        # We can add documents to the index
        index.add_document(work1.to_search_document())
        index.search_service().refresh()
        [indexed_work_1] = index.query_works("")
        assert indexed_work_1.work_id == work1.id

        # Now we migrate to the new revision
        script.migrate_search = MagicMock(wraps=script.migrate_search)
        script._container.search.revision_directory.override(new_revision_directory)
        with patch(
            "palace.manager.scripts.initialization.get_migrate_search_chain"
        ) as get_migrate_search_chain:
            script.initialize_search()

        # We should have created the new search index, and started the migration
        script.migrate_search.assert_called_once()

        # The new index should exist
        assert client.indices.get(index=new_revision_index_name)

        # The write pointer should point to the new revision
        write_pointer = service.write_pointer()
        assert write_pointer is not None
        assert write_pointer.index == new_revision_index_name
        assert write_pointer.version == new_revision.version

        # The read pointer should still point to the old revision
        read_pointer = service.read_pointer()
        assert read_pointer is not None
        assert read_pointer.version == revision.version

        # We can add more documents to the index
        index.add_document(work2.to_search_document())
        index.search_service().refresh()

        # But the documents are not searchable yet, since they are added to the new index
        # and the read pointer is still pointing to the old index. So we find work1 but not work2.
        [indexed_work_1] = index.query_works("")
        assert indexed_work_1.work_id == work1.id

        # The migration should have been queued
        get_migrate_search_chain.assert_called_once()
        get_migrate_search_chain.return_value.apply_async.assert_called_once()

        # If the initialization is run again, the migration should not be run again, but we do log a message
        # about the read pointer being out of date
        script.migrate_search.reset_mock()
        caplog.clear()
        caplog.set_level(LogLevel.info)
        script.initialize_search()
        script.migrate_search.assert_not_called()
        assert "Search read pointer is out-of-date" in caplog.text

        # We simulate the migration task completing, by setting the read pointer to the new index
        service.read_pointer_set(new_revision)

        # Now work2 is searchable, but work1 is not, since the migration was mocked out and did not actually run
        [indexed_work_2] = index.query_works("")
        assert indexed_work_2.work_id == work2.id

    def test_migrate_downgrade(
        self,
        external_search_fixture: ExternalSearchFixture,
        db: DatabaseTransactionFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        service = external_search_fixture.service

        script = InstanceInitializationScript()

        old_revision = MockSearchSchemaRevisionLatest(1000000)
        new_revision = MockSearchSchemaRevisionLatest(1000001)
        revision_directory = SearchRevisionDirectory(
            {
                new_revision.version: new_revision,
                old_revision.version: old_revision,
            }
        )
        script._container.search.revision_directory.override(revision_directory)

        # Do the initial search index creation
        script.initialize_search()

        # We should have migrated the search index to the latest revision
        read_pointer = service.read_pointer()
        assert read_pointer is not None
        assert read_pointer.version == new_revision.version
        write_pointer = service.write_pointer()
        assert write_pointer is not None
        assert write_pointer.version == new_revision.version

        # Now we run the migration script again, but this time the most recent revision is the old revision
        revision_directory = SearchRevisionDirectory(
            {
                old_revision.version: old_revision,
            }
        )
        script._container.search.revision_directory.override(revision_directory)
        script.initialize_search()

        # We should not have touched the read and write pointers, since they are more recent than the latest revision
        read_pointer = service.read_pointer()
        assert read_pointer is not None
        assert read_pointer.version == new_revision.version
        write_pointer = service.write_pointer()
        assert write_pointer is not None
        assert write_pointer.version == new_revision.version

        # And we should have logged a message about the situation
        assert (
            "You may be running an old version of the application against a new search index"
            in caplog.text
        )
