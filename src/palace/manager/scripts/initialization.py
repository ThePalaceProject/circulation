from __future__ import annotations

from argparse import ArgumentParser
from collections.abc import Callable, Sequence
from pathlib import Path

from alembic import command, config
from alembic.util import CommandError
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.search import get_migrate_search_chain
from palace.manager.scripts.startup import (
    run_startup_tasks as _run_startup_tasks,
    stamp_startup_tasks as _stamp_startup_tasks,
)
from palace.manager.search.revision import SearchSchemaRevision
from palace.manager.search.service import SearchService
from palace.manager.service.container import container_instance
from palace.manager.sqlalchemy.session import SessionManager
from palace.manager.sqlalchemy.util import LOCK_ID_DB_INIT, pg_advisory_lock
from palace.manager.util.log import LoggerMixin


class InstanceInitializationScript(LoggerMixin):
    """An idempotent script to initialize an instance of the Circulation Manager.

    This script is intended for use in servers, Docker containers, etc,
    when the Circulation Manager app is being installed. It initializes
    the database and sets an appropriate alias on the OpenSearch index.

    Because it's currently run every time a container is started, it must
    remain idempotent.
    """

    def __init__(
        self,
        config_file: Path | None = None,
        engine_factory: Callable[[], Engine] = SessionManager.engine,
    ) -> None:
        self._container = container_instance()

        # Call init_resources() to initialize the logging configuration.
        self._container.init_resources()
        self._config_file = config_file

        self._engine_factory = engine_factory

    @staticmethod
    def _get_alembic_config(engine: Engine, config_file: Path | None) -> config.Config:
        """Get the Alembic config object for the current app."""
        filename = "alembic.ini" if config_file is None else str(config_file.resolve())
        conf = config.Config(filename)
        conf.attributes["configure_logger"] = False
        conf.attributes["connection"] = engine
        conf.attributes["need_lock"] = False
        return conf

    @staticmethod
    def _db_initialized(engine: Engine) -> bool:
        """
        Test if the database is already initialized
        """
        inspector = inspect(engine)
        return inspector.has_table("alembic_version")

    def migrate_database(self, engine: Engine) -> None:
        """Run our database migrations to make sure the database is up-to-date."""
        alembic_conf = self._get_alembic_config(engine, self._config_file)
        command.upgrade(alembic_conf, "head")

    def initialize_database_schema(self, engine: Engine) -> None:
        """
        Initialize the database, creating tables, loading default data and then
        stamping the most recent migration as the current state of the DB.
        """
        with engine.begin() as connection:
            SessionManager.initialize_schema(connection)
            with Session(connection) as session:
                # Initialize the database with default data
                SessionManager.initialize_data(session)

            # Stamp the most recent migration as the current state of the DB.
            # If this fails, schema/data changes above are rolled back with the
            # surrounding transaction.
            alembic_conf = self._get_alembic_config(engine, self._config_file)
            command.stamp(alembic_conf, "head")

    def initialize_database(self, engine: Engine) -> bool:
        """
        Initialize the database if necessary.
        """
        already_initialized = self._db_initialized(engine)
        if already_initialized:
            self.log.info("Database schema already exists. Running migrations.")
            try:
                self.migrate_database(engine)
                self.log.info("Migrations complete.")
            except CommandError as e:
                self.log.error(
                    f"Error running database migrations: {str(e)}. This "
                    f"is possibly because you are running a old version "
                    f"of the application against a new database."
                )
        else:
            self.log.info("Database schema does not exist. Initializing.")
            self.initialize_database_schema(engine)
            self.log.info("Initialization complete.")

        return already_initialized

    @classmethod
    def create_search_index(
        cls, service: SearchService, revision: SearchSchemaRevision
    ) -> None:
        # Initialize a new search index by creating the index, setting the mapping,
        # and setting the read and write pointers.
        service.index_create(revision)
        service.index_set_mapping(revision)
        service.write_pointer_set(revision)

    @classmethod
    def migrate_search(
        cls,
        service: SearchService,
        revision: SearchSchemaRevision,
    ) -> None:
        # The revision is not the most recent. We need to create a new index.
        # and start reindexing our data into it asynchronously. When the reindex
        # is complete, we will switch the read pointer to the new index.
        cls.logger().info(f"Creating a new index for revision (v{revision.version}).")
        cls.create_search_index(service, revision)
        task = get_migrate_search_chain().apply_async()
        cls.logger().info(
            f"Task queued to index data into new search index (Task ID: {task.id})."
        )

    def initialize_search(self) -> None:
        service = self._container.search.service()
        revision_directory = self._container.search.revision_directory()
        revision = revision_directory.highest()
        write_pointer = service.write_pointer()
        read_pointer = service.read_pointer()

        if write_pointer is None or read_pointer is None:
            # Pointers do not exist. This is a fresh index.
            self.log.info("Search index does not exist. Creating a new index.")
            self.create_search_index(service, revision)
            service.read_pointer_set(revision)
        elif write_pointer.version < revision.version:
            self.log.info(
                f"Search index is out-of-date ({service.base_revision_name} v{write_pointer.version})."
            )
            self.migrate_search(service, revision)
        elif read_pointer.version < revision.version:
            self.log.info(
                f"Search read pointer is out-of-date (v{read_pointer.version}). Latest is v{revision.version}."
                f"This likely means that the reindexing task is in progress. If there is no reindexing task "
                f"running, you may need to repair the search index."
            )
        elif (
            read_pointer.version > revision.version
            or write_pointer.version > revision.version
        ):
            self.log.error(
                f"Search index is in an inconsistent state. Read pointer: v{read_pointer.version}, "
                f"Write pointer: v{write_pointer.version}, Latest revision: v{revision.version}. "
                f"You may be running an old version of the application against a new search index. "
            )
            return
        else:
            self.log.info(
                f"Search index is up-to-date ({service.base_revision_name} v{revision.version})."
            )
        self.log.info("Search initialization complete.")

    def run_startup_tasks(self, engine: Engine, already_initialized: bool) -> None:
        """Run any registered one-time startup tasks.

        On a fresh database install, tasks state is set to stamped
        without dispatching them — there is no existing data to migrate.
        """
        if already_initialized:
            _run_startup_tasks(engine, self._container)
        else:
            _stamp_startup_tasks(engine)

    def run(self, args: Sequence[str] | None = None) -> None:
        """
        Initialize the database if necessary. This script is idempotent, so it
        can be run every time the app starts.

        The script uses a PostgreSQL advisory lock to ensure that only one
        instance of the script is running at a time. This prevents multiple
        instances from trying to initialize the database at the same time.
        """

        # This script doesn't take any arguments, but we still call argparse, so that
        # we can use the --help option to print out a help message. This avoids the
        # surprise of the script actually running when the user just wanted to see the help.
        ArgumentParser(
            description="Initialize the database and search index for the Palace Manager."
        ).parse_args(args)

        engine = self._engine_factory()
        with pg_advisory_lock(engine, LOCK_ID_DB_INIT):
            db_initialized = self.initialize_database(engine)
            self.initialize_search()
            self.run_startup_tasks(engine, db_initialized)
        engine.dispose()
