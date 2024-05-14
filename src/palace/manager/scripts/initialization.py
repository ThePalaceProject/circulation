from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from alembic import command, config
from alembic.util import CommandError
from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.orm import Session

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
    def _get_alembic_config(
        connection: Connection, config_file: Path | None
    ) -> config.Config:
        """Get the Alembic config object for the current app."""
        filename = "alembic.ini" if config_file is None else str(config_file.resolve())
        conf = config.Config(filename)
        conf.attributes["configure_logger"] = False
        conf.attributes["connection"] = connection.engine
        conf.attributes["need_lock"] = False
        return conf

    def migrate_database(self, connection: Connection) -> None:
        """Run our database migrations to make sure the database is up-to-date."""
        alembic_conf = self._get_alembic_config(connection, self._config_file)
        command.upgrade(alembic_conf, "head")

    def initialize_database(self, connection: Connection) -> None:
        """
        Initialize the database, creating tables, loading default data and then
        stamping the most recent migration as the current state of the DB.
        """
        SessionManager.initialize_schema(connection)

        with Session(connection) as session:
            # Initialize the database with default data
            SessionManager.initialize_data(session)

        # Stamp the most recent migration as the current state of the DB
        alembic_conf = self._get_alembic_config(connection, self._config_file)
        command.stamp(alembic_conf, "head")

    def initialize_search_indexes(self) -> bool:
        search = self._container.search.index()
        return search.initialize_indices()

    def initialize(self, connection: Connection):
        """Initialize the database if necessary."""
        inspector = inspect(connection)
        if inspector.has_table("alembic_version"):
            self.log.info("Database schema already exists. Running migrations.")
            try:
                self.migrate_database(connection)
                self.log.info("Migrations complete.")
            except CommandError as e:
                self.log.error(
                    f"Error running database migrations: {str(e)}. This "
                    f"is possibly because you are running a old version "
                    f"of the application against a new database."
                )
        else:
            self.log.info("Database schema does not exist. Initializing.")
            self.initialize_database(connection)
            self.log.info("Initialization complete.")

        self.initialize_search_indexes()

    def run(self) -> None:
        """
        Initialize the database if necessary. This script is idempotent, so it
        can be run every time the app starts.

        The script uses a PostgreSQL advisory lock to ensure that only one
        instance of the script is running at a time. This prevents multiple
        instances from trying to initialize the database at the same time.
        """
        engine = self._engine_factory()
        with engine.begin() as connection:
            with pg_advisory_lock(connection, LOCK_ID_DB_INIT):
                self.initialize(connection)

        engine.dispose()
