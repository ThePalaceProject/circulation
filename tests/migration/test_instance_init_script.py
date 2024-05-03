import logging
import multiprocessing
import sys
from collections.abc import Generator
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Engine
from typing_extensions import Self

from palace.manager.scripts import InstanceInitializationScript
from palace.manager.sqlalchemy.session import SessionManager
from tests.fixtures.database import DatabaseFixture
from tests.fixtures.services import ServicesFixture, mock_services_container


class InstanceInitScriptFixture:
    def __init__(
        self,
        function_database: DatabaseFixture,
        services_fixture: ServicesFixture,
        alembic_config_path: Path,
    ):
        self.database = function_database
        self.services = services_fixture
        self.alembic_config_path = alembic_config_path

    def script(self) -> InstanceInitializationScript:
        with self.database.patch_engine():
            return InstanceInitializationScript(
                config_file=self.alembic_config_path,
            )

    @classmethod
    @contextmanager
    def fixture(
        cls,
        function_database: DatabaseFixture,
        services_fixture: ServicesFixture,
        alembic_config_path: Path,
    ) -> Generator[Self, None, None]:
        fixture = cls(function_database, services_fixture, alembic_config_path)
        yield fixture


@pytest.fixture
def instance_init_script_fixture(
    function_database: DatabaseFixture,
    services_fixture: ServicesFixture,
    alembic_config_path: Path,
) -> Generator[InstanceInitScriptFixture, None, None]:
    with InstanceInitScriptFixture.fixture(
        function_database, services_fixture, alembic_config_path
    ) as fixture:
        yield fixture


def _run_script(config_path: Path, db_url: str) -> None:
    try:
        # Capturing the log output
        stream = StringIO()
        logging.basicConfig(stream=stream, level=logging.INFO, force=True)

        def engine_factory() -> Engine:
            return SessionManager.engine(db_url)

        mock_services = MagicMock()
        with (mock_services_container(mock_services),):
            script = InstanceInitializationScript(
                config_file=config_path, engine_factory=engine_factory
            )
            script.run()

        # Set our exit code to the number of upgrades we ran
        sys.exit(stream.getvalue().count("Running upgrade"))
    except Exception as e:
        # Print the exception for debugging and exit with -1
        # which will cause the test to fail.
        print(str(e))
        sys.exit(-1)


def test_locking(
    alembic_runner: MigrationContext,
    alembic_config_path: Path,
    instance_init_script_fixture: InstanceInitScriptFixture,
) -> None:
    # Migrate to the initial revision
    alembic_runner.migrate_down_to("base")
    db_url = instance_init_script_fixture.database.database_name.url

    # Spawn three processes, that will all try to migrate to head
    # at the same time. One of them should do the migration, and
    # the other two should wait, then do no migration since it
    # has already been done.
    mp_ctx = multiprocessing.get_context("spawn")
    process_kwargs = {
        "config_path": alembic_config_path,
        "db_url": db_url,
    }
    p1 = mp_ctx.Process(target=_run_script, kwargs=process_kwargs)
    p2 = mp_ctx.Process(target=_run_script, kwargs=process_kwargs)
    p3 = mp_ctx.Process(target=_run_script, kwargs=process_kwargs)

    p1.start()
    p2.start()
    p3.start()

    p1.join()
    p2.join()
    p3.join()

    assert p1.exitcode is not None
    assert p2.exitcode is not None
    assert p3.exitcode is not None

    exit_codes = sorted([p1.exitcode, p2.exitcode, p3.exitcode], reverse=True)
    # One process did all the migrations
    assert exit_codes[0] > 0

    # The other two waited, then did no migrations
    assert exit_codes[1] == 0
    assert exit_codes[2] == 0


def test_initialize(instance_init_script_fixture: InstanceInitScriptFixture) -> None:
    # Drop any existing schema
    instance_init_script_fixture.database.drop_existing_schema()

    # Run the script and make sure we create the alembic_version table
    engine = instance_init_script_fixture.database.engine
    inspector = inspect(engine)
    assert "alembic_version" not in inspector.get_table_names()
    assert len(inspector.get_table_names()) == 0

    script = instance_init_script_fixture.script()
    script.initialize_database = Mock(wraps=script.initialize_database)
    script.migrate_database = Mock(wraps=script.migrate_database)
    script.run()

    inspector = inspect(engine)
    assert "alembic_version" in inspector.get_table_names()
    assert "libraries" in inspector.get_table_names()
    assert len(inspector.get_table_names()) > 2

    assert script.initialize_database.call_count == 1
    assert script.migrate_database.call_count == 0

    # Run the script again. Ensure we don't call initialize_database again,
    # but that we do call migrate_database, since the schema already exists.
    script.run()
    assert script.initialize_database.call_count == 1
    assert script.migrate_database.call_count == 1


def test_migrate(
    alembic_runner: MigrationContext,
    instance_init_script_fixture: InstanceInitScriptFixture,
) -> None:
    # Run the script and make sure we create the alembic_version table
    # Migrate to the initial revision
    alembic_runner.migrate_down_to("base")

    # Check the revision
    assert alembic_runner.current == "base"
    assert alembic_runner.current != alembic_runner.heads[0]

    script = instance_init_script_fixture.script()
    script.initialize_database = Mock(wraps=script.initialize_database)
    script.migrate_database = Mock(wraps=script.migrate_database)
    script.run()

    # Make sure we have upgraded
    assert alembic_runner.current == alembic_runner.heads[0]

    assert script.initialize_database.call_count == 0
    assert script.migrate_database.call_count == 1
