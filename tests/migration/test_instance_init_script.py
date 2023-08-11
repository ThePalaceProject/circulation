import logging
import sys
from io import StringIO
from multiprocessing import Process
from unittest.mock import Mock

from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from core.model import SessionManager
from scripts import InstanceInitializationScript
from tests.fixtures.database import ApplicationFixture


def _run_script() -> None:
    try:
        # Run the script, capturing the log output
        script = InstanceInitializationScript()
        stream = StringIO()
        logging.basicConfig(stream=stream, level=logging.INFO, force=True)
        script.run()

        # Set our exit code to the number of upgrades we ran
        sys.exit(stream.getvalue().count("Running upgrade"))
    except Exception as e:
        # Print the exception for debugging and exit with -1
        # which will cause the test to fail.
        print(str(e))
        sys.exit(-1)


def test_locking(alembic_runner: MigrationContext, alembic_engine: Engine) -> None:
    # Migrate to the initial revision
    alembic_runner.migrate_down_to("base")

    # Spawn three processes, that will all try to migrate to head
    # at the same time. One of them should do the migration, and
    # the other two should wait, then do no migration since it
    # has already been done.
    p1 = Process(target=_run_script)
    p2 = Process(target=_run_script)
    p3 = Process(target=_run_script)

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


def test_initialize(application: ApplicationFixture) -> None:
    # Run the script and make sure we create the alembic_version table

    application.drop_existing_schema()

    engine = SessionManager.engine()
    inspector = inspect(engine)
    assert "alembic_version" not in inspector.get_table_names()
    assert len(inspector.get_table_names()) == 0

    script = InstanceInitializationScript()
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


def test_migrate(alembic_runner: MigrationContext) -> None:
    # Run the script and make sure we create the alembic_version table
    # Migrate to the initial revision
    alembic_runner.migrate_down_to("base")

    # Check the revision
    assert alembic_runner.current == "base"
    assert alembic_runner.current != alembic_runner.heads[0]

    script = InstanceInitializationScript()
    script.initialize_database = Mock(wraps=script.initialize_database)
    script.migrate_database = Mock(wraps=script.migrate_database)
    script.run()

    # Make sure we have upgraded
    assert alembic_runner.current == alembic_runner.heads[0]

    assert script.initialize_database.call_count == 0
    assert script.migrate_database.call_count == 1
