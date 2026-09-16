from __future__ import annotations

from pytest_alembic import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Engine

REVISION = "58ebd34c5092"


def _coveragerecord_count(engine: Engine) -> int:
    with engine.begin() as connection:
        return connection.execute(
            text("SELECT count(*) FROM coveragerecords")
        ).scalar_one()


def test_empties_coveragerecords(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """The migration removes every row from coveragerecords.

    Every column except the primary key is nullable, so a bare insert is enough
    to stand in for a leftover row written by the retired CoverageProvider.
    """
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    with alembic_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO coveragerecords (operation) VALUES ('import')")
        )
    assert _coveragerecord_count(alembic_engine) == 1

    alembic_runner.migrate_up_one()
    assert alembic_runner.current == REVISION

    assert _coveragerecord_count(alembic_engine) == 0

    # The table itself survives this release -- it is dropped in the next one.
    with alembic_engine.begin() as connection:
        connection.execute(
            text("INSERT INTO coveragerecords (operation) VALUES ('import')")
        )
    assert _coveragerecord_count(alembic_engine) == 1
