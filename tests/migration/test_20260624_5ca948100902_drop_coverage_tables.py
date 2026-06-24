from __future__ import annotations

from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

REVISION = "5ca948100902"
DOWN_REVISION = "a6c85605404c"


def test_drop_coverage_tables(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """The migration drops coveragerecords and equivalentscoveragerecords.

    Stepping the migration down recreates both tables (and the shared
    ``coverage_status`` enum); stepping it back up drops them again while
    leaving the unrelated ``timestamps`` table in place.
    """
    alembic_runner.migrate_down_to(REVISION)
    # Step down once more so the tables exist again.
    alembic_runner.migrate_down_one()
    assert alembic_runner.current == DOWN_REVISION

    tables = set(inspect(alembic_engine).get_table_names())
    assert "coveragerecords" in tables
    assert "equivalentscoveragerecords" in tables

    # Apply the drop.
    alembic_runner.migrate_up_one()
    assert alembic_runner.current == REVISION

    tables = set(inspect(alembic_engine).get_table_names())
    assert "coveragerecords" not in tables
    assert "equivalentscoveragerecords" not in tables
    # The unrelated timestamps table is left in place.
    assert "timestamps" in tables
