from __future__ import annotations

from pytest_alembic import MigrationContext
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

REVISION = "5ca948100902"
DOWN_REVISION = "58ebd34c5092"


def _coverage_status_exists(engine: Engine) -> bool:
    with engine.begin() as connection:
        # Raw SQL rather than Inspector.get_enums(), which exists only on the
        # Postgres inspector and so is not on the type mypy sees here.
        return (
            connection.execute(
                text("SELECT count(*) FROM pg_type WHERE typname = 'coverage_status'")
            ).scalar_one()
            == 1
        )


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
    assert _coverage_status_exists(alembic_engine)

    # Apply the drop.
    alembic_runner.migrate_up_one()
    assert alembic_runner.current == REVISION

    tables = set(inspect(alembic_engine).get_table_names())
    assert "coveragerecords" not in tables
    assert "equivalentscoveragerecords" not in tables
    # The enum was shared by the two tables just dropped and goes with them;
    # nothing else asserts this, so losing the drop would otherwise be silent.
    assert not _coverage_status_exists(alembic_engine)
    # The unrelated timestamps table is left in place.
    assert "timestamps" in tables
