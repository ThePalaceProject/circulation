"""Test migration a5ee359c2d31_add_startup_tasks_table."""

from pytest_alembic import MigrationContext
from sqlalchemy import text
from sqlalchemy.engine import Engine


def test_add_startup_tasks_table_uses_expected_enum_labels(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """The startup task enum should use labels that match the ORM mapping."""
    alembic_runner.migrate_down_to("08aba65e21e0")
    alembic_runner.migrate_up_to("a5ee359c2d31")

    with alembic_engine.begin() as connection:
        labels = connection.execute(
            text(
                """
                SELECT enumlabel
                FROM pg_enum
                JOIN pg_type ON pg_type.oid = pg_enum.enumtypid
                WHERE pg_type.typname = 'startuptaskstate'
                ORDER BY enumsortorder
                """
            )
        ).scalars()
        assert list(labels) == ["RUN", "MARKED"]

        connection.execute(
            text(
                """
                INSERT INTO startup_tasks (key, recorded_at, state)
                VALUES ('test_task', NOW(), 'RUN')
                """
            )
        )


def test_add_startup_tasks_table_downgrade_drops_enum_type(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
) -> None:
    """Downgrading should remove the startup task enum type."""
    alembic_runner.migrate_down_to("08aba65e21e0")
    alembic_runner.migrate_up_to("a5ee359c2d31")
    alembic_runner.migrate_down_to("08aba65e21e0")

    with alembic_engine.begin() as connection:
        enum_type_exists = connection.execute(
            text(
                """
                SELECT 1
                FROM pg_type
                WHERE typname = 'startuptaskstate'
                """
            )
        ).scalar_one_or_none()
        assert enum_type_exists is None
