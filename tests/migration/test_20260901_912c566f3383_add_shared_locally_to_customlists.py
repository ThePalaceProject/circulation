from pytest_alembic import MigrationContext

from tests.migration.conftest import AlembicDatabaseFixture

REVISION = "912c566f3383"


def test_backfills_shared_locally(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    """Rows in the sharing join table collapse into the shared_locally flag."""
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    owner_id = alembic_database.library()
    shared_with_id = alembic_database.library()

    shared_id = alembic_database.customlist(library_id=owner_id)
    alembic_database.share_customlist(shared_id, shared_with_id)

    unshared_id = alembic_database.customlist(library_id=owner_id)

    # A list whose owning library has been deleted keeps the join rows for the
    # libraries it was shared with, so it is still shared.
    orphaned_id = alembic_database.customlist(library_id=None)
    alembic_database.share_customlist(orphaned_id, shared_with_id)

    alembic_runner.migrate_up_one()

    assert alembic_database.fetch_customlist(shared_id).shared_locally is True
    assert alembic_database.fetch_customlist(unshared_id).shared_locally is False
    assert alembic_database.fetch_customlist(orphaned_id).shared_locally is True
