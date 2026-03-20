from pytest_alembic import MigrationContext

from tests.migration.conftest import AlembicDatabaseFixture

REVISION = "23795e50c915"
SIRSI_PROTOCOL = "api.sirsidynix_authentication_provider"
PATRON_AUTH_GOAL = "PATRON_AUTH_GOAL"


def test_remove_sirsi_library_id(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    """The migration removes the library_id key from SirsiDynix library settings."""
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    library_id = alembic_database.library()
    integration_id = alembic_database.integration(
        protocol=SIRSI_PROTOCOL,
        goal=PATRON_AUTH_GOAL,
    )
    alembic_database.integration_library_configuration(
        parent_id=integration_id,
        library_id=library_id,
        settings={"library_id": "testlib", "library_disallowed_suffixes": []},
    )

    alembic_runner.migrate_up_one()

    row = alembic_database.fetch_integration_library_configuration(
        parent_id=integration_id, library_id=library_id
    )
    assert "library_id" not in row.settings
    assert row.settings["library_disallowed_suffixes"] == []


def test_non_sirsi_integration_untouched(
    alembic_runner: MigrationContext,
    alembic_database: AlembicDatabaseFixture,
) -> None:
    """Non-SirsiDynix integrations with a library_id key are not modified."""
    alembic_runner.migrate_down_to(REVISION)
    alembic_runner.migrate_down_one()

    library_id = alembic_database.library()
    other_integration_id = alembic_database.integration(
        protocol="some.other.provider",
        goal=PATRON_AUTH_GOAL,
    )
    alembic_database.integration_library_configuration(
        parent_id=other_integration_id,
        library_id=library_id,
        settings={"library_id": "keep_me"},
    )

    alembic_runner.migrate_up_one()

    row = alembic_database.fetch_integration_library_configuration(
        parent_id=other_integration_id, library_id=library_id
    )
    assert row.settings["library_id"] == "keep_me"
