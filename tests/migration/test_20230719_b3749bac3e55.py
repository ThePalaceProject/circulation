import json

from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Engine

from tests.migration.conftest import CreateConfigSetting, CreateLibrary


def column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    inspector = inspect(engine)
    columns = [column["name"] for column in inspector.get_columns(table_name)]
    return column_name in columns


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_config_setting: CreateConfigSetting,
    create_library: CreateLibrary,
) -> None:
    alembic_runner.migrate_down_to("b3749bac3e55")

    # Make sure settings column exists
    assert column_exists(alembic_engine, "libraries", "settings_dict")

    # Test down migration, make sure settings column is dropped
    alembic_runner.migrate_down_one()
    assert not column_exists(alembic_engine, "libraries", "settings_dict")

    # Create a library with some configuration settings
    with alembic_engine.connect() as connection:
        library = create_library(connection)
        create_config_setting(
            connection, "website", "https://foo.bar", library_id=library
        )
        create_config_setting(
            connection, "help_web", "https://foo.bar/helpme", library_id=library
        )
        create_config_setting(
            connection, "logo", "https://foo.bar/logo.png", library_id=library
        )
        create_config_setting(connection, "key-pair", "foo", library_id=library)
        create_config_setting(connection, "foo", "foo", library_id=library)
        create_config_setting(
            connection,
            "enabled_entry_points",
            json.dumps(["xyz", "abc"]),
            library_id=library,
        )

    # Run the up migration, and make sure settings column is added
    alembic_runner.migrate_up_one()
    assert column_exists(alembic_engine, "libraries", "settings_dict")

    # Make sure settings are migrated into table correctly
    with alembic_engine.connect() as connection:
        result = connection.execute("select settings_dict from libraries").fetchone()
        assert result is not None
        settings_dict = result.settings_dict
        assert len(settings_dict) == 3
        assert settings_dict["website"] == "https://foo.bar"
        assert settings_dict["help_web"] == "https://foo.bar/helpme"
        assert settings_dict["enabled_entry_points"] == ["xyz", "abc"]
