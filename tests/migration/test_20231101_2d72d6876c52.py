from typing import Any

import pytest
from pytest_alembic import MigrationContext
from sqlalchemy import inspect
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import IntegrityError

from core.model import json_serializer
from tests.migration.conftest import (
    CreateConfigSetting,
    CreateExternalIntegration,
    CreateLibrary,
)


def create_integration_configuration(
    connection: Connection,
    name: str,
    protocol: str,
    goal: str,
    settings: dict[str, Any] | None = None,
) -> int:
    if settings is None:
        settings = {}

    settings_str = json_serializer(settings)

    integration_configuration = connection.execute(
        "INSERT INTO integration_configurations (name, protocol, goal, settings, self_test_results) "
        "VALUES (%s, %s, %s, %s, '{}') returning id",
        name,
        protocol,
        goal,
        settings_str,
    ).fetchone()
    assert integration_configuration is not None
    assert isinstance(integration_configuration.id, int)
    return integration_configuration.id


def create_integration_library_configuration(
    connection: Connection,
    integration_id: int,
    library_id: int,
    settings: dict[str, Any] | None = None,
) -> None:
    if settings is None:
        settings = {}

    settings_str = json_serializer(settings)
    connection.execute(
        "INSERT INTO integration_library_configurations (parent_id, library_id, settings) "
        "VALUES (%s, %s, %s)",
        integration_id,
        library_id,
        settings_str,
    )


def create_collection_library(
    connection: Connection, collection_id: int, library_id: int
) -> None:
    connection.execute(
        "INSERT INTO collections_libraries (collection_id, library_id) "
        "VALUES (%s, %s)",
        collection_id,
        library_id,
    )


def create_collection(
    connection: Connection,
    name: str,
    integration_configuration_id: int,
    external_account_id: str | None = None,
    external_integration_id: int | None = None,
    parent_id: int | None = None,
) -> int:
    collection = connection.execute(
        "INSERT INTO collections "
        "(name, external_account_id, integration_configuration_id, external_integration_id, parent_id) VALUES "
        "(%s, %s, %s, %s, %s) "
        "returning id",
        name,
        external_account_id,
        integration_configuration_id,
        external_integration_id,
        parent_id,
    ).fetchone()
    assert collection is not None
    assert isinstance(collection.id, int)
    return collection.id


def column_exists(engine: Engine, table_name: str, column_name: str) -> bool:
    inspector = inspect(engine)
    columns = [column["name"] for column in inspector.get_columns(table_name)]
    return column_name in columns


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
) -> None:
    alembic_runner.migrate_down_to("2d72d6876c52")
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        # Test setup, create all the data we need for the migration
        library_1 = create_library(connection, "library_1")
        library_2 = create_library(connection, "library_2")

        integration_1_settings = {"data_source": "integration_1"}
        integration_1 = create_integration_configuration(
            connection,
            "integration_1",
            "OPDS Import",
            "LICENSE_GOAL",
            settings=integration_1_settings,
        )

        integration_2_settings = {
            "overdrive_website_id": "2",
            "overdrive_client_key": "3",
            "overdrive_client_secret": "4",
        }
        integration_2 = create_integration_configuration(
            connection,
            "collection_2",
            "Overdrive",
            "LICENSE_GOAL",
            settings=integration_2_settings,
        )
        integration_3_settings: dict[str, str] = {}
        integration_3 = create_integration_configuration(
            connection,
            "collection_1",
            "Overdrive",
            "LICENSE_GOAL",
            settings=integration_3_settings,
        )

        external_1 = create_external_integration(connection)
        external_2 = create_external_integration(connection)
        external_3 = create_external_integration(connection)

        create_config_setting(
            connection, "token_auth_endpoint", "http://token.com/auth", external_1
        )

        collection_1 = create_collection(
            connection, "collection_1", integration_1, "http://test.com", external_1
        )
        collection_2 = create_collection(
            connection, "collection_2", integration_2, "1", external_2
        )
        collection_3 = create_collection(
            connection, "collection_3", integration_3, "5656", external_3, collection_2
        )

        create_integration_library_configuration(connection, integration_1, library_1)
        create_integration_library_configuration(connection, integration_1, library_2)
        create_collection_library(connection, collection_1, library_1)
        create_collection_library(connection, collection_1, library_2)

        create_integration_library_configuration(connection, integration_2, library_2)
        create_collection_library(connection, collection_2, library_2)

        # Test that the collections_libraries table has the correct foreign key constraints
        with pytest.raises(IntegrityError) as excinfo:
            create_collection_library(connection, 99, 99)
        assert "violates foreign key constraint" in str(excinfo.value)

        # Make sure we have the data we expect before we run the migration
        integration_1_actual = connection.execute(
            "select name, settings from integration_configurations where id = (%s)",
            integration_1,
        ).fetchone()
        assert integration_1_actual is not None
        assert integration_1_actual.name == "integration_1"
        assert integration_1_actual.settings == integration_1_settings
        assert (
            column_exists(alembic_engine, "integration_configurations", "context")
            is False
        )

        integration_2_actual = connection.execute(
            "select name, settings from integration_configurations where id = (%s)",
            integration_2,
        ).fetchone()
        assert integration_2_actual is not None
        assert integration_2_actual.name == "collection_2"
        assert integration_2_actual.settings == integration_2_settings
        assert (
            column_exists(alembic_engine, "integration_configurations", "context")
            is False
        )

    # Run the migration
    alembic_runner.migrate_up_one()

    with alembic_engine.connect() as connection:
        # Make sure the migration updated the integration name, added the context column, and updated the settings
        # column to contain the external_account_id
        integration_1_actual = connection.execute(
            "select name, settings, context from integration_configurations where id = (%s)",
            integration_1,
        ).fetchone()
        assert integration_1_actual is not None
        assert integration_1_actual.name == "collection_1"
        assert integration_1_actual.settings != integration_1_settings
        assert integration_1_actual.settings == {
            "data_source": "integration_1",
            "external_account_id": "http://test.com",
        }
        assert integration_1_actual.context == {
            "token_auth_endpoint": "http://token.com/auth"
        }

        integration_2_actual = connection.execute(
            "select name, settings, context from integration_configurations where id = (%s)",
            integration_2,
        ).fetchone()
        assert integration_2_actual is not None
        assert integration_2_actual.name == "collection_2"
        assert integration_2_actual.settings != integration_2_settings
        assert integration_2_actual.settings == {
            "overdrive_website_id": "2",
            "overdrive_client_key": "3",
            "overdrive_client_secret": "4",
            "external_account_id": "1",
        }
        assert integration_2_actual.context == {}

        integration_3_actual = connection.execute(
            "select name, settings, context from integration_configurations where id = (%s)",
            integration_3,
        ).fetchone()
        assert integration_3_actual is not None
        assert integration_3_actual.name == "collection_3"
        assert integration_3_actual.settings != integration_3_settings
        assert integration_3_actual.settings == {
            "external_account_id": "5656",
        }
        assert integration_3_actual.context == {}

        # The foreign key constraints have been removed from the collections_libraries table
        create_collection_library(connection, 99, 99)

    # If we try to run the migration, it will fail when it tries to add back the foreign key constraints
    with pytest.raises(IntegrityError):
        alembic_runner.migrate_down_one()

    # But if we remove the data that violates the foreign key constraints, the migration will run successfully
    with alembic_engine.connect() as connection:
        connection.execute(
            "delete from collections_libraries where collection_id = 99 and library_id = 99"
        )
    alembic_runner.migrate_down_one()
