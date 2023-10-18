from dataclasses import dataclass
from typing import Any, Dict

from pytest_alembic import MigrationContext
from sqlalchemy.engine import Connection, Engine

from tests.migration.conftest import (
    CreateCollection,
    CreateConfigSetting,
    CreateExternalIntegration,
    CreateLibrary,
)


@dataclass
class IntegrationConfiguration:
    name: str
    goal: str
    id: int
    settings: Dict[str, Any]
    library_settings: Dict[int, Dict[str, Any]]


def query_integration_configurations(
    connection: Connection, goal: str, name: str
) -> IntegrationConfiguration:
    result = connection.execute(
        "select id, name, protocol, goal, settings from integration_configurations where goal=%s and name=%s",
        (goal, name),
    ).fetchone()
    assert result is not None

    library_results = connection.execute(
        "select library_id, settings from integration_library_configurations where parent_id=%s",
        result.id,
    ).fetchall()

    library_settings = {lr.library_id: lr.settings for lr in library_results}
    return IntegrationConfiguration(
        result.name, result.goal, result.id, result.settings, library_settings
    )


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
    create_collection: CreateCollection,
) -> None:
    """Test the migration of configurationsettings to integration_configurations for the licenses type goals"""
    # alembic_runner.set_revision("a9ed3f76d649")
    alembic_runner.migrate_down_to("a9ed3f76d649")
    with alembic_engine.connect() as connection:
        library_id = create_library(connection)
        integration_id = create_external_integration(
            connection, "Axis 360", "licenses", "Test B&T"
        )
        create_config_setting(connection, "username", "username", integration_id)
        create_config_setting(connection, "password", "password", integration_id)
        create_config_setting(connection, "url", "http://url", integration_id)
        create_config_setting(
            connection,
            "default_loan_duration",
            "77",
            integration_id,
            library_id,
            associate_library=True,
        )
        create_collection(connection, "Test B&T", integration_id, "ExternalAccountID")

        # Fake value, never used
        create_config_setting(
            connection, "external_account_id", "external_account_id", integration_id
        )

    alembic_runner.migrate_up_to("0af587ff8595")

    with alembic_engine.connect() as connection:
        configuration = query_integration_configurations(
            connection, "LICENSE_GOAL", "Test B&T"
        )

        assert configuration.settings == {
            "username": "username",
            "password": "password",
            "url": "http://url",
            "external_account_id": "ExternalAccountID",
        }
        assert configuration.library_settings == {
            library_id: {"default_loan_duration": 77}
        }


def test_key_rename(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
    create_collection: CreateCollection,
) -> None:
    alembic_runner.migrate_down_to("a9ed3f76d649")
    with alembic_engine.connect() as connection:
        integration_id = create_external_integration(
            connection, "Overdrive", "licenses", "Test Overdrive"
        )
        create_config_setting(
            connection, "overdrive_website_id", "website", integration_id
        )
        create_config_setting(
            connection, "overdrive_client_key", "overdrive_client_key", integration_id
        )
        create_config_setting(
            connection,
            "overdrive_client_secret",
            "overdrive_client_secret",
            integration_id,
        )
        create_collection(
            connection, "Test Overdrive", integration_id, "ExternalAccountID"
        )

        # Fake value, never used
        create_config_setting(
            connection, "external_account_id", "external_account_id", integration_id
        )

    alembic_runner.migrate_up_to("0af587ff8595")

    with alembic_engine.connect() as connection:
        configuration = query_integration_configurations(
            connection, "LICENSE_GOAL", "Test Overdrive"
        )

        assert configuration.settings == {
            "overdrive_website_id": "website",
            "overdrive_client_key": "overdrive_client_key",
            "overdrive_client_secret": "overdrive_client_secret",
            "external_account_id": "ExternalAccountID",
        }
