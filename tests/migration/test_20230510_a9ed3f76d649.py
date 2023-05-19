from __future__ import annotations

import json
from typing import TYPE_CHECKING

from sqlalchemy import inspect

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Engine

    from tests.migration.conftest import (
        CreateConfigSetting,
        CreateExternalIntegration,
        CreateLibrary,
    )


def assert_tables_exist(alembic_engine: Engine) -> None:
    # We should have the tables for this migration
    insp = inspect(alembic_engine)
    assert "integration_configurations" in insp.get_table_names()
    assert "integration_library_configurations" in insp.get_table_names()
    assert "integration_errors" in insp.get_table_names()

    # We should have the enum defined in this migration
    with alembic_engine.connect() as connection:
        result = connection.execute("SELECT * FROM pg_type WHERE typname = 'goals'")
        assert result.rowcount == 1
        result = connection.execute("SELECT * FROM pg_type WHERE typname = 'status'")
        assert result.rowcount == 1


def assert_tables_dont_exist(alembic_engine: Engine) -> None:
    # We should not have the tables for this migration
    insp = inspect(alembic_engine)
    assert "integration_configurations" not in insp.get_table_names()
    assert "integration_library_configurations" not in insp.get_table_names()
    assert "integration_errors" not in insp.get_table_names()

    # We should not have the enum defined in this migration
    with alembic_engine.connect() as connection:
        result = connection.execute("SELECT * FROM pg_type WHERE typname = 'goals'")
        assert result.rowcount == 0
        result = connection.execute("SELECT * FROM pg_type WHERE typname = 'status'")
        assert result.rowcount == 0


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
) -> None:
    # Migrate to just before our migration
    alembic_runner.migrate_down_to("a9ed3f76d649")
    assert_tables_exist(alembic_engine)

    # Migrate down past our migration, running the downgrade migration
    alembic_runner.migrate_down_one()
    assert_tables_dont_exist(alembic_engine)

    # Insert configuration settings for testing
    with alembic_engine.connect() as connection:
        # Set up two libraries
        library = create_library(connection)
        library2 = create_library(connection)

        # Set up four integrations
        sip_integration = create_external_integration(
            connection, "api.sip", "patron_auth", "Integration 1"
        )
        millenium_integration = create_external_integration(
            connection, "api.millenium_patron", "patron_auth", "Integration 2"
        )
        simple_integration = create_external_integration(
            connection, "api.simple_authentication", "patron_auth", "Integration 3"
        )
        unrelated_integration = create_external_integration(
            connection, "unrelated", "other_goal", "Integration 4"
        )

        # Add configuration settings for the sip integration
        create_config_setting(connection, "setting1", "value1", sip_integration)
        create_config_setting(connection, "url", "sip url", sip_integration)
        create_config_setting(
            connection, "institution_id", "institution", sip_integration
        )
        create_config_setting(
            connection,
            "self_test_results",
            json.dumps({"test": "test"}),
            sip_integration,
        )
        create_config_setting(
            connection, "patron status block", "false", sip_integration
        )
        create_config_setting(
            connection, "identifier_barcode_format", "", sip_integration
        )
        create_config_setting(
            connection, "institution_id", "bar", sip_integration, library
        )

        # Add configuration settings for the millenium integration
        create_config_setting(connection, "setting2", "value2", millenium_integration)
        create_config_setting(
            connection, "url", "https://url.com", millenium_integration
        )
        create_config_setting(
            connection, "verify_certificate", "false", millenium_integration
        )
        create_config_setting(
            connection, "use_post_requests", "true", millenium_integration
        )
        create_config_setting(
            connection,
            "identifier_blacklist",
            json.dumps(["a", "b", "c"]),
            millenium_integration,
        )
        create_config_setting(
            connection,
            "library_identifier_field",
            "foo",
            millenium_integration,
            library,
        )

        # Add configuration settings for the simple integration
        create_config_setting(connection, "test_identifier", "123", simple_integration)
        create_config_setting(connection, "test_password", "456", simple_integration)

        # Associate the millenium integration with the library
        connection.execute(
            "INSERT INTO externalintegrations_libraries (library_id, externalintegration_id) VALUES (%s, %s)",
            (library, millenium_integration),
        )

        # Associate the simple integration with library 2
        connection.execute(
            "INSERT INTO externalintegrations_libraries (library_id, externalintegration_id) VALUES (%s, %s)",
            (library2, simple_integration),
        )

    # Migrate back up, running our upgrade migration
    alembic_runner.migrate_up_one()
    assert_tables_exist(alembic_engine)

    # Check that the configuration settings were migrated correctly
    with alembic_engine.connect() as connection:
        # Check that we have the correct number of integrations
        integrations = connection.execute(
            "SELECT * FROM integration_configurations",
        )
        assert integrations.rowcount == 3

        # Check that the sip integration was migrated correctly
        # The unknown setting 'setting1' was dropped, self test results were migrated, and the patron status block
        # setting was renamed, based on the field alias.
        sip_result = connection.execute(
            "SELECT protocol, goal, settings, self_test_results FROM integration_configurations WHERE name = %s",
            ("Integration 1",),
        ).fetchone()
        assert sip_result is not None
        assert sip_result[0] == "api.sip"
        assert sip_result[1] == "PATRON_AUTH_GOAL"
        assert sip_result[2] == {
            "patron_status_block": False,
            "url": "sip url",
        }
        assert sip_result[3] == {"test": "test"}

        # Check that the millenium integration was migrated correctly
        # The unknown setting 'setting2' was dropped, the list and bool values were serialized correctly, and
        # the empty self test results were migrated as an empty dict.
        millenium_result = connection.execute(
            "SELECT protocol, goal, settings, self_test_results, id FROM integration_configurations WHERE name = %s",
            ("Integration 2",),
        ).fetchone()
        assert millenium_result is not None
        assert millenium_result[0] == "api.millenium_patron"
        assert millenium_result[1] == "PATRON_AUTH_GOAL"
        assert millenium_result[2] == {
            "url": "https://url.com",
            "verify_certificate": False,
            "use_post_requests": True,
            "identifier_blacklist": ["a", "b", "c"],
        }
        assert millenium_result[3] == {}

        # Check that the simple integration was migrated correctly
        simple_result = connection.execute(
            "SELECT protocol, goal, settings, self_test_results, id FROM integration_configurations WHERE name = %s",
            ("Integration 3",),
        ).fetchone()
        assert simple_result is not None
        assert simple_result[0] == "api.simple_authentication"
        assert simple_result[1] == "PATRON_AUTH_GOAL"
        assert simple_result[2] == {
            "test_identifier": "123",
            "test_password": "456",
        }
        assert simple_result[3] == {}

        # Check that we have the correct number of library integrations
        # The SIP integration has library settings, but no association with a library, so no
        # library integration was created for it. And the simple auth integration has a library
        # association, but no library settings, so we do create a integration with no settings for it.
        integrations = connection.execute(
            "SELECT parent_id, library_id, settings FROM integration_library_configurations ORDER BY library_id asc",
        )
        assert integrations.rowcount == 2

        # Check that the millenium integration was migrated correctly
        [
            millenium_library_integration,
            simple_library_integration,
        ] = integrations.fetchall()
        assert millenium_library_integration is not None
        assert millenium_library_integration[0] == millenium_result[4]
        assert millenium_library_integration[1] == library
        assert millenium_library_integration[2] == {
            "library_identifier_field": "foo",
        }

        assert simple_library_integration is not None
        assert simple_library_integration[0] == simple_result[4]
        assert simple_library_integration[1] == library2
        assert simple_library_integration[2] == {}
