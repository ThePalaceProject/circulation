import json
from typing import Any, Dict

import pytest
from pytest_alembic import MigrationContext
from sqlalchemy.engine import Connection, Engine

from tests.migration.conftest import CreateLibrary


class CreateConfiguration:
    def __call__(
        self,
        connection: Connection,
        goal: str,
        protocol: str,
        name: str,
        settings: Dict[str, Any],
    ) -> int:
        integration_configuration = connection.execute(
            "INSERT INTO integration_configurations (goal, protocol, name, settings, self_test_results) VALUES (%s, %s, %s, %s, '{}') returning id",
            goal,
            protocol,
            name,
            json.dumps(settings),
        ).fetchone()
        assert integration_configuration is not None
        assert isinstance(integration_configuration.id, int)
        return integration_configuration.id


@pytest.fixture
def create_integration_configuration() -> CreateConfiguration:
    return CreateConfiguration()


def fetch_config(connection: Connection, _id: int) -> Dict[str, Any]:
    integration_config = connection.execute(
        "SELECT settings FROM integration_configurations where id=%s", _id
    ).fetchone()
    assert integration_config is not None
    assert isinstance(integration_config.settings, dict)
    return integration_config.settings


def fetch_library_config(
    connection: Connection, parent_id: int, library_id: int
) -> Dict[str, Any]:
    integration_lib_config = connection.execute(
        "SELECT parent_id, settings FROM integration_library_configurations where parent_id=%s and library_id=%s",
        parent_id,
        library_id,
    ).fetchone()
    assert integration_lib_config is not None
    assert isinstance(integration_lib_config.settings, dict)
    return integration_lib_config.settings


MIGRATION_UID = "2b672c6fb2b9"


def test_settings_coersion(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_integration_configuration: CreateConfiguration,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_UID)
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        config_id = create_integration_configuration(
            connection,
            "LICENSE_GOAL",
            "Axis 360",
            "axis-test-1",
            dict(
                verify_certificate="true",
                loan_limit="20",
                default_reservation_period="12",
                key="value",
            ),
        )

        # Test 2 library configs, to the same parent
        library_id = create_library(connection)
        library_id2 = create_library(connection)

        library_settings = dict(
            hold_limit="30",
            max_retry_count="2",
            ebook_loan_duration="10",
            default_loan_duration="11",
            unchanged="value",
        )
        connection.execute(
            "INSERT INTO integration_library_configurations (library_id, parent_id, settings) VALUES (%s, %s, %s)",
            library_id,
            config_id,
            json.dumps(library_settings),
        )
        library_settings = dict(
            hold_limit="31",
            max_retry_count="3",
            ebook_loan_duration="",
            default_loan_duration="12",
            unchanged="value1",
        )
        connection.execute(
            "INSERT INTO integration_library_configurations (library_id, parent_id, settings) VALUES (%s, %s, %s)",
            library_id2,
            config_id,
            json.dumps(library_settings),
        )

        other_config_settings = dict(
            verify_certificate="true",
            loan_limit="20",
            default_reservation_period="12",
            key="value",
        )
        other_config_id = create_integration_configuration(
            connection, "PATRON_AUTH_GOAL", "Other", "other-test", other_config_settings
        )
        connection.execute(
            "INSERT INTO integration_library_configurations (library_id, parent_id, settings) VALUES (%s, %s, %s)",
            library_id2,
            other_config_id,
            json.dumps(other_config_settings),
        )

        alembic_runner.migrate_up_one()

        axis_config = fetch_config(connection, config_id)
        assert axis_config["verify_certificate"] == True
        assert axis_config["loan_limit"] == 20
        assert axis_config["default_reservation_period"] == 12
        # Unknown settings remain as-is
        assert axis_config["key"] == "value"

        odl_config = fetch_library_config(
            connection, parent_id=config_id, library_id=library_id
        )
        assert odl_config["hold_limit"] == 30
        assert odl_config["max_retry_count"] == 2
        assert odl_config["ebook_loan_duration"] == 10
        assert odl_config["default_loan_duration"] == 11
        # Unknown settings remain as-is
        assert odl_config["unchanged"] == "value"

        odl_config2 = fetch_library_config(
            connection, parent_id=config_id, library_id=library_id2
        )
        assert odl_config2["hold_limit"] == 31
        assert odl_config2["max_retry_count"] == 3
        assert odl_config2["ebook_loan_duration"] is None
        assert odl_config2["default_loan_duration"] == 12
        # Unknown settings remain as-is
        assert odl_config2["unchanged"] == "value1"

        # Other integration is unchanged
        other_config = fetch_config(connection, other_config_id)
        assert other_config == other_config_settings
        other_library_config = fetch_library_config(
            connection, parent_id=other_config_id, library_id=library_id2
        )
        assert other_library_config == other_config_settings
