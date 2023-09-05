import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

import pytest
from pytest_alembic import MigrationContext
from sqlalchemy.engine import Connection, Engine

from tests.migration.conftest import CreateLibrary


@dataclass
class IntegrationConfiguration:
    id: int
    settings: Dict[str, Any]


class CreateConfiguration(Protocol):
    def __call__(
        self, connection: Connection, protocol: str, name: str, settings: Dict[str, Any]
    ) -> IntegrationConfiguration:
        ...


@pytest.fixture
def create_integration_configuration() -> CreateConfiguration:
    def insert_config(
        connection: Connection, protocol: str, name: str, settings: Dict[str, Any]
    ) -> IntegrationConfiguration:
        connection.execute(
            "INSERT INTO integration_configurations (goal, protocol, name, settings, self_test_results) VALUES (%s, %s, %s, %s, '{}')",
            "LICENSE_GOAL",
            protocol,
            name,
            json.dumps(settings),
        )
        return fetch_config(connection, name=name)

    return insert_config


def fetch_config(
    connection: Connection, name: Optional[str] = None, parent_id: Optional[int] = None
) -> IntegrationConfiguration:
    if name is not None:
        _id, settings = connection.execute(  # type: ignore[misc]
            "SELECT id, settings FROM integration_configurations where name=%s", name
        ).fetchone()
    else:
        _id, settings = connection.execute(  # type: ignore[misc]
            "SELECT parent_id, settings FROM integration_library_configurations where parent_id=%s",
            parent_id,
        ).fetchone()
    return IntegrationConfiguration(_id, settings)


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
        config = create_integration_configuration(
            connection,
            "Axis 360",
            "axis-test-1",
            dict(
                verify_certificate="true",
                loan_limit="20",
                key="value",
            ),
        )

        library_id = create_library(connection)

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
            config.id,
            json.dumps(library_settings),
        )
        alembic_runner.migrate_up_one()

        axis_config = fetch_config(connection, name="axis-test-1")
        assert axis_config.settings["verify_certificate"] == True
        assert axis_config.settings["loan_limit"] == 20
        # Unknown settings remain as-is
        assert axis_config.settings["key"] == "value"

        odl_config = fetch_config(connection, parent_id=config.id)
        assert odl_config.settings["hold_limit"] == 30
        assert odl_config.settings["max_retry_count"] == 2
        assert odl_config.settings["ebook_loan_duration"] == 10
        assert odl_config.settings["default_loan_duration"] == 11
        # Unknown settings remain as-is
        assert odl_config.settings["unchanged"] == "value"
