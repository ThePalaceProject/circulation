from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

import pytest

if TYPE_CHECKING:
    from pytest_alembic import MigrationContext
    from sqlalchemy.engine import Connection, Engine

    from tests.migration.conftest import CreateConfigSetting, CreateExternalIntegration


@pytest.fixture
def create_test_settings(
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
) -> Callable[..., int]:
    def fixture(
        connection: Connection,
        url: str,
        post: Optional[str] = None,
        set_post: bool = True,
    ) -> int:
        integration = create_external_integration(
            connection, protocol="api.millenium_patron"
        )
        create_config_setting(
            connection, integration_id=integration, key="url", value=url
        )
        if set_post:
            create_config_setting(
                connection,
                integration_id=integration,
                key="use_post_requests",
                value=post,
            )

        return integration

    return fixture


def assert_setting(connection: Connection, integration_id: int, value: str) -> None:
    result = connection.execute(
        "SELECT cs.value FROM configurationsettings cs join externalintegrations ei ON cs.external_integration_id = ei.id WHERE ei.id=%(id)s and cs.key='use_post_requests'",
        id=integration_id,
    )
    row = result.fetchone()
    assert row is not None
    assert row.value == value


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_test_settings: Callable[..., int],
) -> None:
    alembic_runner.migrate_down_to("5a425ebe026c")

    # Test down migration
    with alembic_engine.connect() as connection:
        integration = create_test_settings(
            connection, "https://vlc.thepalaceproject.org"
        )

    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        assert_setting(connection, integration, "false")

    # Test up migration
    with alembic_engine.connect() as connection:
        integration_dev = create_test_settings(
            connection, "http://vlc.dev.palaceproject.io/api", "false"
        )
        integration_staging = create_test_settings(
            connection, "https://vlc.staging.palaceproject.io/PATRONAPI", "false"
        )
        integration_local1 = create_test_settings(
            connection, "localhost:6500/PATRONAPI", "false"
        )
        integration_local2 = create_test_settings(
            connection, "http://localhost:6500/api", "false"
        )
        integration_prod = create_test_settings(
            connection, "https://vlc.thepalaceproject.org/anything...", "false"
        )
        integration_other = create_test_settings(
            connection, "https://vendor.millenium.com/PATRONAPI", "false"
        )
        integration_null = create_test_settings(
            connection, "http://vlc.dev.palaceproject.io/api"
        )
        integration_missing = create_test_settings(
            connection, "http://vlc.dev.palaceproject.io/api", set_post=False
        )

    alembic_runner.migrate_up_one()

    with alembic_engine.connect() as connection:
        assert_setting(connection, integration, "true")
        assert_setting(connection, integration_dev, "true")
        assert_setting(connection, integration_staging, "true")
        assert_setting(connection, integration_local1, "true")
        assert_setting(connection, integration_local2, "true")
        assert_setting(connection, integration_prod, "true")
        assert_setting(connection, integration_other, "false")
        assert_setting(connection, integration_null, "true")
        assert_setting(connection, integration_missing, "true")

    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as connection:
        assert_setting(connection, integration, "false")
        assert_setting(connection, integration_dev, "false")
        assert_setting(connection, integration_staging, "false")
        assert_setting(connection, integration_local1, "false")
        assert_setting(connection, integration_local2, "false")
        assert_setting(connection, integration_prod, "false")
        assert_setting(connection, integration_other, "false")
        assert_setting(connection, integration_null, "false")
        assert_setting(connection, integration_missing, "false")
