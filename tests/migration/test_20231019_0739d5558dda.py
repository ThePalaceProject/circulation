from pytest_alembic import MigrationContext
from sqlalchemy.engine import Engine

from tests.migration.conftest import (
    CreateConfigSetting,
    CreateExternalIntegration,
    CreateLibrary,
)

MIGRATION_UID = "0739d5558dda"


def test_settings_deletion(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
    create_library: CreateLibrary,
) -> None:
    alembic_runner.migrate_down_to(MIGRATION_UID)
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        lib_id = create_library(conn, "Test")
        ext_id = create_external_integration(
            conn,
            protocol="api.google_analytics_provider",
            goal="analytics",
            name="Google Analytics Test",
        )
        key_id = create_config_setting(
            conn, "tracking_id", "trackingid", ext_id, lib_id, associate_library=True
        )

    alembic_runner.migrate_up_one()

    with alembic_engine.connect() as conn:
        assert (
            conn.execute(
                "SELECT id from externalintegrations where id=%s", ext_id
            ).first()
            is None
        )
        assert (
            conn.execute(
                "SELECT id from configurationsettings where external_integration_id=%s",
                ext_id,
            ).first()
            is None
        )
        assert (
            conn.execute(
                "SELECT externalintegration_id from externalintegrations_libraries where externalintegration_id=%s",
                ext_id,
            ).first()
            is None
        )
