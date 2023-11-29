import pytest
from pytest_alembic import MigrationContext
from sqlalchemy.engine import Engine

from api.integration.registry.catalog_services import CatalogServicesRegistry
from core.integration.base import integration_settings_load
from core.marc import MARCExporter, MarcExporterLibrarySettings, MarcExporterSettings
from tests.migration.conftest import (
    CreateConfigSetting,
    CreateExternalIntegration,
    CreateLibrary,
)


def test_migration(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
) -> None:
    alembic_runner.migrate_down_to("1e46a5bc33b5")
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        lib_1_id = create_library(conn, "Test Library 1")
        lib_2_id = create_library(conn, "Test Library 2")
        ext_id = create_external_integration(
            conn,
            protocol="MARC Export",
            goal="ils_catalog",
            name="MARC Export Test",
        )

        create_config_setting(
            conn, "marc_update_frequency", "8", ext_id, lib_1_id, associate_library=True
        )
        create_config_setting(
            conn,
            "marc_organization_code",
            "org1",
            ext_id,
            lib_1_id,
            associate_library=True,
        )
        create_config_setting(
            conn, "include_summary", "true", ext_id, lib_1_id, associate_library=True
        )

        create_config_setting(
            conn,
            "marc_organization_code",
            "org2",
            ext_id,
            lib_2_id,
            associate_library=True,
        )
        create_config_setting(
            conn,
            "marc_web_client_url",
            "http://web.com",
            ext_id,
            lib_2_id,
            associate_library=True,
        )
        create_config_setting(
            conn,
            "include_simplified_genres",
            "true",
            ext_id,
            lib_2_id,
            associate_library=True,
        )

    alembic_runner.migrate_up_one()

    with alembic_engine.connect() as conn:
        rows = conn.execute(
            "select id, protocol, goal, settings from integration_configurations where name='MARC Export Test'"
        ).all()
        assert len(rows) == 1

        integration = rows[0]

        protocol_cls = CatalogServicesRegistry()[integration.protocol]
        assert protocol_cls == MARCExporter
        settings = integration_settings_load(
            protocol_cls.settings_class(), integration.settings
        )
        assert isinstance(settings, MarcExporterSettings)
        assert settings.update_frequency == 8

        rows = conn.execute(
            "select library_id, settings from integration_library_configurations where parent_id = %s order by library_id",
            integration.id,
        ).all()
        assert len(rows) == 2
        [library_1_integration, library_2_integration] = rows

        assert library_1_integration.library_id == lib_1_id
        assert library_2_integration.library_id == lib_2_id

        library_1_settings = integration_settings_load(
            protocol_cls.library_settings_class(), library_1_integration.settings
        )
        assert isinstance(library_1_settings, MarcExporterLibrarySettings)
        assert library_1_settings.organization_code == "org1"
        assert library_1_settings.include_summary is True

        library_2_settings = integration_settings_load(
            protocol_cls.library_settings_class(), library_2_integration.settings
        )
        assert isinstance(library_2_settings, MarcExporterLibrarySettings)
        assert library_2_settings.organization_code == "org2"
        assert library_2_settings.web_client_url == "http://web.com"
        assert library_2_settings.include_genres is True


def test_different_update_frequency(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
) -> None:
    alembic_runner.migrate_down_to("1e46a5bc33b5")
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        lib_1_id = create_library(conn, "Test Library 1")
        lib_2_id = create_library(conn, "Test Library 2")
        ext_id = create_external_integration(
            conn,
            protocol="MARC Export",
            goal="ils_catalog",
            name="MARC Export Test",
        )

        create_config_setting(
            conn, "marc_update_frequency", "8", ext_id, lib_1_id, associate_library=True
        )

        create_config_setting(
            conn,
            "marc_update_frequency",
            "12",
            ext_id,
            lib_2_id,
            associate_library=True,
        )

    with pytest.raises(RuntimeError) as excinfo:
        alembic_runner.migrate_up_one()

    assert "Found different update frequencies for different libraries (8/12)." in str(
        excinfo.value
    )


def test_unknown_protocol(
    alembic_runner: MigrationContext,
    alembic_engine: Engine,
    create_library: CreateLibrary,
    create_external_integration: CreateExternalIntegration,
    create_config_setting: CreateConfigSetting,
) -> None:
    alembic_runner.migrate_down_to("1e46a5bc33b5")
    alembic_runner.migrate_down_one()

    with alembic_engine.connect() as conn:
        ext_id = create_external_integration(
            conn,
            protocol="unknown",
            goal="ils_catalog",
            name="MARC Export Test",
        )

    with pytest.raises(RuntimeError) as excinfo:
        alembic_runner.migrate_up_one()

    assert "Unknown catalog service" in str(excinfo.value)
