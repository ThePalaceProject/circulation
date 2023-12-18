"""Migrate catalog services.

Revision ID: 1e46a5bc33b5
Revises: 382d7921f500
Create Date: 2023-11-21 17:48:04.535064+00:00

"""

from alembic import op
from core.marc import MARCExporter
from core.migration.migrate_external_integration import (
    _migrate_external_integration,
    _migrate_library_settings,
    get_configuration_settings,
    get_integrations,
    get_library_for_integration,
)
from core.migration.util import pg_update_enum

# revision identifiers, used by Alembic.
revision = "1e46a5bc33b5"
down_revision = "382d7921f500"
branch_labels = None
depends_on = None

CATALOG_GOAL = "CATALOG_GOAL"
old_goals_enum = ["PATRON_AUTH_GOAL", "LICENSE_GOAL", "DISCOVERY_GOAL"]
new_goals_enum = old_goals_enum + [CATALOG_GOAL]


def upgrade() -> None:
    # Add the new enum value to our goals enum
    pg_update_enum(
        op,
        "integration_configurations",
        "goal",
        "goals",
        old_goals_enum,
        new_goals_enum,
    )

    # Migrate the existing catalog services to integration configurations
    connection = op.get_bind()
    integrations = get_integrations(connection, "ils_catalog")
    for integration in integrations:
        _id, protocol, name = integration

        if protocol != "MARC Export":
            raise RuntimeError(f"Unknown catalog service '{protocol}'")

        (
            settings_dict,
            libraries_settings,
            self_test_result,
        ) = get_configuration_settings(connection, integration)

        # We moved the setting for update_frequency from the library settings to the integration settings.
        update_frequency: str | None = None
        for library_id, library_settings in libraries_settings.items():
            if "marc_update_frequency" in library_settings:
                frequency = library_settings["marc_update_frequency"]
                del library_settings["marc_update_frequency"]
                if update_frequency is not None and update_frequency != frequency:
                    raise RuntimeError(
                        f"Found different update frequencies for different libraries ({update_frequency}/{frequency})."
                    )
                update_frequency = frequency

        if update_frequency is not None:
            settings_dict["marc_update_frequency"] = update_frequency

        integration_configuration_id = _migrate_external_integration(
            connection,
            integration.name,
            MARCExporter.__name__,
            MARCExporter,
            CATALOG_GOAL,
            settings_dict,
            self_test_result,
        )

        integration_libraries = get_library_for_integration(connection, _id)
        for library in integration_libraries:
            _migrate_library_settings(
                connection,
                integration_configuration_id,
                library.library_id,
                libraries_settings[library.library_id],
                MARCExporter,
            )


def downgrade() -> None:
    pg_update_enum(
        op,
        "integration_configurations",
        "goal",
        "goals",
        new_goals_enum,
        old_goals_enum,
    )
