"""migrate metadata services

Revision ID: 993729d4bf97
Revises: 735bf6ced8b9
Create Date: 2024-01-24 23:51:13.464107+00:00

"""
from alembic import op
from api.integration.registry.metadata import MetadataRegistry
from core.integration.base import HasLibraryIntegrationConfiguration
from core.migration.migrate_external_integration import (
    _migrate_external_integration,
    _migrate_library_settings,
    get_configuration_settings,
    get_integrations,
    get_library_for_integration,
)
from core.migration.util import pg_update_enum

# revision identifiers, used by Alembic.
revision = "993729d4bf97"
down_revision = "735bf6ced8b9"
branch_labels = None
depends_on = None

METADATA_GOAL = "METADATA_GOAL"
old_goals_enum = ["PATRON_AUTH_GOAL", "LICENSE_GOAL", "DISCOVERY_GOAL", "CATALOG_GOAL"]
new_goals_enum = old_goals_enum + [METADATA_GOAL]


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

    # Migrate the existing metadata services to integration configurations
    connection = op.get_bind()
    registry = MetadataRegistry()
    integrations = get_integrations(connection, "metadata")
    for integration in integrations:
        _id, protocol, name = integration
        protocol_class = registry[protocol]

        (
            settings_dict,
            libraries_settings,
            self_test_result,
        ) = get_configuration_settings(connection, integration)

        updated_protocol = registry.get_protocol(protocol_class)
        if updated_protocol is None:
            raise RuntimeError(f"Unknown metadata service '{protocol}'")
        integration_configuration_id = _migrate_external_integration(
            connection,
            integration.name,
            updated_protocol,
            protocol_class,
            METADATA_GOAL,
            settings_dict,
            self_test_result,
        )

        integration_libraries = get_library_for_integration(connection, _id)
        for library in integration_libraries:
            if issubclass(protocol_class, HasLibraryIntegrationConfiguration):
                _migrate_library_settings(
                    connection,
                    integration_configuration_id,
                    library.library_id,
                    libraries_settings[library.library_id],
                    protocol_class,
                )
            else:
                raise RuntimeError(
                    f"Protocol not expected to have library settings '{protocol}'"
                )


def downgrade() -> None:
    # Remove the new enum value from our goals enum.
    pg_update_enum(
        op,
        "integration_configurations",
        "goal",
        "goals",
        new_goals_enum,
        old_goals_enum,
    )
