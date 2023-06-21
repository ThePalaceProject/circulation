"""Migrate license integrations to configuration settings

Revision ID: 0af587ff8595
Revises: b883671b7bc5
Create Date: 2023-05-31 12:34:42.550703+00:00

"""

from typing import Type

from alembic import op
from api.integration.registry.license_providers import LicenseProvidersRegistry
from core.integration.base import HasLibraryIntegrationConfiguration
from core.integration.settings import BaseSettings
from core.migration.migrate_external_integration import (
    _migrate_external_integration,
    _migrate_library_settings,
    get_configuration_settings,
    get_integrations,
    get_library_for_integration,
)

# revision identifiers, used by Alembic.
revision = "0af587ff8595"
down_revision = "b883671b7bc5"
branch_labels = None
depends_on = None


LICENSE_GOAL = "LICENSE_GOAL"


def upgrade() -> None:
    registry = LicenseProvidersRegistry()

    connection = op.get_bind()

    # Fetch all license type integrations
    # The old enum had 'licenses', the new enum has 'LICENSE_GOAL'
    integrations = get_integrations(connection, "licenses")
    for integration in integrations:
        _id, protocol, name = integration

        # Get the right API class for it
        api_class = registry.get(protocol, None)
        if not api_class:
            raise RuntimeError(f"Could not find API class for '{protocol}'")

        # Create the settings and library settings dicts from the configurationsettings
        settings_dict, library_settings, self_test_result = get_configuration_settings(
            connection, integration
        )

        # License type integrations take their external_account_id data from the collection.
        # The configurationsetting for it seems to be unused, so we take the value from the collection
        collection = connection.execute(
            "select id, external_account_id, name from collections where external_integration_id = %s",
            integration.id,
        ).fetchone()
        if not collection:
            raise RuntimeError(
                f"Could not fetch collection for integration {integration}"
            )
        settings_class: Type[BaseSettings] = api_class.settings_class()
        if "external_account_id" in settings_class.__fields__:
            settings_dict["external_account_id"] = collection.external_account_id

        # Write the configurationsettings into the integration_configurations table
        integration_id = _migrate_external_integration(
            connection,
            integration,
            api_class,
            LICENSE_GOAL,
            settings_dict,
            self_test_result,
            name=collection.name,
        )

        # Connect the collection to the settings
        connection.execute(
            "UPDATE collections SET integration_configuration_id=%s where id=%s",
            (integration_id, collection.id),
        )

        # If we have library settings too, then write each one into it's own row
        if issubclass(api_class, HasLibraryIntegrationConfiguration):
            integration_libraries = get_library_for_integration(connection, _id)
            for library in integration_libraries:
                _migrate_library_settings(
                    connection,
                    integration_id,
                    library.library_id,
                    library_settings[library.library_id],
                    api_class,
                )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        "DELETE from integration_configurations where goal = %s", LICENSE_GOAL
    )
