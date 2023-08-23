"""Add discovery service tables

Revision ID: 0df58829fc1a
Revises: 2f1a51aa0ee8
Create Date: 2023-08-10 15:49:36.784169+00:00

"""
import sqlalchemy as sa

from alembic import op
from api.discovery.opds_registration import OpdsRegistrationService
from core.migration.migrate_external_integration import (
    _migrate_external_integration,
    get_configuration_settings,
    get_integrations,
    get_library_for_integration,
)
from core.migration.util import drop_enum, pg_update_enum

# revision identifiers, used by Alembic.
revision = "0df58829fc1a"
down_revision = "2f1a51aa0ee8"
branch_labels = None
depends_on = None

old_goals_enum = [
    "PATRON_AUTH_GOAL",
    "LICENSE_GOAL",
]

new_goals_enum = old_goals_enum + ["DISCOVERY_GOAL"]


def upgrade() -> None:
    op.create_table(
        "discovery_service_registrations",
        sa.Column(
            "status",
            sa.Enum("SUCCESS", "FAILURE", name="registrationstatus"),
            nullable=False,
        ),
        sa.Column(
            "stage",
            sa.Enum("TESTING", "PRODUCTION", name="registrationstage"),
            nullable=False,
        ),
        sa.Column("web_client", sa.Unicode(), nullable=True),
        sa.Column("short_name", sa.Unicode(), nullable=True),
        sa.Column("shared_secret", sa.Unicode(), nullable=True),
        sa.Column("integration_id", sa.Integer(), nullable=False),
        sa.Column("library_id", sa.Integer(), nullable=False),
        sa.Column("vendor_id", sa.Unicode(), nullable=True),
        sa.ForeignKeyConstraint(
            ["integration_id"], ["integration_configurations.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["library_id"], ["libraries.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("integration_id", "library_id"),
    )
    pg_update_enum(
        op,
        "integration_configurations",
        "goal",
        "goals",
        old_goals_enum,
        new_goals_enum,
    )

    # Migrate data
    connection = op.get_bind()
    external_integrations = get_integrations(connection, "discovery")
    for external_integration in external_integrations:
        # This should always be the case, but we want to make sure
        assert external_integration.protocol == "OPDS Registration"

        # Create the settings and library settings dicts from the configurationsettings
        settings_dict, library_settings, self_test_result = get_configuration_settings(
            connection, external_integration
        )

        # Write the configurationsettings into the integration_configurations table
        integration_configuration_id = _migrate_external_integration(
            connection,
            external_integration,
            OpdsRegistrationService,
            "DISCOVERY_GOAL",
            settings_dict,
            self_test_result,
        )

        # Get the libraries that are associated with this external integration
        interation_libraries = get_library_for_integration(
            connection, external_integration.id
        )

        vendor_id = settings_dict.get("vendor_id")

        # Write the library settings into the discovery_service_registrations table
        for library in interation_libraries:
            library_id = library.library_id
            library_settings_dict = library_settings[library_id]

            status = library_settings_dict.get("library-registration-status")
            if status is None:
                status = "FAILURE"
            else:
                status = status.upper()

            stage = library_settings_dict.get("library-registration-stage")
            if stage is None:
                stage = "TESTING"
            else:
                stage = stage.upper()

            web_client = library_settings_dict.get("library-registration-web-client")
            short_name = library_settings_dict.get("username")
            shared_secret = library_settings_dict.get("password")

            connection.execute(
                "insert into discovery_service_registrations "
                "(status, stage, web_client, short_name, shared_secret, integration_id, library_id, vendor_id) "
                "values (%s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    status,
                    stage,
                    web_client,
                    short_name,
                    shared_secret,
                    integration_configuration_id,
                    library_id,
                    vendor_id,
                ),
            )


def downgrade() -> None:
    connection = op.get_bind()
    connection.execute(
        "DELETE from integration_configurations where goal = %s", "DISCOVERY_GOAL"
    )

    op.drop_table("discovery_service_registrations")
    drop_enum(op, "registrationstatus")
    drop_enum(op, "registrationstage")
    pg_update_enum(
        op,
        "integration_configurations",
        "goal",
        "goals",
        new_goals_enum,
        old_goals_enum,
    )
