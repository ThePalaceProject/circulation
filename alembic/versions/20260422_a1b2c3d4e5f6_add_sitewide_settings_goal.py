"""Add SITEWIDE_SETTINGS to Goals enum

Revision ID: a1b2c3d4e5f6
Revises: 05a95c828149
Create Date: 2026-04-22 00:00:00.000000+00:00

"""

from alembic import op

from palace.manager.util.migration.helpers import pg_update_enum

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "05a95c828149"
branch_labels = None
depends_on = None

old_options = (
    "PATRON_AUTH_GOAL",
    "LICENSE_GOAL",
    "DISCOVERY_GOAL",
    "CATALOG_GOAL",
    "METADATA_GOAL",
)
new_options = old_options + ("SITEWIDE_SETTINGS",)


def upgrade() -> None:
    pg_update_enum(
        op, "integration_configurations", "goal", "goals", old_options, new_options
    )


def downgrade() -> None:
    op.execute(
        "DELETE FROM integration_configurations WHERE goal = 'SITEWIDE_SETTINGS'"
    )
    pg_update_enum(
        op, "integration_configurations", "goal", "goals", new_options, old_options
    )
