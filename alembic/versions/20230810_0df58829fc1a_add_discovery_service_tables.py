"""Add discovery service tables

Revision ID: 0df58829fc1a
Revises: 2f1a51aa0ee8
Create Date: 2023-08-10 15:49:36.784169+00:00

"""
import sqlalchemy as sa

from alembic import op
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


def downgrade() -> None:
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
