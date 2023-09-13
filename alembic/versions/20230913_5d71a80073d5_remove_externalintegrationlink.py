"""Remove ExternalIntegrationLink.

Revision ID: 5d71a80073d5
Revises: 1c566151741f
Create Date: 2023-09-13 15:23:07.566404+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "5d71a80073d5"
down_revision = "1c566151741f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "ix_externalintegrationslinks_external_integration_id",
        table_name="externalintegrationslinks",
    )
    op.drop_index(
        "ix_externalintegrationslinks_library_id",
        table_name="externalintegrationslinks",
    )
    op.drop_index(
        "ix_externalintegrationslinks_other_integration_id",
        table_name="externalintegrationslinks",
    )
    op.drop_index(
        "ix_externalintegrationslinks_purpose", table_name="externalintegrationslinks"
    )
    op.drop_table("externalintegrationslinks")


def downgrade() -> None:
    op.create_table(
        "externalintegrationslinks",
        sa.Column("id", sa.INTEGER(), autoincrement=True, nullable=False),
        sa.Column(
            "external_integration_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
        sa.Column("library_id", sa.INTEGER(), autoincrement=False, nullable=True),
        sa.Column(
            "other_integration_id", sa.INTEGER(), autoincrement=False, nullable=True
        ),
        sa.Column("purpose", sa.VARCHAR(), autoincrement=False, nullable=True),
        sa.ForeignKeyConstraint(
            ["external_integration_id"],
            ["externalintegrations.id"],
            name="externalintegrationslinks_external_integration_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["library_id"],
            ["libraries.id"],
            name="externalintegrationslinks_library_id_fkey",
        ),
        sa.ForeignKeyConstraint(
            ["other_integration_id"],
            ["externalintegrations.id"],
            name="externalintegrationslinks_other_integration_id_fkey",
        ),
        sa.PrimaryKeyConstraint("id", name="externalintegrationslinks_pkey"),
    )
    op.create_index(
        "ix_externalintegrationslinks_purpose",
        "externalintegrationslinks",
        ["purpose"],
        unique=False,
    )
    op.create_index(
        "ix_externalintegrationslinks_other_integration_id",
        "externalintegrationslinks",
        ["other_integration_id"],
        unique=False,
    )
    op.create_index(
        "ix_externalintegrationslinks_library_id",
        "externalintegrationslinks",
        ["library_id"],
        unique=False,
    )
    op.create_index(
        "ix_externalintegrationslinks_external_integration_id",
        "externalintegrationslinks",
        ["external_integration_id"],
        unique=False,
    )
