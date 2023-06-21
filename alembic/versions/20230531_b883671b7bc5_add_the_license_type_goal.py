"""Add the license type goal

Revision ID: b883671b7bc5
Revises: 0a1c9c3f5dd2
Create Date: 2023-05-31 10:50:32.045821+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "b883671b7bc5"
down_revision = "0a1c9c3f5dd2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # We need to use an autocommit blcok since the next migration is going to use
    # the new enum value immediately, so we must ensure the value is commited
    # before the next migration runs
    with op.get_context().autocommit_block():
        op.execute(f"ALTER TYPE goals ADD VALUE IF NOT EXISTS 'LICENSE_GOAL'")
        op.add_column(
            "collections",
            sa.Column("integration_configuration_id", sa.Integer(), nullable=True),
        )
        op.create_index(
            op.f("ix_collections_integration_configuration_id"),
            "collections",
            ["integration_configuration_id"],
            unique=True,
        )
        op.create_foreign_key(
            None,
            "collections",
            "integration_configurations",
            ["integration_configuration_id"],
            ["id"],
            ondelete="SET NULL",
        )


def downgrade() -> None:
    """There is no way to drop single values from an Enum from postgres"""
    op.drop_constraint(
        "collections_integration_configuration_id_fkey",
        "collections",
        type_="foreignkey",
    )
    op.drop_index(
        op.f("ix_collections_integration_configuration_id"), table_name="collections"
    )
    op.drop_column("collections", "integration_configuration_id")
