"""Add startup_tasks table

Revision ID: a5ee359c2d31
Revises: 08aba65e21e0
Create Date: 2026-02-10 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a5ee359c2d31"
down_revision = "08aba65e21e0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "startup_tasks",
        sa.Column("key", sa.Unicode(), nullable=False),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("run", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("key"),
    )


def downgrade() -> None:
    op.drop_table("startup_tasks")
