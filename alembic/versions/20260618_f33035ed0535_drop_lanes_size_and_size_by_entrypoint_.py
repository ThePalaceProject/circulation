"""Drop lanes.size and size_by_entrypoint (PP-4506)

Revision ID: f33035ed0535
Revises: a6c85605404c
Create Date: 2026-06-18 21:53:11.162505+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "f33035ed0535"
down_revision = "a6c85605404c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The cached lane-size estimates are no longer populated or read; the code
    # that maintained them was removed in the preceding release.
    op.drop_column("lanes", "size_by_entrypoint")
    op.drop_column("lanes", "size")


def downgrade() -> None:
    # Re-add size with a temporary server default so existing rows are populated
    # (the column is NOT NULL), then drop the default to match the original
    # schema (which relied on an application-side default only).
    op.add_column(
        "lanes",
        sa.Column("size", sa.Integer(), nullable=False, server_default="0"),
    )
    op.alter_column("lanes", "size", server_default=None)
    op.add_column(
        "lanes",
        sa.Column(
            "size_by_entrypoint",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=True,
        ),
    )
