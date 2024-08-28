"""Ability to suppress works per library

Revision ID: 9d2dccb0d6ff
Revises: 1c9f519415b5
Create Date: 2024-02-16 17:08:52.146860+00:00

Note: this migration was updated when older migrations were deleted from the repository history,
and this was made the first migration by changing the down_revision to None.

See: https://alembic.sqlalchemy.org/en/latest/cookbook.html#building-an-up-to-date-database-from-scratch
"""
import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9d2dccb0d6ff"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "work_library_suppressions",
        sa.Column("work_id", sa.Integer(), nullable=False),
        sa.Column("library_id", sa.Integer(), nullable=False),
        sa.ForeignKeyConstraint(["library_id"], ["libraries.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["work_id"], ["works.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("work_id", "library_id"),
    )


def downgrade() -> None:
    op.drop_table("work_library_suppressions")
