"""Make Loan.patron_id and Hold.patron_id non-nullable.

Revision ID: 58b0ae7f5b67
Revises: c3458e1ef9aa
Create Date: 2024-12-04 08:04:24.182444+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "58b0ae7f5b67"
down_revision = "c3458e1ef9aa"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        table_name="loans",
        column_name="patron_id",
        nullable=False,
    )

    op.alter_column(
        table_name="holds",
        column_name="patron_id",
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        table_name="loans",
        column_name="patron_id",
        nullable=True,
    )

    op.alter_column(
        table_name="holds",
        column_name="patron_id",
        nullable=True,
    )
