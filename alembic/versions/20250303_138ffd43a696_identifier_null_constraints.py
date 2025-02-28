"""Identifier null constraints

Revision ID: 138ffd43a696
Revises: 43e7239ab7a2
Create Date: 2025-03-03 16:28:52.272240+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "138ffd43a696"
down_revision = "43e7239ab7a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "identifiers", "type", existing_type=sa.VARCHAR(length=64), nullable=False
    )
    op.alter_column(
        "identifiers", "identifier", existing_type=sa.VARCHAR(), nullable=False
    )


def downgrade() -> None:
    op.alter_column(
        "identifiers", "identifier", existing_type=sa.VARCHAR(), nullable=True
    )
    op.alter_column(
        "identifiers", "type", existing_type=sa.VARCHAR(length=64), nullable=True
    )
