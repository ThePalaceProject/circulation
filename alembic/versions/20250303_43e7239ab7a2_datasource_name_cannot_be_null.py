"""Datasource name cannot be null

Revision ID: 43e7239ab7a2
Revises: 704dd5322783
Create Date: 2025-03-03 14:08:59.539631+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "43e7239ab7a2"
down_revision = "704dd5322783"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column("datasources", "name", existing_type=sa.VARCHAR(), nullable=False)


def downgrade() -> None:
    op.alter_column("datasources", "name", existing_type=sa.VARCHAR(), nullable=True)
