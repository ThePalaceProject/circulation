"""Increase circulation event type field length

Revision ID: 579786fecbf4
Revises: 603b8ebd6daf
Create Date: 2024-12-20 06:30:30.148748+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "579786fecbf4"
down_revision = "603b8ebd6daf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "circulationevents", "type", existing_type=sa.VARCHAR(32), type_=sa.VARCHAR(50)
    )


def downgrade() -> None:
    op.alter_column(
        "circulationevents", "type", existing_type=sa.VARCHAR(50), type_=sa.VARCHAR(32)
    )
