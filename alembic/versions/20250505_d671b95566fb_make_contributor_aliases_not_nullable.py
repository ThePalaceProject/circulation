"""Make contributor.aliases not nullable

Revision ID: d671b95566fb
Revises: f36442df213d
Create Date: 2025-05-05 17:03:57.699199+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "d671b95566fb"
down_revision = "f36442df213d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "contributors",
        "aliases",
        existing_type=postgresql.ARRAY(sa.VARCHAR()),
        nullable=False,
    )


def downgrade() -> None:
    op.alter_column(
        "contributors",
        "aliases",
        existing_type=postgresql.ARRAY(sa.VARCHAR()),
        nullable=True,
    )
