"""Add last_updated to licensepools

Revision ID: 9c3f2b3fba1b
Revises: adea054e2ea1
Create Date: 2026-01-21 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "9c3f2b3fba1b"
down_revision = "adea054e2ea1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "licensepools",
        sa.Column("last_updated", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        sa.text(
            """
            UPDATE licensepools
            SET last_updated = last_checked
            """
        )
    )


def downgrade() -> None:
    op.drop_column("licensepools", "last_updated")
