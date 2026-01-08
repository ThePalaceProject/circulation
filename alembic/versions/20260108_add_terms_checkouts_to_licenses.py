"""Add terms_checkouts to licenses table

Revision ID: adea054e2ea1
Revises: 7c8e14813018
Create Date: 2026-01-08 00:00:00.000000+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "adea054e2ea1"
down_revision = "7c8e14813018"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "licenses",
        sa.Column("terms_checkouts", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("licenses", "terms_checkouts")
