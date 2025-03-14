"""Add LicensePoolDeliveryMechanism.available

Revision ID: df27b4867e56
Revises: 61df6012a5e6
Create Date: 2025-03-14 18:22:01.825315+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "df27b4867e56"
down_revision = "61df6012a5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "licensepooldeliveries",
        sa.Column(
            "available", sa.Boolean(), server_default=sa.text("true"), nullable=False
        ),
    )


def downgrade() -> None:
    op.drop_column("licensepooldeliveries", "available")
