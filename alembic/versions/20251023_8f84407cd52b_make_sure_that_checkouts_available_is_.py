"""Make sure that checkouts_available is not nullable

Revision ID: 8f84407cd52b
Revises: 6212e80c0fab
Create Date: 2025-10-23 18:02:34.605053+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "8f84407cd52b"
down_revision = "6212e80c0fab"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "licenses", "checkouts_available", existing_type=sa.INTEGER(), nullable=False
    )


def downgrade() -> None:
    op.alter_column(
        "licenses", "checkouts_available", existing_type=sa.INTEGER(), nullable=True
    )
