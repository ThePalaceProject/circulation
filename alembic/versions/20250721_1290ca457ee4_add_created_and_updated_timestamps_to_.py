"""Add created and updated timestamps to editions

Revision ID: 1290ca457ee4
Revises: 5e33dc35b5b9
Create Date: 2025-07-21 18:29:35.463120+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1290ca457ee4"
down_revision = "5e33dc35b5b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "editions", sa.Column("created_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column(
        "editions", sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("editions", "updated_at")
    op.drop_column("editions", "created_at")
