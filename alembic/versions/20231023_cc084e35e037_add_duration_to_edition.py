"""Add duration to edition

Revision ID: cc084e35e037
Revises: 0739d5558dda
Create Date: 2023-10-23 10:58:21.856412+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "cc084e35e037"
down_revision = "7fceb9488bc6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("editions", sa.Column("duration", sa.Float(), nullable=True))


def downgrade() -> None:
    op.drop_column("editions", "duration")
