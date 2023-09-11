"""Remove self_hosted from licensepools

Revision ID: 1c566151741f
Revises: 2b672c6fb2b9
Create Date: 2023-08-31 16:13:54.935093+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "1c566151741f"
down_revision = "2b672c6fb2b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_licensepools_self_hosted", table_name="licensepools")
    op.drop_column("licensepools", "self_hosted")


def downgrade() -> None:
    op.add_column(
        "licensepools",
        sa.Column("self_hosted", sa.BOOLEAN(), autoincrement=False, nullable=False),
    )
    op.create_index(
        "ix_licensepools_self_hosted", "licensepools", ["self_hosted"], unique=False
    )
