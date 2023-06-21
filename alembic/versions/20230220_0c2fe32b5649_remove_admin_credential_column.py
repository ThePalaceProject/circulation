"""Remove admin.credential column

Revision ID: 0c2fe32b5649
Revises: 6f96516c7a7b
Create Date: 2023-02-20 12:36:15.204519+00:00

"""
import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision = "0c2fe32b5649"
down_revision = "6f96516c7a7b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("admins", "credential")


def downgrade() -> None:
    op.add_column("admins", sa.Column("credential", sa.Unicode(), nullable=True))
