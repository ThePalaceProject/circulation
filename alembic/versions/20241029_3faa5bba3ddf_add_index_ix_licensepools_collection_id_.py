"""Add index ix_licensepools_collection_id_work_id

Revision ID: 3faa5bba3ddf
Revises: 1938277e993f
Create Date: 2024-10-29 15:29:56.588830+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "3faa5bba3ddf"
down_revision = "1938277e993f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "ix_licensepools_collection_id_work_id",
        "licensepools",
        ["collection_id", "work_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_licensepools_collection_id_work_id", table_name="licensepools")
