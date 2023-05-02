"""Remove import coverage records without collections

Revision ID: f9985f6b7767
Revises: 5dcbc92c20b2
Create Date: 2023-05-01 10:07:45.737475+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "f9985f6b7767"
down_revision = "5dcbc92c20b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "DELETE FROM coveragerecords WHERE collection_id IS NULL AND operation='import'"
    )


def downgrade() -> None:
    pass
