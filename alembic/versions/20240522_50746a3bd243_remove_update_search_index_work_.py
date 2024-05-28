"""Remove update-search-index work coverage records.

Revision ID: 50746a3bd243
Revises: f532186a3d48
Create Date: 2024-05-22 19:08:51.390547+00:00

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "50746a3bd243"
down_revision = "f532186a3d48"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Delete the coverage records with operation='update_search_index', since they are no longer used.
    op.execute(
        "DELETE FROM workcoveragerecords WHERE operation = 'update-search-index'"
    )


def downgrade() -> None:
    # These records are unused, so there is no need to restore them.
    pass
