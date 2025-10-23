"""Remove open_access_download_url column from licensepools

Revision ID: 4c2d754c04e9
Revises: dfc5e8f7ac03
Create Date: 2025-10-23 13:40:29.239298+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "4c2d754c04e9"
down_revision = "dfc5e8f7ac03"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove unused open_access_download_url column that was a stale cache.
    # This column was never invalidated when resources changed and had no
    # production code accessing it after removing better_open_access_pool_than
    # and updating active_license_pool logic.
    op.drop_column("licensepools", "open_access_download_url")


def downgrade() -> None:
    op.add_column(
        "licensepools",
        sa.Column(
            "open_access_download_url", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
