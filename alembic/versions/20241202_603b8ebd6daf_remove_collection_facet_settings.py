"""Remove collection facet settings

Revision ID: 603b8ebd6daf
Revises: 8dde64eab209
Create Date: 2024-12-02 19:42:23.775579+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "603b8ebd6daf"
down_revision = "8dde64eab209"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        sa.text(
            "UPDATE libraries set settings_dict = "
            "settings_dict - array['facets_enabled_collection', 'facets_default_collection']"
        )
    )


def downgrade() -> None:
    pass
