"""Remove collection facet settings

Revision ID: 603b8ebd6daf
Revises: 272da5f400de
Create Date: 2024-12-02 19:42:23.775579+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "603b8ebd6daf"
down_revision = "272da5f400de"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    conn.execute(
        "UPDATE integration_library_configurations set settings = settings::jsonb - 'facets_enabled_collection'"
    )
    conn.execute(
        "UPDATE integration_library_configurations set settings = settings::jsonb - 'facets_default_collection'"
    )


def downgrade() -> None:
    pass
