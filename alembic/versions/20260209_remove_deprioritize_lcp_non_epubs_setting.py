"""Remove deprioritize_lcp_non_epubs setting

Revision ID: a1b2c3d4e5f6
Revises: 9c3f2b3fba1b
Create Date: 2026-02-09

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "a1b2c3d4e5f6"
down_revision = "9c3f2b3fba1b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE integration_configurations SET settings = settings - 'deprioritize_lcp_non_epubs' "
        "WHERE settings ? 'deprioritize_lcp_non_epubs'"
    )


def downgrade() -> None:
    pass
