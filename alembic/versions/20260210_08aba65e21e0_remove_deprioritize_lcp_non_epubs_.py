"""remove_deprioritize_lcp_non_epubs_setting

Revision ID: 08aba65e21e0
Revises: 9c3f2b3fba1b
Create Date: 2026-02-10 19:09:05.304837+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "08aba65e21e0"
down_revision = "9c3f2b3fba1b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "UPDATE integration_configurations SET settings = settings - 'deprioritize_lcp_non_epubs' "
            "WHERE settings ? 'deprioritize_lcp_non_epubs'"
        )
    )


def downgrade() -> None:
    pass
