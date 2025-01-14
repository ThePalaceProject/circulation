"""Set none subtitles to null

Revision ID: c800cc42184a
Revises: 579786fecbf4
Create Date: 2025-01-14 18:36:21.116427+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c800cc42184a"
down_revision = "579786fecbf4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("update editions set subtitle = null where subtitle = 'None'")


def downgrade() -> None:
    pass
