"""reset_checked_for_all_fbjuv_subjects

Palace Marketplace sends BISAC codes with an "FBJUV" prefix (e.g. FBJUV000000,
FBJUV009000N). The previous migration only targeted a specific set of known
identifiers. This migration casts a wider net by resetting checked=False for
every BISAC subject whose identifier starts with "FBJUV", ensuring the
classify_unchecked_subjects task reclassifies any remaining affected subjects
with the corrected scrubber.

Revision ID: 05a95c828149
Revises: 45f74fdcec18
Create Date: 2026-04-27 20:17:51.182730+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "05a95c828149"
down_revision = "45f74fdcec18"
branch_labels = None
depends_on = None

log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()

    result = conn.execute(
        sa.text(
            "UPDATE subjects "
            "SET checked = false "
            "WHERE type = 'BISAC' "
            "  AND identifier LIKE 'FBJUV%' "
            "RETURNING id, identifier"
        )
    )
    for row in result:
        log.info(f"Reset checked=False for subject id={row[0]} identifier={row[1]!r}")


def downgrade() -> None:
    pass
