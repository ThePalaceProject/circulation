"""reset_checked_for_palace_marketplace_juvenile_nonfiction_subjects

Palace Marketplace sends Juvenile Fiction BISAC codes with an FB prefix and
N suffix (e.g. FBJUV000000N). The BISACClassifier.scrub_identifier() method
now strips the trailing N so these codes resolve to their canonical BISAC
entries (e.g. JUV000000 = "Juvenile Fiction / General") instead of falling
through to the Adult catch-all. Reset checked=False so the
classify_unchecked_subjects task reclassifies the affected subjects with the
corrected scrubber.

Revision ID: 45f74fdcec18
Revises: f98e4049c87d
Create Date: 2026-04-23 21:41:11.984357+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "45f74fdcec18"
down_revision = "f98e4049c87d"
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
            "  AND identifier IN ("
            "    'FBJUV000000N', 'FBJUV009000N', 'FBJUV009001N',"
            "    'FBJUV022000N', 'FBJUV038000N'"
            "  ) "
            "RETURNING id, identifier"
        )
    )
    for row in result:
        log.info(f"Reset checked=False for subject id={row[0]} identifier={row[1]!r}")


def downgrade() -> None:
    pass
