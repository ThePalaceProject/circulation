"""Reset audience for works with mis-classified FB BISAC subjects

Works linked to BISAC subjects whose identifier begins with "FB" (e.g. FBJUV*, FBYA*)
were incorrectly assigned audience="Adult" before the scrubber fix landed. The
classify_unchecked_subjects repair task reclassified the subjects correctly, but a
structural side-effect in that task meant some works were never re-presented. This
migration resets work.audience to NULL for those works so the startup task can
re-run calculate_presentation() on them.

Revision ID: d856ff4dbefb
Revises: 05a95c828149
Create Date: 2026-05-12 22:11:05.399881+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "d856ff4dbefb"
down_revision = "05a95c828149"
branch_labels = None
depends_on = None

log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()

    result = conn.execute(
        sa.text(
            """
            UPDATE works
            SET audience = NULL
            WHERE audience = 'Adult'
              AND id IN (
                  SELECT DISTINCT lp.work_id
                  FROM licensepools lp
                  JOIN identifiers i ON i.id = lp.identifier_id
                  JOIN classifications c ON c.identifier_id = i.id
                  JOIN subjects s ON s.id = c.subject_id
                  WHERE s.type = 'BISAC'
                    AND s.identifier LIKE 'FB%'
                    AND s.audience IN ('Children', 'Young Adult')
              )
            RETURNING id
            """
        )
    )
    rows = list(result)
    log.info(
        f"Reset audience=NULL for {len(rows)} works with mis-classified FB BISAC subjects"
    )


def downgrade() -> None:
    # Cannot reliably restore the original audience values; intentionally a no-op.
    pass
