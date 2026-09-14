"""reset_checked_for_non_bisac_nonfiction_subjects

Resets checked=False on BISAC subjects whose stored fiction=False no longer
matches what BISACClassifier scores them as, so classify_unchecked_subjects
re-scores them and recalculates the works they are attached to.

These are subjects whose identifier is not a resolvable BISAC code. The Palace
Marketplace / Feedbooks scheme is stored as type='BISAC' but includes
non-subject codes such as INFEN000 ("English literature"), and classification
used to infer nonfiction from the distributor's name for those. It no longer
does, which leaves the stored values stale.

Selection asks the classifier rather than pattern-matching the identifier, so
the migration and the runtime cannot disagree about which codes are real.

Only subjects currently holding fiction=False are examined; most of those are
legitimate nonfiction BISAC codes and are left untouched.

Must ship in the same release as the classifier change. Run against the old
code, classify_unchecked_subjects would re-score these subjects under the old
rules and re-stamp checked=True, paying for a full reindex that changes
nothing.

Revision ID: 52d1bbdd4671
Revises: 912c566f3383
Create Date: 2026-09-02 17:26:39.209822+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.core.classifier.bisac import BISACClassifier
from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "52d1bbdd4671"
down_revision = "912c566f3383"
branch_labels = None
depends_on = None

log = migration_logger(revision)


def upgrade() -> None:
    conn = op.get_bind()

    candidates = conn.execute(
        sa.text(
            """
            SELECT id, identifier, name
            FROM subjects
            WHERE type = 'BISAC'
              AND checked
              AND fiction IS FALSE
            """
        )
    ).all()

    # Ask the classifier rather than matching a pattern against the identifier.
    # A pattern would accept a shape-valid but non-existent code such as
    # FBZZZ000000, which the classifier rejects, and its fabricated nonfiction
    # value would survive this repair.
    stale_ids = []
    for row in candidates:
        if not row.identifier and not row.name:
            # Nothing to classify. Subject.lookup will not create such a row,
            # but the columns are nullable, so don't assume.
            continue
        identifier, name = BISACClassifier.scrub_identifier_and_name(
            row.identifier, row.name
        )
        if BISACClassifier.is_fiction(identifier, name) is not False:
            stale_ids.append(row.id)
            log.info(
                f"Reset checked=False for subject id={row.id} "
                f"identifier={row.identifier!r} name={row.name!r}"
            )

    if stale_ids:
        conn.execute(
            sa.text("UPDATE subjects SET checked = false WHERE id = ANY(:ids)"),
            {"ids": stale_ids},
        )

    log.info(
        f"Reset checked=False for {len(stale_ids)} of {len(candidates)} "
        f"BISAC subjects stored as nonfiction"
    )


def downgrade() -> None:
    # The previous checked values are not recorded, and re-marking these
    # subjects checked would only re-suppress the reclassification this
    # migration exists to trigger. Intentionally a no-op.
    pass
