"""reset_checked_for_non_bisac_nonfiction_subjects

Everything on the Palace Marketplace / Feedbooks category scheme is stored with
type='BISAC', including codes that are not BISAC at all -- language and
territory categories such as INFEN000 ("English literature") and INFENUSA
("American and Canadian literature"), plus vendor codes like FBSACT000000.

Those codes cannot be resolved to a canonical BISAC heading, so classification
fell back to the distributor's name and hit the catch-all rule at the end of
BISACClassifier.FICTION, which reads "not filed under a Fiction heading,
therefore nonfiction". Each such subject was therefore stored with
fiction=False and cast a nonfiction vote on every work it was attached to --
outvoting the genuine FBFIC* fiction codes on the same book.

The classifier no longer applies the BISAC rulesets to an unresolvable code; it
defers to the keyword classifier instead, which recognises "literature" and
scores the INF* family as fiction. This migration resets checked=False on the
affected subjects so classify_unchecked_subjects re-scores them and recalculates
the works they are attached to.

Rather than approximating "not a real BISAC code" with a pattern, the selection
asks BISACClassifier itself and resets every subject stored as nonfiction that
the classifier no longer scores that way. That keeps the two definitions from
drifting apart: a pattern match on the identifier would, for instance, accept a
shape-valid but non-existent code like FBZZZ000000 that the classifier rejects,
leaving its fabricated nonfiction vote in place forever.

Scope stays narrow: only subjects currently holding fiction=False are examined,
so codes already scored as fiction or as unknown are left alone. The great
majority of the rows examined are legitimate nonfiction BISAC codes and are
untouched.

Revision ID: 52d1bbdd4671
Revises: de6ae4bbf4a5
Create Date: 2026-09-02 17:26:39.209822+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.core.classifier.bisac import BISACClassifier
from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "52d1bbdd4671"
down_revision = "de6ae4bbf4a5"
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
