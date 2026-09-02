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

Scope is deliberately narrow: only non-canonical identifiers that currently hold
fiction=False. Canonical BISAC codes are untouched (the great majority of those
are legitimately nonfiction), as are non-canonical codes already holding
fiction=True or NULL, which are not implicated and whose reset would enlarge the
reindex for no benefit.

Revision ID: 52d1bbdd4671
Revises: de6ae4bbf4a5
Create Date: 2026-09-02 17:26:39.209822+00:00

"""

import sqlalchemy as sa
from alembic import op

from palace.manager.util.migration.helpers import migration_logger

# revision identifiers, used by Alembic.
revision = "52d1bbdd4671"
down_revision = "de6ae4bbf4a5"
branch_labels = None
depends_on = None

log = migration_logger(revision)

# An official BISAC code is three letters followed by six digits. Some
# distributors add an "FB" prefix and/or an "N" suffix, both of which
# BISACClassifier.scrub_identifier strips before looking the code up. Anything
# that does not match this shape is not a BISAC code.
CANONICAL_BISAC_CODE = r"^(FB)?[A-Z]{3}[0-9]{6}N?$"


def upgrade() -> None:
    conn = op.get_bind()

    result = conn.execute(
        sa.text(
            """
            UPDATE subjects
            SET checked = false
            WHERE type = 'BISAC'
              AND checked
              AND fiction IS FALSE
              AND identifier !~ :canonical_bisac_code
            RETURNING id, identifier, name
            """
        ),
        {"canonical_bisac_code": CANONICAL_BISAC_CODE},
    )
    rows = list(result)
    for row in rows:
        log.info(
            f"Reset checked=False for subject id={row[0]} "
            f"identifier={row[1]!r} name={row[2]!r}"
        )
    log.info(f"Reset checked=False for {len(rows)} non-BISAC nonfiction subjects")


def downgrade() -> None:
    # The previous checked values are not recorded, and re-marking these
    # subjects checked would only re-suppress the reclassification this
    # migration exists to trigger. Intentionally a no-op.
    pass
