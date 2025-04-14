"""Remove unsafe characters summary

Revision ID: c3458e1ef9aa
Revises: 272da5f400de
Create Date: 2024-11-27 20:32:41.431147+00:00

"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "c3458e1ef9aa"
down_revision = "272da5f400de"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove any characters that are not XML safe from the summary_text field. The code has been
    # updated to filter out these characters, but this cleans up any existing data.
    # https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP
    op.execute(
        "UPDATE works SET summary_text = regexp_replace("
        "  summary_text, '[^\u0020-\ud7ff\u0009\u000a\u000d\ue000-\ufffd\U00010000-\U0010ffff]+', '', 'g'"
        ") WHERE "
        "summary_text ~ '[^\u0020-\ud7ff\u0009\u000a\u000d\ue000-\ufffd\U00010000-\U0010ffff]'"
    )


def downgrade() -> None:
    # No need to do anything on downgrade.
    pass
