"""Add server_default to lanes.size

Revision ID: dbeb42224c1b
Revises: a6c85605404c
Create Date: 2026-07-15 13:42:28.828886+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "dbeb42224c1b"
down_revision = "a6c85605404c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ``lanes.size`` is deprecated and no longer read or written by the current
    # code, but it is NOT NULL with no database default. Moving the default into
    # the database lets the ORM omit the column from INSERTs (it previously
    # supplied a Python-side ``default=0``), so a follow-up release can drop the
    # column without breaking the still-running previous release's inserts.
    #
    # ``SET DEFAULT`` is a catalog-only change in Postgres: instant, no table
    # rewrite. It is backwards-compatible because the previous release still
    # explicitly writes ``size=0``, which simply overrides the new default.
    op.alter_column("lanes", "size", server_default=sa.text("0"))


def downgrade() -> None:
    op.alter_column("lanes", "size", server_default=None)
