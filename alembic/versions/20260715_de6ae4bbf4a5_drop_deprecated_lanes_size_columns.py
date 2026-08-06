"""Drop deprecated lanes.size columns

Revision ID: de6ae4bbf4a5
Revises: dbeb42224c1b
Create Date: 2026-07-15 14:08:25.692742+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "de6ae4bbf4a5"
down_revision = "dbeb42224c1b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop the deprecated lane-size cache columns. They are no longer read or
    # written by the current code (release 1 marked them ``deferred`` and moved
    # ``size``'s default into the database), so no running release references
    # them. See the online-migration two-release sequence in CLAUDE.md.
    op.drop_column("lanes", "size_by_entrypoint")
    op.drop_column("lanes", "size")


def downgrade() -> None:
    # Recreate the columns in the state they had after release 1: ``size`` is
    # NOT NULL with a server_default of 0 (so existing rows are backfilled),
    # ``size_by_entrypoint`` is nullable JSON.
    op.add_column(
        "lanes",
        sa.Column(
            "size",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "lanes",
        sa.Column(
            "size_by_entrypoint",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=True,
        ),
    )
