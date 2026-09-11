"""Drop customlist_sharedlibraries

Revision ID: 5b1f4f3c7979
Revises: 912c566f3383
Create Date: 2026-09-01 14:52:11.884213+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "5b1f4f3c7979"
down_revision = "912c566f3383"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Sharing moved to customlists.shared_locally one release ago, so no running
    # code (current or previous release) reads or writes this table.
    #
    # Deliberately not re-run here: the previous release stopped writing to this
    # table entirely, including when a list is unshared. Any list unshared since
    # that release still has its rows, so repeating the backfill would silently
    # re-share every one of them.
    op.drop_table("customlist_sharedlibraries")


def downgrade() -> None:
    # Recreated as create_all built it, then repopulated from shared_locally so
    # that older code sees the sharing state again. The recreate is also what
    # makes the alembic up/down consistency test pass: the preceding revision's
    # upgrade reads this table.
    op.create_table(
        "customlist_sharedlibraries",
        sa.Column(
            "customlist_id",
            sa.Integer(),
            sa.ForeignKey("customlists.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "library_id",
            sa.Integer(),
            sa.ForeignKey("libraries.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.UniqueConstraint("customlist_id", "library_id"),
    )
    op.create_index(
        "ix_customlist_sharedlibraries_customlist_id",
        "customlist_sharedlibraries",
        ["customlist_id"],
    )
    op.create_index(
        "ix_customlist_sharedlibraries_library_id",
        "customlist_sharedlibraries",
        ["library_id"],
    )
    op.execute(
        sa.text(
            """
            INSERT INTO customlist_sharedlibraries (customlist_id, library_id)
            SELECT c.id, l.id
            FROM customlists c CROSS JOIN libraries l
            WHERE c.shared_locally AND c.library_id IS DISTINCT FROM l.id
            """
        )
    )
