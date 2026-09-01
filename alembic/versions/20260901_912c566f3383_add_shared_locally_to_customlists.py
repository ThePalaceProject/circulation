"""Add shared_locally to customlists

Revision ID: 912c566f3383
Revises: de6ae4bbf4a5
Create Date: 2026-09-01 14:34:44.569772+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "912c566f3383"
down_revision = "de6ae4bbf4a5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Sharing a custom list has always been all-or-nothing -- there is no way to share
    # one with a single library -- so the customlist_sharedlibraries join table is
    # replaced by a flag on the list itself. The server default is required, not just
    # the model's Python-side default: the previous release does not know about this
    # column and omits it from its INSERTs, which would violate NOT NULL while this
    # migration runs online.
    op.add_column(
        "customlists",
        sa.Column(
            "shared_locally", sa.Boolean(), server_default=sa.false(), nullable=False
        ),
    )

    # A list with at least one row in the join table was in a shared state. Lists whose
    # owning library has since been deleted (library_id IS NULL) are included on
    # purpose: deleting a library nulls customlists.library_id but leaves the join rows
    # for the libraries the list was shared *with*, so such a list is still shared.
    op.execute(
        sa.text(
            """
            UPDATE customlists
            SET shared_locally = true
            WHERE id IN (SELECT customlist_id FROM customlist_sharedlibraries)
            """
        )
    )


def downgrade() -> None:
    op.drop_column("customlists", "shared_locally")
