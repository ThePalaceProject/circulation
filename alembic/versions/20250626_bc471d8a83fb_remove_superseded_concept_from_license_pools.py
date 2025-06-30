"""Remove superseded concept from license pools.

Revision ID: bc471d8a83fb
Revises: 87051f7b2905
Create Date: 2025-06-26 20:24:52.724565+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "bc471d8a83fb"
down_revision = "87051f7b2905"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("licensepools", "superceded")


def downgrade() -> None:
    # Initially create the column with a default of False, to
    # populate the value efficiently.
    op.add_column(
        "licensepools",
        sa.Column(
            "superceded",
            sa.BOOLEAN(),
            autoincrement=False,
            nullable=False,
            server_default=sa.false(),
        ),
    )
    # Then remove the database default constraint.
    op.alter_column(
        "licensepools",
        "superceded",
        existing_type=sa.BOOLEAN(),
        server_default=None,
    )
