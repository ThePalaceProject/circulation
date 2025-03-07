"""Stop storing location information

Revision ID: 80a70e6ec724
Revises: 61df6012a5e6
Create Date: 2025-03-07 02:32:46.583364+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "80a70e6ec724"
down_revision = "61df6012a5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index("ix_circulationevents_location", table_name="circulationevents")
    op.drop_column("circulationevents", "location")
    op.drop_index("ix_patrons_cached_neighborhood", table_name="patrons")
    op.drop_column("patrons", "cached_neighborhood")


def downgrade() -> None:
    op.add_column(
        "patrons",
        sa.Column(
            "cached_neighborhood", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.create_index(
        "ix_patrons_cached_neighborhood",
        "patrons",
        ["cached_neighborhood"],
        unique=False,
    )
    op.add_column(
        "circulationevents",
        sa.Column("location", sa.VARCHAR(), autoincrement=False, nullable=True),
    )
    op.create_index(
        "ix_circulationevents_location", "circulationevents", ["location"], unique=False
    )
