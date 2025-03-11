"""Remove patron location

Revision ID: 16c016e599a0
Revises: 61df6012a5e6
Create Date: 2025-03-11 13:42:12.187155+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "16c016e599a0"
down_revision = "61df6012a5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Remove unused columns
    op.drop_index("ix_circulationevents_location", table_name="circulationevents")
    op.drop_column("circulationevents", "location")
    op.drop_index("ix_patrons_cached_neighborhood", table_name="patrons")
    op.drop_column("patrons", "cached_neighborhood")

    # Remove unused settings
    op.execute(
        "UPDATE integration_configurations set settings = settings - 'neighborhood_mode' "
        "where protocol = 'api.millenium_patron' and goal = 'PATRON_AUTH_GOAL' and settings ? 'neighborhood_mode'"
    )
    op.execute(
        "UPDATE integration_configurations set settings = settings - 'neighborhood' "
        "where protocol = 'api.simple_authentication' and goal = 'PATRON_AUTH_GOAL' and settings ? 'neighborhood'"
    )


def downgrade() -> None:
    # Add back the removed columns
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

    # No need to restore neighborhood settings. Defaults will be used.
