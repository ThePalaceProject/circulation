"""Remove Hold.external_identifier

Revision ID: 1938277e993f
Revises: 87901a6323d6
Create Date: 2024-10-15 19:47:55.697280+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "1938277e993f"
down_revision = "87901a6323d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("holds_external_identifier_key", "holds", type_="unique")
    op.drop_column("holds", "external_identifier")


def downgrade() -> None:
    op.add_column(
        "holds",
        sa.Column(
            "external_identifier", sa.VARCHAR(), autoincrement=False, nullable=True
        ),
    )
    op.create_unique_constraint(
        "holds_external_identifier_key", "holds", ["external_identifier"]
    )
