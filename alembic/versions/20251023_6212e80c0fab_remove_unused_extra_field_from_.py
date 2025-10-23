"""Remove unused extra field from datasources

Revision ID: 6212e80c0fab
Revises: 4c2d754c04e9
Create Date: 2025-10-23 16:03:14.742447+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "6212e80c0fab"
down_revision = "4c2d754c04e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # The extra field was never used anywhere in the codebase. It was originally
    # added with a default value of {} and marked as NOT NULL, but no code ever
    # read from or wrote to this field.
    op.drop_column("datasources", "extra")


def downgrade() -> None:
    op.add_column(
        "datasources",
        sa.Column(
            "extra",
            postgresql.JSON(astext_type=sa.Text()),
            autoincrement=False,
            nullable=False,
        ),
    )
