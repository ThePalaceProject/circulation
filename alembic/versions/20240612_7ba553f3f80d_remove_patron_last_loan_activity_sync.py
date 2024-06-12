"""Remove Patron.last_loan_activity_sync

Revision ID: 7ba553f3f80d
Revises: 50746a3bd243
Create Date: 2024-06-12 00:53:37.497861+00:00

"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "7ba553f3f80d"
down_revision = "50746a3bd243"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_column("patrons", "last_loan_activity_sync")


def downgrade() -> None:
    op.add_column(
        "patrons",
        sa.Column(
            "last_loan_activity_sync",
            postgresql.TIMESTAMP(timezone=True),
            autoincrement=False,
            nullable=True,
        ),
    )
