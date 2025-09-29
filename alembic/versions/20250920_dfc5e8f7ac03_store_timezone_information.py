"""Store timezone information

Revision ID: dfc5e8f7ac03
Revises: 1290ca457ee4
Create Date: 2025-09-20 14:58:52.986883+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "dfc5e8f7ac03"
down_revision = "1290ca457ee4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "holds",
        "patron_last_notified",
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=True,
    )
    op.alter_column(
        "loans",
        "patron_last_notified",
        existing_type=postgresql.TIMESTAMP(),
        type_=sa.DateTime(timezone=True),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "loans",
        "patron_last_notified",
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=True,
    )
    op.alter_column(
        "holds",
        "patron_last_notified",
        existing_type=sa.DateTime(timezone=True),
        type_=postgresql.TIMESTAMP(),
        existing_nullable=True,
    )
