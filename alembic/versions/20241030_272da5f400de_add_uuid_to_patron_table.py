"""Add UUID to patron table

Revision ID: 272da5f400de
Revises: 1938277e993f
Create Date: 2024-10-30 17:41:28.151677+00:00

"""

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision = "272da5f400de"
down_revision = "1938277e993f"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "patrons",
        sa.Column("uuid", UUID(as_uuid=True), nullable=False, default=uuid.uuid4),
    )


def downgrade() -> None:
    op.drop_column("patrons", "uuid")
