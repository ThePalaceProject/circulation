"""Add UUID to patron table

Revision ID: 272da5f400de
Revises: 3faa5bba3ddf
Create Date: 2024-10-30 17:41:28.151677+00:00

"""

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID

# revision identifiers, used by Alembic.
revision = "272da5f400de"
down_revision = "3faa5bba3ddf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "patrons",
        sa.Column("uuid", UUID(as_uuid=True), nullable=True, default=uuid.uuid4),
    )

    conn = op.get_bind()
    rows = conn.execute(sa.text("SELECT id from patrons")).all()

    for row in rows:
        uid = uuid.uuid4()
        conn.execute(
            sa.text(
                """
                UPDATE patrons SET uuid = %s
                WHERE id = %s
                """
            ),
            (
                uid,
                row.id,
            ),
        )

    op.alter_column(
        table_name="patrons",
        column_name="uuid",
        nullable=False,
    )


def downgrade() -> None:
    op.drop_column("patrons", "uuid")
