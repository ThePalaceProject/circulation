"""Add status and type enum to licensepool

Revision ID: 2ec8857ae150
Revises: 8f84407cd52b
Create Date: 2025-11-13 14:29:34.740641+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "2ec8857ae150"
down_revision = "8f84407cd52b"
branch_labels = None
depends_on = None


LICENSEPOOL_TYPE_ENUM = sa.Enum(
    "metered", "unlimited", "aggregated", name="licensepooltype"
)
LICENSEPOOL_STATUS_ENUM = sa.Enum(
    "pre_order", "active", "exhausted", "removed", name="licensepoolstatus"
)


def upgrade() -> None:
    LICENSEPOOL_TYPE_ENUM.create(op.get_bind(), checkfirst=False)
    LICENSEPOOL_STATUS_ENUM.create(op.get_bind(), checkfirst=False)
    op.add_column(
        "licensepools",
        sa.Column(
            "type",
            LICENSEPOOL_TYPE_ENUM,
            server_default="metered",
            nullable=False,
        ),
    )
    op.add_column(
        "licensepools",
        sa.Column(
            "status",
            LICENSEPOOL_STATUS_ENUM,
            server_default="active",
            nullable=False,
        ),
    )

    # Make sure open_access column is not null, and set it to not nullable
    op.execute(
        sa.text("UPDATE licensepools SET open_access = false WHERE open_access IS NULL")
    )
    op.alter_column(
        "licensepools", "open_access", existing_type=sa.BOOLEAN(), nullable=False
    )


def downgrade() -> None:
    op.alter_column(
        "licensepools", "open_access", existing_type=sa.BOOLEAN(), nullable=True
    )
    op.drop_column("licensepools", "status")
    op.drop_column("licensepools", "type")
    LICENSEPOOL_TYPE_ENUM.drop(op.get_bind(), checkfirst=False)
    LICENSEPOOL_STATUS_ENUM.drop(op.get_bind(), checkfirst=False)
