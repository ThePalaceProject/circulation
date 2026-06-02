"""Drop equivalentscoveragerecords table

Revision ID: 7ae5c2b3cbf3
Revises: d856ff4dbefb
Create Date: 2026-06-02 17:49:35.182272+00:00

"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "7ae5c2b3cbf3"
down_revision = "a6c85605404c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_table("equivalentscoveragerecords")


def downgrade() -> None:
    op.create_table(
        "equivalentscoveragerecords",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("equivalency_id", sa.Integer(), nullable=False),
        sa.Column("operation", sa.String(length=255), nullable=True),
        sa.Column("timestamp", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "status",
            # coverage_status is shared with the coveragerecords table, so we
            # must not attempt to re-create the type here.
            sa.Enum(
                "success",
                "transient failure",
                "persistent failure",
                "registered",
                name="coverage_status",
                create_type=False,
            ),
            nullable=True,
        ),
        sa.Column("exception", sa.Unicode(), nullable=True),
        sa.ForeignKeyConstraint(
            ["equivalency_id"],
            ["equivalents.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("equivalency_id", "operation"),
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_equivalency_id"),
        "equivalentscoveragerecords",
        ["equivalency_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_operation"),
        "equivalentscoveragerecords",
        ["operation"],
        unique=False,
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_status"),
        "equivalentscoveragerecords",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_equivalentscoveragerecords_timestamp"),
        "equivalentscoveragerecords",
        ["timestamp"],
        unique=False,
    )
