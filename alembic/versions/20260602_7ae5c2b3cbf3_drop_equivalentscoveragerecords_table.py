"""Drop equivalentscoveragerecords table

Revision ID: 7ae5c2b3cbf3
Revises: a6c85605404c
Create Date: 2026-06-02 17:49:35.182272+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

from palace.manager.sqlalchemy.model.coverage import BaseCoverageRecord

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
            # coverage_status is shared with coveragerecords; use postgresql.ENUM
            # with create_type=False so Alembic does not attempt to re-create it.
            postgresql.ENUM(
                *BaseCoverageRecord.status_enum.enums,  # type: ignore[attr-defined]
                name=BaseCoverageRecord.status_enum.name,
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
