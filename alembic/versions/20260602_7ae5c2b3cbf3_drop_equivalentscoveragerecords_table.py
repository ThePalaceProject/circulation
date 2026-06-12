"""Drop equivalentscoveragerecords table

Revision ID: 7ae5c2b3cbf3
Revises: a6c85605404c
Create Date: 2026-06-02 17:49:35.182272+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "7ae5c2b3cbf3"
down_revision = "a6c85605404c"
branch_labels = None
depends_on = None

# Inlined (rather than imported from the model) so this migration stays
# reproducible even if BaseCoverageRecord.status_enum later changes. These are
# the name and members of the shared "coverage_status" enum as of this revision.
COVERAGE_STATUS_ENUM_NAME = "coverage_status"
COVERAGE_STATUS_ENUM_VALUES = (
    "success",
    "transient failure",
    "persistent failure",
    "registered",
)


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
                *COVERAGE_STATUS_ENUM_VALUES,
                name=COVERAGE_STATUS_ENUM_NAME,
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
