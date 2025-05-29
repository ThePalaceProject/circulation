"""Remove work_coverage table


Revision ID: 01b1e464a9d1
Revises: d671b95566fb
Create Date: 2025-05-28 17:19:45.090766+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy import DateTime, ForeignKey, Integer, String, Unicode
from sqlalchemy.dialects import postgresql

from palace.manager.sqlalchemy.model.coverage import BaseCoverageRecord

# revision identifiers, used by Alembic.
revision = "01b1e464a9d1"
down_revision = "d671b95566fb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        op.f("ix_workcoveragerecords_exception"), table_name="workcoveragerecords"
    )
    op.drop_index(
        op.f("ix_workcoveragerecords_operation"), table_name="workcoveragerecords"
    )
    op.drop_index(
        op.f("ix_workcoveragerecords_operation_work_id"),
        table_name="workcoveragerecords",
    )
    op.drop_index(
        op.f("ix_workcoveragerecords_status"), table_name="workcoveragerecords"
    )
    op.drop_index(
        op.f("ix_workcoveragerecords_timestamp"), table_name="workcoveragerecords"
    )
    op.drop_index(
        op.f("ix_workcoveragerecords_work_id"), table_name="workcoveragerecords"
    )
    op.drop_constraint(
        op.f("workcoveragerecords_work_id_operation_key"),
        table_name="workcoveragerecords",
    )
    op.drop_constraint(
        op.f("workcoveragerecords_work_id_fkey"), table_name="workcoveragerecords"
    )
    op.drop_table("workcoveragerecords")


def downgrade() -> None:
    op.create_table(
        "workcoveragerecords",
        sa.Column(
            "id",
            Integer,
            nullable=False,
            primary_key=True,
        ),
        sa.Column(
            "work_id",
            Integer,
            ForeignKey("works.id"),
            index=True,
        ),
        sa.Column(
            "operation",
            String(255),
            default=None,
            index=True,
        ),
        sa.Column(
            "timestamp",
            DateTime(timezone=True),
            index=True,
        ),
        sa.Column(
            "status",
            postgresql.ENUM(
                *BaseCoverageRecord.status_enum.enums,  #  type: ignore[attr-defined]
                name=BaseCoverageRecord.status_enum.name,
                create_type=False,
            ),
            index=True,
        ),
        sa.Column(
            "exception",
            Unicode,
            index=True,
        ),
    )

    op.create_index(
        op.f("ix_workcoveragerecords_operation_work_id"),
        "workcoveragerecords",
        columns=[
            "operation",
            "work_id",
        ],
    )

    op.create_unique_constraint(
        "workcoveragerecords_work_id_operation_key",
        columns=[
            "operation",
            "work_id",
        ],
        table_name="workcoveragerecords",
    )
