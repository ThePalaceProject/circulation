"""Add asynctasks table

Revision ID: b2353c25a95e
Revises: 9d2dccb0d6ff
Create Date: 2024-03-13 21:48:45.911507+00:00

"""
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "b2353c25a95e"
down_revision = "9d2dccb0d6ff"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "asynctasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "READY",
                "PROCESSING",
                "SUCCESS",
                "FAILURE",
                name="asynctaskstatus",
            ),
            nullable=False,
        ),
        sa.Column(
            "task_type",
            sa.Enum(
                "INVENTORY_REPORT",
                name="asynctasktype",
            ),
            nullable=False,
        ),
        sa.Column("processing_start_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_end_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status_details", sa.Unicode(), nullable=True),
        sa.Column("data", postgresql.JSONB(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_asynctasks_created"),
        "asynctasks",
        ["created"],
        unique=False,
    )

    op.create_index(
        op.f("ix_asynctasks_task_type"),
        "asynctasks",
        ["task_type"],
        unique=False,
    )

    op.create_index(
        op.f("ix_asynctasks_status"),
        "asynctasks",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_asynctasks_created"), table_name="asynctasks")
    op.drop_index(op.f("ix_asynctasks_task_type"), table_name="asynctasks")
    op.drop_index(op.f("ix_asynctasks_status"), table_name="asynctasks")
    op.drop_table("asynctasks")
    sa.Enum(name="asynctasktype").drop(op.get_bind(), checkfirst=False)
    sa.Enum(name="asynctaskstatus").drop(op.get_bind(), checkfirst=False)
