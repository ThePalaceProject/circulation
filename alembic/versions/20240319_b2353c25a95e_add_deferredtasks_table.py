"""Add deferredtasks table

Revision ID: b2353c25a95e
Revises: 3e43ed59f256
Create Date: 2024-03-19 21:48:45.911507+00:00

"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "b2353c25a95e"
down_revision = "3e43ed59f256"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "deferredtasks",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("created", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "READY",
                "PROCESSING",
                "SUCCESS",
                "FAILURE",
                name="deferredtaskstatus",
            ),
            nullable=False,
        ),
        sa.Column(
            "task_type",
            sa.Enum(
                "INVENTORY_REPORT",
                name="deferredtasktype",
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
        op.f("ix_deferredtasks_created"),
        "deferredtasks",
        ["created"],
        unique=False,
    )

    op.create_index(
        op.f("ix_deferredtasks_task_type"),
        "deferredtasks",
        ["task_type"],
        unique=False,
    )

    op.create_index(
        op.f("ix_deferredtasks_status"),
        "deferredtasks",
        ["status"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_deferredtasks_created"), table_name="deferredtasks")
    op.drop_index(op.f("ix_deferredtasks_task_type"), table_name="deferredtasks")
    op.drop_index(op.f("ix_deferredtasks_status"), table_name="deferredtasks")
    op.drop_table("deferredtasks")
    sa.Enum(name="deferredtasktype").drop(op.get_bind(), checkfirst=False)
    sa.Enum(name="deferredtaskstatus").drop(op.get_bind(), checkfirst=False)
