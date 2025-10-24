"""Add task type to timestamp type enum

Revision ID: 704dd5322783
Revises: c800cc42184a
Create Date: 2025-02-24 18:24:45.709550+00:00

"""

from alembic import op

from palace.manager.sqlalchemy.model.coverage import Timestamp
from palace.manager.util.migration.helpers import pg_update_enum

# revision identifiers, used by Alembic.
revision = "704dd5322783"
down_revision = "c800cc42184a"
branch_labels = None
depends_on = None

old_options = (
    Timestamp.COVERAGE_PROVIDER_TYPE,
    Timestamp.MONITOR_TYPE,
    Timestamp.SCRIPT_TYPE,
)
new_options = old_options + (Timestamp.TASK_TYPE,)


def upgrade() -> None:
    # Add task type to the enum
    pg_update_enum(
        op, "timestamps", "service_type", "service_type", old_options, new_options
    )


def downgrade() -> None:
    # Convert 'task' service_type into 'script'
    op.execute(
        f"UPDATE timestamps SET service_type='{Timestamp.SCRIPT_TYPE}' "
        f"where service_type='{Timestamp.TASK_TYPE}'"
    )

    # Update the enum
    pg_update_enum(
        op, "timestamps", "service_type", "service_type", new_options, old_options
    )
