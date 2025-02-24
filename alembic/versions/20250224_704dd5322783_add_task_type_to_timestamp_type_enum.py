"""Add task type to timestamp type enum

Revision ID: 704dd5322783
Revises: c800cc42184a
Create Date: 2025-02-24 18:24:45.709550+00:00

"""

import sqlalchemy
from alembic import op

from palace.manager.sqlalchemy.model.coverage import Timestamp

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

old_type = sqlalchemy.Enum(*old_options, name="service_type")
new_type = sqlalchemy.Enum(*new_options, name="service_type")
tmp_type = sqlalchemy.Enum(*new_options, name="_service_type")

tcr = sqlalchemy.sql.table(
    "timestamps", sqlalchemy.Column("service_type", new_type, nullable=False)
)


def upgrade():
    # Create a tempoary "service_type" type, convert and drop the "old" type
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE timestamps ALTER COLUMN service_type TYPE _service_type"
        " USING service_type::text::_service_type"
    )
    old_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "new" service_type
    new_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE timestamps ALTER COLUMN service_type TYPE service_type"
        " USING service_type::text::service_type"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)


def downgrade():
    # Convert 'task' service_type into 'script'
    op.execute(
        tcr.update()
        .where(tcr.c.service_type == "task")
        .values(service_type=Timestamp.SCRIPT_TYPE)
    )
    # Create a temporary "_service_type" type, convert and drop the "new" type
    tmp_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE timestamps ALTER COLUMN service_type TYPE _service_type"
        " USING service_type::text::_service_type"
    )
    new_type.drop(op.get_bind(), checkfirst=False)
    # Create and convert to the "old" service_type type
    old_type.create(op.get_bind(), checkfirst=False)
    op.execute(
        "ALTER TABLE timestamps ALTER COLUMN service_type TYPE service_type"
        " USING service_type::text::service_type"
    )
    tmp_type.drop(op.get_bind(), checkfirst=False)
