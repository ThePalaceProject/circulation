"""Migrate work coverage records to work id policy redis set

Revision ID: 7b8cb141ec16
Revises: d671b95566fb
Create Date: 2025-06-06 16:16:37.611313+00:00

"""

from alembic import op

from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.container import container_instance
from palace.manager.sqlalchemy.model.work import Work

# revision identifiers, used by Alembic.
revision = "7b8cb141ec16"
down_revision = "d671b95566fb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(
        "select work_id from workcoveragerecords where status = 'registered'"
    ).all()
    policy = PresentationCalculationPolicy.recalculate_everything()

    services = container_instance()
    services.wire()
    # sanity test the queue: if there are no work ids we want make sure
    # that we can successfully queue works.  Since the work queue does not require
    # that the work_id is valid (if not it will be ignored by downstream processes),
    # it is safe to do this.
    Work.queue_presentation_recalculation(work_id=1, policy=policy)

    for row in rows:
        Work.queue_presentation_recalculation(work_id=row["work_id"], policy=policy)

    op.execute("delete from workcoveragerecords where status = 'registered'")


def downgrade() -> None:
    pass
