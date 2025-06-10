from celery import shared_task
from sqlalchemy import delete, select

from palace.manager.celery.task import Task
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import TaskLock
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
)
from palace.manager.sqlalchemy.model.coverage import WorkCoverageRecord
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import elapsed_time_logging


@shared_task(queue=QueueNames.default, bind=True)
def calculate_work_presentations(
    task: Task,
    batch_size: int = 100,
) -> None:

    with TaskLock(task).lock():
        waiting = WaitingForPresentationCalculation(task.services.redis.client())
        work_policies = waiting.pop(batch_size)

        if work_policies:

            try:
                with (
                    task.session() as session,
                    elapsed_time_logging(
                        log_method=task.log.info,
                        message_prefix=f"Presentation calculated presentation for works: count={len(work_policies)}, "
                        f"remaining={waiting.len()}",
                        skip_start=True,
                    ),
                ):
                    for wp in work_policies:
                        work = get_one(session, Work, id=wp.work_id)
                        if not work:
                            task.log.warning(
                                f"No work with id={wp.work_id}. Skipping..."
                            )
                            continue
                        work.calculate_presentation(policy=wp.policy)
            except Exception as e:
                # if a failure occurs requeue the items so that can be recalculated in the next round
                waiting.add(*work_policies)
                raise e

    if len(work_policies) == batch_size:
        # This task is complete, but there are more works waiting to be recalculated. Requeue ourselves
        # to process the next batch.
        raise task.replace(calculate_work_presentations.s(batch_size=batch_size))

    task.log.info(f"Finished calculating presentation for works.")


@shared_task(queue=QueueNames.default, bind=True)
def migrate_work_coverage_records(task: Task) -> None:
    """
    TODO: Remove in next release
    This task is TEMPORARY and should be removed in the next release along with the reference to it in
    palace/manager/service/celery/celery.py
    Initially I had implemented this routine as an alembic migration but caused issues with the tests
    and introduced unnecessary complexity into the migration process.
    """
    with task.transaction() as session:
        rows = session.execute(
            select(WorkCoverageRecord.work_id).where(
                WorkCoverageRecord.status == WorkCoverageRecord.REGISTERED
            )
        ).all()

        policy = PresentationCalculationPolicy.recalculate_everything()

        for row in rows:
            Work.queue_presentation_recalculation(work_id=row["work_id"], policy=policy)

        session.execute(
            delete(WorkCoverageRecord).where(
                WorkCoverageRecord.status == WorkCoverageRecord.REGISTERED
            )
        )
