from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import TaskLock
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
)
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
                        f"remaining={len(waiting.len())}",
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
