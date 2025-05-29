from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames
from palace.manager.service.redis.models.lock import TaskLock
from palace.manager.service.redis.models.work import (
    WaitingForPresentationCalculation,
    WorkIdAndPolicy,
)
from palace.manager.sqlalchemy.model.work import Work
from palace.manager.util.backoff import exponential_backoff
from palace.manager.util.log import elapsed_time_logging


@shared_task(queue=QueueNames.default, bind=True)
def calculate_work_presentations(
    task: Task,
    batch_size: int = 100,
) -> None:

    redis_client = task.services.redis.client()
    with TaskLock(task).lock():
        waiting = WaitingForPresentationCalculation(redis_client)
        work_policies = waiting.pop(batch_size)

        if len(work_policies) > 0:

            calculate_presentation_for_works.delay(list(work_policies))

    if len(work_policies) == batch_size:
        # This task is complete, but there are more works waiting to be indexed. Requeue ourselves
        # to process the next batch.
        raise task.replace(calculate_work_presentations.s(batch_size=batch_size))

    task.log.info(f"Finished queuing indexing tasks.")
    return


class OperationalErrorn:
    pass


@shared_task(queue=QueueNames.default, bind=True, max_retries=4)
def calculate_presentation_for_works(
    task: Task,
    work_policies: list[WorkIdAndPolicy],
    disable_exponential_back_off: bool = False,
) -> None:
    with (
        task.session() as session,
        elapsed_time_logging(
            log_method=task.log.info,
            message_prefix="Presentation calculated for works",
            skip_start=True,
        ),
    ):
        try:
            for wp in work_policies:
                work = Work.by_id(session, id=wp.work_id)
                if not work:
                    task.log.warning(f"No work with id={wp.work_id}. Skipping...")
                    continue
                work.calculate_presentation(policy=wp.policy)
        except Exception as e:
            wait_time = (
                1
                if disable_exponential_back_off
                else exponential_backoff(task.request.retries)
            )

            task.log.exception(
                f"Something unexpected went wrong while calculating the presentation for one of the works in "
                f"task(id={task.request.id} due to {e}. Retrying in {wait_time} seconds."
            )
            raise task.retry(countdown=wait_time)
