from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames


@shared_task(queue=QueueNames.default, bind=True)
def calculate_presentation(
    task: Task,
):
    pass
    # with task.session() as session:
    #     work = Work.by_id(task.session)
    #     work.calculate_presentation(policy)
