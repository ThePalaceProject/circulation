from celery import shared_task

from palace.manager.celery.task import Task
from palace.manager.data_layer.policy.presentation import PresentationCalculationPolicy
from palace.manager.sqlalchemy.model.work import Work


@shared_task()
def calculate_presentation(
    task: Task, work_id: int, policy: PresentationCalculationPolicy
):

    with task.session() as session:
        work = Work.by_id(task.session)
        work.calculate_presentation(policy)
