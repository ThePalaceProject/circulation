import sys
from enum import auto
from typing import Any

from celery.schedules import crontab
from kombu import Exchange, Queue

from palace.manager.celery.celery import Celery

# TODO: Remove this when we drop support for Python 3.10
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum


class QueueNames(StrEnum):
    high = auto()
    default = auto()


def beat_schedule() -> dict[str, Any]:
    """
    Configure the Celery beat schedule.

    This is a dictionary of tasks that should be run periodically. The key is the schedule name, and
    the value is a dictionary of options for the task. The `schedule` key is required and should
    be a timedelta object or a crontab string. The `task` key is required and should be the name
    of the task to run.
    """
    return {
        "update_custom_lists": {
            "task": "custom_list.update_custom_lists",
            "schedule": crontab(minute="5"),  # Run every hour at 5 minutes past
        }
    }


def task_queue_config() -> dict[str, Any]:
    """
    Configure the task queues for our Celery app.

    Currently we have two queues, `high` and `default`. The `high` queue is for tasks that are
    short running and should be processed quickly because they are time-sensitive. The `default`
    queue is for tasks that are longer running and can be processed when the worker has capacity.

    TODO: Evaluate if we need more granular queues for different types of tasks, as we roll this
      out to production and start to monitor worker utilization.
    """
    task_queues = []
    for queue in QueueNames:
        task_queues.append(Queue(queue, Exchange(queue), routing_key=queue))

    return {
        "task_queues": task_queues,
        "task_default_queue": QueueNames.default,
        "task_default_exchange": QueueNames.default,
        "task_default_routing_key": QueueNames.default,
    }


def celery_factory(config: dict[str, Any]) -> Celery:
    """
    Create and configure our Celery app, setting it as the default so that tasks registered
    with `shared_task` will use this app.
    """

    # Create a new Celery app
    app = Celery(task_cls="palace.manager.celery.task:Task")
    app.conf.update(config)
    app.conf.update(task_queue_config())
    app.conf.update({"beat_schedule": beat_schedule()})
    app.set_default()

    return app
