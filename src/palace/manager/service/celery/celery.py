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
        "full_search_reindex": {
            "task": "search.search_reindex",
            "schedule": crontab(hour="0", minute="10"),  # Run every day at 12:10 AM
        },
        "search_indexing": {
            "task": "search.search_indexing",
            "schedule": crontab(minute="*"),  # Run every minute
        },
        "marc_export": {
            "task": "marc.marc_export",
            "schedule": crontab(
                hour="3,11", minute="0"
            ),  # Run twice a day at 3:00 AM and 11:00 AM
        },
        "marc_export_cleanup": {
            "task": "marc.marc_export_cleanup",
            "schedule": crontab(
                hour="1",
                minute="0",
            ),  # Run every day at 1:00 AM
        },
        "opds2_odl_remove_expired_holds": {
            "task": "opds_odl.remove_expired_holds",
            "schedule": crontab(
                minute="16",
            ),  # Run every hour at 16 minutes past the hour
        },
        "opds2_odl_recalculate_hold_queue": {
            "task": "opds_odl.recalculate_hold_queue",
            "schedule": crontab(
                minute="31",
            ),  # Run every hour at 31 minutes past the hour
        },
        "rotate_jwe_key": {
            "task": "rotate_jwe_key.rotate_jwe_key",
            "schedule": crontab(
                minute="0",
                hour="3",
            ),  # Run every day at 3:00 AM
        },
        "loan_expiration_notifications": {
            "task": "notifications.loan_expiration",
            "schedule": crontab(
                minute="*/20",
            ),  # Run every 20 minutes
        },
        "hold_available_notifications": {
            "task": "notifications.hold_available",
            "schedule": crontab(
                minute="*/20",
            ),  # Run every 20 minutes
        },
        "axis_import_all_collections": {
            "task": "axis.import_all_collections",
            "schedule": crontab(
                minute="15,30,45,0",
            ),  # Run every 15 minutes
        },
        "axis_reap_all_collections": {
            "task": "axis.reap_all_collections",
            "schedule": crontab(
                minute="0",
                hour="4",
            ),  # Once a day at 4:00 AM
        },
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
