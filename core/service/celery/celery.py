from typing import Any

from celery import Celery
from kombu import Exchange, Queue


def task_queue_config(cm_name: str) -> dict[str, Any]:
    """
    Configure the task queues for our Celery app.

    The task queues are prefixed with the name passed in as `cm_name`. This is to ensure that
    the task queue names are unique when we have multiple Celery apps running using the same
    Redis instance as the broker.

    In order to select the correct queue, you can use the `key` parameter, which routes the task
    to the correct queue based on the routing key, which isn't prefixed.

    Currently we have two queues, `high` and `default`. The `high` queue is for tasks that are
    short running and should be processed quickly because they are time-sensitive. The `default`
    queue is for tasks that are longer running and can be processed when the worker has capacity.

    TODO: Evaluate if we need more granular queues for different types of tasks, as we roll this
      out to production and start to monitor worker utilization.
    """

    high_prefixed = f"{cm_name}:high"
    default_prefixed = f"{cm_name}:default"

    return {
        "task_queues": (
            Queue(high_prefixed, Exchange(high_prefixed), routing_key="high"),
            Queue(default_prefixed, Exchange(default_prefixed), routing_key="default"),
        ),
        "task_default_queue": default_prefixed,
        "task_default_exchange": default_prefixed,
        "task_default_routing_key": "default",
    }


def celery_factory(config: dict[str, Any]) -> Celery:
    """
    Create and configure our Celery app, setting it as the default so that tasks registered
    with `shared_task` will use this app.
    """

    # Create a new Celery app
    cm_name = config.get("cm_name")
    assert isinstance(cm_name, str)
    app = Celery(cm_name, task_cls="core.celery.task:Task")
    app.conf.update(config)
    app.conf.update(task_queue_config(cm_name))
    app.set_default()

    return app
