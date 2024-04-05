from typing import Any

from celery import Celery
from kombu import Exchange, Queue


def task_queue_config(cm_name: str) -> dict[str, Any]:
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
    # Create a new Celery app
    cm_name = config.get("cm_name")
    assert isinstance(cm_name, str)
    app = Celery(cm_name, task_cls="core.celery.task:Task")
    app.conf.update(config)
    app.conf.update(task_queue_config(cm_name))
    app.set_default()

    return app
