from pydantic import RedisDsn

from core.service.configuration import ServiceConfiguration


class CeleryConfiguration(ServiceConfiguration):
    # All the settings here are named following the Celery configuration, so we can
    # easily pass them into the Celery app. You can find more details about any of
    # these settings in the Celery documentation.
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html
    broker_url: RedisDsn
    broker_connection_retry_on_startup: bool = True

    task_acks_late = True
    task_reject_on_worker_lost = True
    task_remote_tracebacks = True
    task_create_missing_queues = False

    worker_cancel_long_running_tasks_on_connection_loss: bool = False
    worker_max_tasks_per_child: int = 100
    worker_prefetch_multiplier: int = 1
    worker_hijack_root_logger: bool = False
    worker_log_color: bool = False

    cm_name: str = "palace"

    class Config:
        env_prefix = "PALACE_CELERY_"
