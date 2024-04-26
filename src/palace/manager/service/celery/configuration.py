from typing import Any

from pydantic import RedisDsn

from palace.manager.service.configuration import ServiceConfiguration


class CeleryConfiguration(ServiceConfiguration):
    # All the settings here are named following the Celery configuration, so we can
    # easily pass them into the Celery app. You can find more details about any of
    # these settings in the Celery documentation.
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html
    broker_url: RedisDsn
    broker_connection_retry_on_startup: bool = True
    broker_transport_options_global_keyprefix: str = "palace"
    broker_transport_options_queue_order_strategy: str = "priority"

    task_acks_late = True
    task_reject_on_worker_lost = True
    task_remote_tracebacks = True
    task_create_missing_queues = False

    worker_cancel_long_running_tasks_on_connection_loss: bool = False
    worker_max_tasks_per_child: int = 100
    worker_prefetch_multiplier: int = 1
    worker_hijack_root_logger: bool = False
    worker_log_color: bool = False

    timezone: str = "US/Eastern"

    class Config:
        env_prefix = "PALACE_CELERY_"

    def dict(self, *, merge_options: bool = True, **kwargs: Any) -> dict[str, Any]:
        results = super().dict(**kwargs)
        if merge_options:
            result_keys = results.copy().keys()
            broker_transport_options = {}
            for key in result_keys:
                if key.startswith("broker_transport_options_"):
                    value = results.pop(key)
                    broker_transport_options[
                        key.replace("broker_transport_options_", "")
                    ] = value
            results["broker_transport_options"] = broker_transport_options
        return results
