from typing import Any

from pydantic import RedisDsn

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class CeleryConfiguration(ServiceConfiguration):
    # All the settings here are named following the Celery configuration, so we can
    # easily pass them into the Celery app. You can find more details about any of
    # these settings in the Celery documentation.
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html
    broker_url: RedisDsn
    broker_connection_retry_on_startup: bool = True
    broker_transport_options_global_keyprefix: str = "palace"
    broker_transport_options_queue_order_strategy: str = "priority"

    task_acks_late: bool = True
    task_reject_on_worker_lost: bool = True
    task_remote_tracebacks: bool = True
    task_create_missing_queues: bool = False
    task_send_sent_event: bool = True
    task_track_started: bool = True

    worker_cancel_long_running_tasks_on_connection_loss: bool = False
    worker_max_tasks_per_child: int = 100
    worker_prefetch_multiplier: int = 1
    worker_hijack_root_logger: bool = False
    worker_log_color: bool = False
    worker_send_task_events: bool = True

    timezone: str = "US/Eastern"

    # These settings are specific to the custom event reporting we are doing
    # to send Celery task and queue statistics to Cloudwatch. You can see
    # how they are used in `palace.manager.celery.monitoring.Cloudwatch`.
    cloudwatch_statistics_dryrun: bool = False
    cloudwatch_statistics_namespace: str = "Celery"
    cloudwatch_statistics_region: str = "us-west-2"
    cloudwatch_statistics_upload_size: int = 500

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
