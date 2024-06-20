import os
from typing import Any

from pydantic import AnyUrl, Extra
from pydantic.env_settings import BaseSettings, SettingsSourceCallable

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class CeleryBrokerUrl(AnyUrl):
    host_required: bool = False
    allowed_schemes = {"redis", "sqs"}


class CeleryConfiguration(ServiceConfiguration):
    # All the settings here are named following the Celery configuration, so we can
    # easily pass them into the Celery app. You can find more details about any of
    # these settings in the Celery documentation.
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html
    broker_url: CeleryBrokerUrl
    broker_connection_retry_on_startup: bool = True

    # Redis broker options
    broker_transport_options_global_keyprefix: str = "palace"
    broker_transport_options_queue_order_strategy: str = "priority"

    # SQS broker options
    broker_transport_options_region: str = "us-west-2"
    broker_transport_options_queue_name_prefix: str = "palace-"

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
        extra = Extra.allow

        # See `pydantic` documentation on customizing sources.
        # https://docs.pydantic.dev/1.10/usage/settings/#adding-sources
        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> tuple[SettingsSourceCallable, ...]:
            # We return an additional function that will parse the environment
            # variables and extract any that are not part of the settings model,
            # so that we can set additional configuration options for Celery at
            # deployment time if needed.
            return (
                init_settings,
                env_settings,
                file_secret_settings,
                additional_fields_from_env,
            )

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


def additional_fields_from_env(settings: BaseSettings) -> dict[str, Any]:
    """
    This function will extract any environment variables that start with
    the settings model's env_prefix but are not part of the settings model.

    This allows us to set additional configuration options via environment
    variables at deployment time.
    """
    additional_fields = {}
    env_prefix = settings.__config__.env_prefix or ""
    for key, value in os.environ.items():
        if key.startswith(env_prefix):
            field_name = key.replace(env_prefix, "").lower()
            if field_name not in settings.__fields__:
                additional_fields[field_name] = value
    return additional_fields
