import os
from typing import Any

from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from palace.manager.service.configuration.service_configuration import (
    ServiceConfiguration,
)


class CeleryConfiguration(ServiceConfiguration):
    # All the settings here are named following the Celery configuration, so we can
    # easily pass them into the Celery app. You can find more details about any of
    # these settings in the Celery documentation.
    # https://docs.celeryq.dev/en/stable/userguide/configuration.html

    # It would be nice to validate the broker_url via a Pydantic URL type, but for
    # sqs:// urls, the host isn't required, but you can still supply a username and
    # password. This isn't supported by Pydantic URL type. There is an open bug for
    # this issue: https://github.com/pydantic/pydantic/issues/7267. If / when that
    # is resolved we can switch to using the Pydantic URL type.
    broker_url: str
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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # We return an additional function that will parse the environment
        # variables and extract any that are not part of the settings model,
        # so that we can set additional configuration options for Celery at
        # deployment time if needed.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            file_secret_settings,
            AdditionalFieldsFromEnv(settings_cls),
        )

    model_config = SettingsConfigDict(env_prefix="PALACE_CELERY_", extra="allow")

    def model_dump(
        self, *, merge_options: bool = True, **kwargs: Any
    ) -> dict[str, Any]:
        results = super().model_dump(**kwargs)
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


class AdditionalFieldsFromEnv(PydanticBaseSettingsSource):
    def get_field_value(
        self, field: FieldInfo | None, field_name: str
    ) -> tuple[Any, str, bool]:
        env_prefix = self.config.get("env_prefix") or ""
        name = field_name.replace(env_prefix, "").lower()
        value = os.environ.get(field_name)
        return value, name, False

    def __call__(self) -> dict[str, Any]:
        additional_fields = {}
        env_prefix = self.config.get("env_prefix") or ""
        for key in os.environ.keys():
            if key.startswith(env_prefix):
                value, field_name, _ = self.get_field_value(None, key)
                if field_name not in self.current_state:
                    additional_fields[field_name] = value
        return additional_fields
