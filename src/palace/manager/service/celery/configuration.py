import importlib
from typing import Any

from kombu.utils.json import register_type
from pydantic import BaseModel
from pydantic_settings import SettingsConfigDict

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
    result_backend: str
    broker_connection_retry_on_startup: bool = True

    # Redis broker options
    broker_transport_options_global_keyprefix: str = "palace"
    broker_transport_options_queue_order_strategy: str = "priority"

    # SQS broker options
    broker_transport_options_region: str = "us-west-2"
    broker_transport_options_queue_name_prefix: str = "palace-"

    # Broker options for both Redis and SQS
    broker_transport_options_visibility_timeout: int = 3600  # 1 hour
    result_expires: int = 3600  # 1 hour  (default is 1 day)
    result_backend_transport_options_global_keyprefix: str = "palace-"
    task_acks_late: bool = True
    task_reject_on_worker_lost: bool = True
    task_remote_tracebacks: bool = True
    task_create_missing_queues: bool = False
    task_send_sent_event: bool = True
    task_track_started: bool = True
    task_time_limit: int | None = (
        1800  # 30 minutes, tasks must complete within this time
    )

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

    model_config = SettingsConfigDict(env_prefix="PALACE_CELERY_")

    def model_dump(
        self, *, merge_options: bool = True, **kwargs: Any
    ) -> dict[str, Any]:
        results = super().model_dump(**kwargs)
        if merge_options:
            result_keys = results.copy().keys()
            broker_transport_options: dict[str, Any] = {}
            result_backend_transport_options: dict[str, Any] = {}
            for key in result_keys:
                self.extract_keys_by_prefix(
                    broker_transport_options, results, "broker_transport_options_", key
                )
                self.extract_keys_by_prefix(
                    result_backend_transport_options,
                    results,
                    "result_backend_transport_options_",
                    key,
                )
            results["broker_transport_options"] = broker_transport_options
            results["result_backend_transport_options"] = (
                result_backend_transport_options
            )
        return results

    def extract_keys_by_prefix(
        self, new_dict: dict[str, Any], results: dict[str, Any], prefix: str, key: str
    ) -> None:
        if key.startswith(prefix):
            value = results.pop(key)
            new_dict[key.replace(prefix, "")] = value


# Allow Celery (via Kombu) to accept pydantic classes via the json serializer.
#
# This is a custom serializer for Pydantic models. It serializes the model to a
# dictionary containing the module and class name, and the model data. The
# deserializer takes the dictionary and reconstructs the model.
#
# This allows us to pass Pydantic models as arguments to Celery tasks, and have
# them automatically serialized and deserialized.
#
# See:
# - https://github.com/celery/kombu/blob/f78c440e9a9a48696ec04aef77f15f8d1a01e158/kombu/utils/json.py#L101-L115
# - https://docs.celeryq.dev/projects/kombu/en/stable/userguide/serialization.html#serializers
def _serialize_pydantic(obj: BaseModel) -> dict[str, Any]:
    return {
        "__module__": obj.__class__.__module__,
        "__qualname__": obj.__class__.__qualname__,
        "__model__": obj.model_dump(mode="json"),
    }


def _deserialize_pydantic(obj: dict[str, Any]) -> BaseModel:
    module_path = obj["__module__"]
    qualname = obj["__qualname__"]
    model_data = obj["__model__"]
    module = importlib.import_module(module_path)
    cls = getattr(module, qualname)
    return cls.model_validate(model_data)  # type: ignore[no-any-return]


register_type(
    BaseModel, "pydantic_base_model", _serialize_pydantic, _deserialize_pydantic
)
