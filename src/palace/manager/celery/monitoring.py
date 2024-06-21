from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import boto3
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError
from celery.events.snapshot import Polaroid
from celery.events.state import State, Task
from kombu.transport.redis import PrefixedStrictRedis
from redis import ConnectionPool

from palace.manager.core.exceptions import PalaceValueError
from palace.manager.util import chunks
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import logger_for_cls

if TYPE_CHECKING:
    from mypy_boto3_cloudwatch.literals import StandardUnitType
    from mypy_boto3_cloudwatch.type_defs import DimensionTypeDef, MetricDatumTypeDef


def metric_dimensions(dimensions: dict[str, str]) -> Sequence[DimensionTypeDef]:
    return [{"Name": key, "Value": value} for key, value in dimensions.items()]


def value_metric(
    metric_name: str,
    value: int,
    timestamp: datetime,
    dimensions: dict[str, str],
    unit: StandardUnitType = "Count",
) -> MetricDatumTypeDef:
    """
    Format a metric for a single value into the format expected by Cloudwatch.

    See Boto3 documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch/client/put_metric_data.html
    """
    return {
        "MetricName": metric_name,
        "Value": value,
        "Timestamp": timestamp.isoformat(),
        "Dimensions": metric_dimensions(dimensions),
        "Unit": unit,
    }


def statistic_metric(
    metric_name: str,
    values: Sequence[float],
    timestamp: datetime,
    dimensions: dict[str, str],
    unit: StandardUnitType = "Seconds",
) -> MetricDatumTypeDef:
    """
    Format a metric for multiple values into the format expected by Cloudwatch. This
    includes the statistic values for the maximum, minimum, sum, and sample count.

    See Boto3 documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch/client/put_metric_data.html
    """
    return {
        "MetricName": metric_name,
        "StatisticValues": {
            "Maximum": max(values),
            "Minimum": min(values),
            "SampleCount": len(values),
            "Sum": sum(values),
        },
        "Timestamp": timestamp.isoformat(),
        "Dimensions": metric_dimensions(dimensions),
        "Unit": unit,
    }


@dataclass
class TaskStats:
    """
    Tracks the number of tasks that have succeeded, failed, and how long they took to run
    for a specific task, so we can report this out to Cloudwatch metrics.
    """

    succeeded: int = 0
    failed: int = 0
    runtime: list[float] = field(default_factory=list)

    def update(self, task: Task) -> None:
        if task.succeeded:
            self.succeeded += 1
            if task.runtime:
                self.runtime.append(task.runtime)
        if task.failed:
            self.failed += 1

    def metrics(
        self, timestamp: datetime, dimensions: dict[str, str]
    ) -> list[MetricDatumTypeDef]:
        metric_data = [
            value_metric("TaskSucceeded", self.succeeded, timestamp, dimensions),
            value_metric("TaskFailed", self.failed, timestamp, dimensions),
        ]

        if self.runtime:
            metric_data.append(
                statistic_metric("TaskRuntime", self.runtime, timestamp, dimensions)
            )

        return metric_data

    def reset(self) -> None:
        self.succeeded = 0
        self.failed = 0
        self.runtime = []


@dataclass(frozen=True)
class QueueStats:
    """
    Tracks the number of tasks queued for a specific queue, so we can
    report this out to Cloudwatch metrics.
    """

    queued: int

    def metrics(
        self, timestamp: datetime, dimensions: dict[str, str]
    ) -> list[MetricDatumTypeDef]:
        return [
            value_metric("QueueWaiting", self.queued, timestamp, dimensions),
        ]


class Cloudwatch(Polaroid):
    """
    Implements a Celery custom camera that sends task and queue statistics to Cloudwatch.

    See Celery documentation for more information on custom cameras:
    https://docs.celeryq.dev/en/stable/userguide/monitoring.html#custom-camera
    """

    clear_after = True  # clear after flush (incl, state.event_count).

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        # We use logger_for_cls instead of just inheriting from LoggerMixin
        # because the base class Polaroid already defines a logger attribute,
        # which conflicts with the logger() method in LoggerMixin.
        self.logger = logger_for_cls(self.__class__)
        broker_url = self.app.conf.get("broker_url")
        broker_type = urlparse(broker_url).scheme if broker_url else None
        if broker_type != "redis":
            raise PalaceValueError(f"Broker type '{broker_type}' is not supported.")

        region = self.app.conf.get("cloudwatch_statistics_region")
        dryrun = self.app.conf.get("cloudwatch_statistics_dryrun")
        self.cloudwatch_client = (
            boto3.client("cloudwatch", region_name=region) if not dryrun else None
        )
        self.manager_name = self.app.conf.get("broker_transport_options", {}).get(
            "global_keyprefix"
        )
        self.redis_client = self.get_redis_client(broker_url, self.manager_name)
        self.namespace = self.app.conf.get("cloudwatch_statistics_namespace")
        self.upload_size = self.app.conf.get("cloudwatch_statistics_upload_size")
        self.queues = {queue.name for queue in self.app.conf.get("task_queues")}
        self.tasks: defaultdict[str, TaskStats] = defaultdict(
            TaskStats,
            {
                task: TaskStats()
                for task in self.app.tasks.keys()
                if not self.is_celery_task(task)
            },
        )

    @classmethod
    def get_redis_client(
        cls, broker_url: str, global_keyprefix: str | None
    ) -> PrefixedStrictRedis:
        connection_pool = ConnectionPool.from_url(broker_url)
        return PrefixedStrictRedis(
            connection_pool=connection_pool, global_keyprefix=global_keyprefix
        )

    @staticmethod
    def is_celery_task(task_name: str) -> bool:
        return task_name.startswith("celery.")

    @staticmethod
    def task_info_str(task: Task) -> str:
        return ", ".join(
            [
                f"[{k}]:{v}"
                for k, v in task.info(extra=["name", "sent", "started", "uuid"]).items()
            ]
        )

    def reset_tasks(self) -> None:
        for task in self.tasks.values():
            task.reset()

    def update_task_stats(self, state: State) -> None:
        # Reset the task stats for each snapshot
        self.reset_tasks()

        # Update task stats for each task in the state
        for task in state.tasks.values():
            # Update task stats for each task
            if task.name is None:
                self.logger.warning(f"Task has no name. {self.task_info_str(task)}.")
            elif self.is_celery_task(task.name):
                # If this is an internal Celery task, we skip it entirely.
                # We don't want to track internal Celery tasks.
                continue
            else:
                self.tasks[task.name].update(task)

    def get_queue_stats(self) -> dict[str, QueueStats]:
        return {
            queue: QueueStats(self.redis_client.llen(queue)) for queue in self.queues
        }

    def on_shutter(self, state: State) -> None:
        timestamp = utc_now()
        self.update_task_stats(state)
        queue_stats = self.get_queue_stats()

        self.publish(self.tasks, queue_stats, timestamp)

    def publish(
        self,
        tasks: dict[str, TaskStats],
        queues: dict[str, QueueStats],
        timestamp: datetime,
    ) -> None:
        metric_data = []
        for task_name, task_stats in tasks.items():
            metric_data.extend(
                task_stats.metrics(
                    timestamp, {"TaskName": task_name, "Manager": self.manager_name}
                )
            )

        for queue_name, queue_stats in queues.items():
            metric_data.extend(
                queue_stats.metrics(
                    timestamp, {"QueueName": queue_name, "Manager": self.manager_name}
                )
            )

        for chunk in chunks(metric_data, self.upload_size):
            self.logger.info("Sending %d metrics to Cloudwatch.", len(chunk))
            if self.cloudwatch_client is not None:
                try:
                    self.cloudwatch_client.put_metric_data(
                        Namespace=self.namespace,
                        MetricData=chunk,
                    )
                except (Boto3Error, BotoCoreError):
                    self.logger.exception("Error sending metrics to Cloudwatch.")
            else:
                self.logger.info("Dry run enabled. Not sending metrics to Cloudwatch.")
                for data in chunk:
                    self.logger.info(f"Data: {data}")
