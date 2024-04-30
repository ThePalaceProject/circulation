from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import boto3
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError
from celery.events.snapshot import Polaroid
from celery.events.state import State, Task

from palace.manager.util import chunks
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin

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


@dataclass
class QueueStats(LoggerMixin):
    """
    Tracks the number of tasks queued for a specific queue, so we can
    report this out to Cloudwatch metrics.
    """

    queued: set[str] = field(default_factory=set)

    def update(self, task: Task) -> None:
        self.log.debug("Task: %r", task)
        if task.uuid in self.queued:
            if task.started:
                self.log.debug("Task %s started.", task.uuid)
                self.queued.remove(task.uuid)
        else:
            if task.sent and not task.started:
                self.log.debug("Task %s queued.", task.uuid)
                self.queued.add(task.uuid)

    def metrics(
        self, timestamp: datetime, dimensions: dict[str, str]
    ) -> list[MetricDatumTypeDef]:
        return [
            value_metric("QueueWaiting", len(self.queued), timestamp, dimensions),
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
        self.logger = LoggerMixin.logger_for_cls(self.__class__)
        region = self.app.conf.get("cloudwatch_statistics_region")
        dryrun = self.app.conf.get("cloudwatch_statistics_dryrun")
        self.cloudwatch_client = (
            boto3.client("cloudwatch", region_name=region) if not dryrun else None
        )
        self.manager_name = self.app.conf.get("broker_transport_options", {}).get(
            "global_keyprefix"
        )
        self.namespace = self.app.conf.get("cloudwatch_statistics_namespace")
        self.upload_size = self.app.conf.get("cloudwatch_statistics_upload_size")
        self.queues = {
            str(queue.name): QueueStats() for queue in self.app.conf.get("task_queues")
        }

    def on_shutter(self, state: State) -> None:
        timestamp = utc_now()
        tasks = {
            task: TaskStats()
            for task in self.app.tasks.keys()
            if not task.startswith("celery.")
        }

        for task in state.tasks.values():
            try:
                tasks[task.name].update(task)
                self.queues[task.routing_key].update(task)
            except KeyError:
                self.logger.exception(
                    "Error processing task %s with routing key %s",
                    task.name,
                    task.routing_key,
                )

        self.publish(tasks, self.queues, timestamp)

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
                    self.logger.info("Data: %s", data)
