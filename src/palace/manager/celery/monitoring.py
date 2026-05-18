from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import boto3
from boto3.exceptions import Boto3Error
from botocore.exceptions import BotoCoreError
from celery.events.snapshot import Polaroid
from celery.events.state import State
from kombu.transport.redis import PrefixedStrictRedis
from redis import ConnectionPool

from palace.util.datetime_helpers import utc_now
from palace.util.exceptions import PalaceValueError
from palace.util.log import logger_for_cls

from palace.manager.util import chunks

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


@dataclass(frozen=True)
class QueueStats:
    """
    Tracks the number of tasks queued for a specific queue and the age of the
    oldest waiting task, so we can report these out to Cloudwatch metrics.
    """

    queued: int
    oldest_age_seconds: int | None = None

    def metrics(
        self, timestamp: datetime, dimensions: dict[str, str]
    ) -> list[MetricDatumTypeDef]:
        metric_data = [
            value_metric("QueueWaiting", self.queued, timestamp, dimensions),
        ]
        if self.oldest_age_seconds is not None:
            metric_data.append(
                value_metric(
                    "QueueOldestAge",
                    self.oldest_age_seconds,
                    timestamp,
                    dimensions,
                    unit="Seconds",
                )
            )
        return metric_data


class _PrefixedRedis(PrefixedStrictRedis):
    """A ``PrefixedStrictRedis`` that also prefixes ``LINDEX``.

    kombu's ``PrefixedStrictRedis`` only prefixes the Redis commands it needs
    for its own broker operations, and ``LINDEX`` is not one of them. The
    Cloudwatch camera uses ``LINDEX`` to read the oldest message in a queue, so
    without this it would query an unprefixed key and always get ``None`` back.
    """

    PREFIXED_SIMPLE_COMMANDS = [
        *PrefixedStrictRedis.PREFIXED_SIMPLE_COMMANDS,
        "LINDEX",
    ]


class Cloudwatch(Polaroid):
    """
    Implements a Celery custom camera that sends queue statistics to Cloudwatch.

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

    @classmethod
    def get_redis_client(
        cls, broker_url: str, global_keyprefix: str | None
    ) -> _PrefixedRedis:
        connection_pool = ConnectionPool.from_url(broker_url)
        return _PrefixedRedis(
            connection_pool=connection_pool, global_keyprefix=global_keyprefix
        )

    def _oldest_message_age(self, queue: str) -> int | None:
        # Kombu uses LPUSH on publish and BRPOP on consume, so the oldest unconsumed
        # message is at the tail of the list (index -1).
        raw = self.redis_client.lindex(queue, -1)
        if raw is None:
            return None
        try:
            msg = json.loads(raw)
            ts = msg.get("headers", {}).get("enqueued_at")
            if not ts:
                return None
            # Clamp at 0; the publisher's clock may be ahead of the camera host.
            return max(0, int((utc_now() - datetime.fromisoformat(ts)).total_seconds()))
        except (ValueError, AttributeError, TypeError):
            # ValueError: malformed JSON or non-ISO timestamp string.
            # AttributeError: json envelope isn't a dict (.get fails).
            # TypeError: enqueued_at is set to a non-string — fromisoformat raises.
            self.logger.exception(
                "Failed to parse oldest message in queue %r for age metric.",
                queue,
                extra={"palace_raw_message": raw},
            )
            return None

    def get_queue_stats(self) -> dict[str, QueueStats]:
        return {
            queue: QueueStats(
                queued=self.redis_client.llen(queue),
                oldest_age_seconds=self._oldest_message_age(queue),
            )
            for queue in self.queues
        }

    def on_shutter(self, state: State) -> None:
        timestamp = utc_now()
        self.publish(self.get_queue_stats(), timestamp)

    def publish(
        self,
        queues: dict[str, QueueStats],
        timestamp: datetime,
    ) -> None:
        metric_data = []
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
