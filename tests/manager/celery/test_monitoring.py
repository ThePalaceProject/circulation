import json
from unittest.mock import MagicMock, call, create_autospec, patch

import pytest
from boto3.exceptions import Boto3Error
from freezegun import freeze_time

from palace.util.log import LogLevel

from palace.manager.celery.celery import Celery
from palace.manager.celery.monitoring import Cloudwatch, QueueStats


class CloudwatchCameraFixture:
    def __init__(self, boto_client: MagicMock):
        self.app = create_autospec(Celery)
        self.configure_app()
        self.client = boto_client
        self._mock_get_redis: MagicMock | None = None

    def create_cloudwatch(self):
        with patch.object(Cloudwatch, "get_redis_client") as mock_get_redis:
            self._mock_get_redis = mock_get_redis
            return Cloudwatch(state=MagicMock(), app=self.app)

    @property
    def mock_get_redis(self):
        if self._mock_get_redis is None:
            raise ValueError(
                "get_redis_client not mocked because create_cloudwatch was not called."
            )
        return self._mock_get_redis

    def mock_queue(self, name: str) -> MagicMock:
        queue = MagicMock()
        queue.name = name
        return queue

    def configure_app(
        self,
        broker_url: str = "redis://testtesttest:1234/0",
        result_backend: str = "redis://testtesttest:1234/2",
        region: str = "region",
        dry_run: bool = False,
        manager_name: str = "manager",
        namespace: str = "namespace",
        upload_size: int = 100,
        queues: list[str] | None = None,
    ) -> None:
        queues = queues or ["queue1", "queue2"]
        self.app.conf = {
            "broker_url": broker_url,
            "result_backend": result_backend,
            "cloudwatch_statistics_region": region,
            "cloudwatch_statistics_dryrun": dry_run,
            "broker_transport_options": {"global_keyprefix": manager_name},
            "cloudwatch_statistics_namespace": namespace,
            "cloudwatch_statistics_upload_size": upload_size,
            "task_queues": [self.mock_queue(queue) for queue in queues],
        }


@pytest.fixture
def cloudwatch_camera():
    with patch("boto3.client") as boto_client:
        yield CloudwatchCameraFixture(boto_client)


class TestQueueStats:
    def test_metrics_without_age(self):
        stats = QueueStats(queued=2)
        timestamp = MagicMock()
        dimensions = {"key": "value", "key2": "value2"}
        expected_dimensions = [
            {"Name": key, "Value": value} for key, value in dimensions.items()
        ]
        [metric] = stats.metrics(timestamp, dimensions)
        assert metric["MetricName"] == "QueueWaiting"
        assert metric["Value"] == 2
        assert metric["Timestamp"] == timestamp.isoformat()
        assert metric["Dimensions"] == expected_dimensions
        assert metric["Unit"] == "Count"

    def test_metrics_with_age(self):
        stats = QueueStats(queued=2, oldest_age_seconds=42)
        timestamp = MagicMock()
        dimensions = {"key": "value"}
        expected_dimensions = [{"Name": "key", "Value": "value"}]

        [waiting, oldest] = stats.metrics(timestamp, dimensions)

        assert waiting["MetricName"] == "QueueWaiting"
        assert waiting["Value"] == 2
        assert waiting["Unit"] == "Count"

        assert oldest["MetricName"] == "QueueOldestAge"
        assert oldest["Value"] == 42
        assert oldest["Timestamp"] == timestamp.isoformat()
        assert oldest["Dimensions"] == expected_dimensions
        assert oldest["Unit"] == "Seconds"


class TestCloudwatch:
    def test__init__(self, cloudwatch_camera: CloudwatchCameraFixture):
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        assert cloudwatch.logger is not None
        assert cloudwatch.logger.name == "palace.manager.celery.monitoring.Cloudwatch"
        assert cloudwatch.cloudwatch_client == cloudwatch_camera.client.return_value
        cloudwatch_camera.client.assert_called_once_with(
            "cloudwatch", region_name="region"
        )
        assert cloudwatch.manager_name == "manager"
        assert cloudwatch.namespace == "namespace"
        assert cloudwatch.upload_size == 100
        assert cloudwatch.queues == {"queue1", "queue2"}
        assert cloudwatch.redis_client == cloudwatch_camera.mock_get_redis.return_value
        cloudwatch_camera.mock_get_redis.assert_called_once_with(
            "redis://testtesttest:1234/0",
            "manager",
        )

    def test__init__error(self, cloudwatch_camera: CloudwatchCameraFixture):
        cloudwatch_camera.configure_app(broker_url="sqs://")
        with pytest.raises(ValueError) as exc_info:
            cloudwatch_camera.create_cloudwatch()
        assert "Broker type 'sqs' is not supported." in str(exc_info.value)

    def test__init__dryrun(self, cloudwatch_camera: CloudwatchCameraFixture):
        cloudwatch_camera.configure_app(dry_run=True)
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        assert cloudwatch.cloudwatch_client is None

    def test_on_shutter(
        self,
        cloudwatch_camera: CloudwatchCameraFixture,
    ):
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        mock_publish = create_autospec(cloudwatch.publish)
        cloudwatch.publish = mock_publish
        cloudwatch_camera.mock_get_redis.return_value.llen.return_value = 10
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = None
        with freeze_time("2021-01-01"):
            cloudwatch.on_shutter(MagicMock())
        assert cloudwatch_camera.mock_get_redis.return_value.llen.call_count == 2
        cloudwatch_camera.mock_get_redis.return_value.llen.assert_has_calls(
            [call("queue1"), call("queue2")], any_order=True
        )
        mock_publish.assert_called_once()
        [queues, time] = mock_publish.call_args.args

        assert queues == {
            "queue1": QueueStats(queued=10, oldest_age_seconds=None),
            "queue2": QueueStats(queued=10, oldest_age_seconds=None),
        }
        assert time.isoformat() == "2021-01-01T00:00:00+00:00"

    def test__oldest_message_age_happy_path(
        self, cloudwatch_camera: CloudwatchCameraFixture
    ):
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        envelope = {"headers": {"enqueued_at": "2026-05-13T11:59:00+00:00"}}
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = json.dumps(
            envelope
        )
        with freeze_time("2026-05-13T12:00:30+00:00"):
            age = cloudwatch._oldest_message_age("queue1")
        cloudwatch_camera.mock_get_redis.return_value.lindex.assert_called_once_with(
            "queue1", -1
        )
        assert age == 90
        assert isinstance(age, int)

    def test__oldest_message_age_empty_queue(
        self, cloudwatch_camera: CloudwatchCameraFixture
    ):
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = None
        assert cloudwatch._oldest_message_age("queue1") is None

    def test__oldest_message_age_future_timestamp_clamped(
        self, cloudwatch_camera: CloudwatchCameraFixture
    ):
        # Future-dated enqueued_at (publisher clock ahead of camera) clamps to 0.
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        envelope = {"headers": {"enqueued_at": "2026-05-13T12:01:00+00:00"}}
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = json.dumps(
            envelope
        )
        with freeze_time("2026-05-13T12:00:00+00:00"):
            age = cloudwatch._oldest_message_age("queue1")
        assert age == 0

    def test__oldest_message_age_missing_header(
        self, cloudwatch_camera: CloudwatchCameraFixture
    ):
        # External publisher or pre-rollout message without our header — no signal, not an error.
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = json.dumps(
            {"headers": {}}
        )
        assert cloudwatch._oldest_message_age("queue1") is None

        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = json.dumps(
            {}
        )
        assert cloudwatch._oldest_message_age("queue1") is None

    def test__oldest_message_age_malformed(
        self,
        cloudwatch_camera: CloudwatchCameraFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        caplog.set_level(LogLevel.error)
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = (
            "not valid json"
        )
        assert cloudwatch._oldest_message_age("queue1") is None
        assert "Failed to parse oldest message in queue 'queue1'" in caplog.text
        # The raw redis value rides along under the palace_ prefix so it lands in
        # structured logs and we can debug whatever weird thing was on the queue.
        assert getattr(caplog.records[-1], "palace_raw_message") == "not valid json"

        caplog.clear()
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = json.dumps(
            {"headers": {"enqueued_at": "not-a-timestamp"}}
        )
        assert cloudwatch._oldest_message_age("queue1") is None
        assert "Failed to parse oldest message in queue 'queue1'" in caplog.text

        # `enqueued_at` is a non-string (e.g., an int) —
        # fromisoformat raises TypeError. Still logged, still returns None.
        caplog.clear()
        cloudwatch_camera.mock_get_redis.return_value.lindex.return_value = json.dumps(
            {"headers": {"enqueued_at": 1234567890}}
        )
        assert cloudwatch._oldest_message_age("queue1") is None
        assert "Failed to parse oldest message in queue 'queue1'" in caplog.text

    def test_publish(
        self,
        cloudwatch_camera: CloudwatchCameraFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        assert cloudwatch.cloudwatch_client is not None
        mock_put_metric_data = cloudwatch_camera.client.return_value.put_metric_data
        mock_put_metric_data.return_value = {
            "ResponseMetadata": {"HTTPStatusCode": 200}
        }
        timestamp = MagicMock()
        queues = {"queue1": QueueStats(queued=2)}

        cloudwatch.publish(queues, timestamp)
        mock_put_metric_data.assert_called_once()
        kwargs = mock_put_metric_data.call_args.kwargs
        assert kwargs["Namespace"] == "namespace"
        expected = list(
            queues["queue1"].metrics(
                timestamp, {"QueueName": "queue1", "Manager": "manager"}
            )
        )

        assert kwargs["MetricData"] == expected

        # If chunking is enabled, put_metric_data should be called multiple times. Once for each chunk.
        cloudwatch.upload_size = 1
        mock_put_metric_data.reset_mock()
        cloudwatch.publish(queues, timestamp)
        assert mock_put_metric_data.call_count == len(expected)

        # If there is an error, it should be logged.
        mock_put_metric_data.side_effect = Boto3Error("Boom")
        cloudwatch.publish(queues, timestamp)
        assert "Error sending metrics to Cloudwatch." in caplog.text

        # If dry run is enabled, no metrics should be sent and a log message should be generated.
        caplog.clear()
        caplog.set_level(LogLevel.info)
        cloudwatch.cloudwatch_client = None
        mock_put_metric_data.reset_mock()
        cloudwatch.publish(queues, timestamp)
        mock_put_metric_data.assert_not_called()
        assert "Dry run enabled. Not sending metrics to Cloudwatch." in caplog.text
