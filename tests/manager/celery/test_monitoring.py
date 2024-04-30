from functools import partial
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from boto3.exceptions import Boto3Error
from celery.events.state import State, Task
from freezegun import freeze_time

from palace.manager.celery.celery import Celery
from palace.manager.celery.monitoring import Cloudwatch, QueueStats, TaskStats
from palace.manager.service.logging.configuration import LogLevel


class TestTaskStats:
    def test_update(self):
        mock_task = create_autospec(Task)
        stats = TaskStats()

        mock_task.succeeded = True
        mock_task.failed = False
        mock_task.runtime = 1.0
        stats.update(mock_task)
        assert stats.failed == 0
        assert stats.succeeded == 1
        assert stats.runtime == [1.0]

        mock_task.succeeded = False
        mock_task.failed = True
        mock_task.runtime = None
        stats.update(mock_task)
        assert stats.failed == 1
        assert stats.succeeded == 1
        assert stats.runtime == [1.0]

        mock_task.succeeded = True
        mock_task.failed = False
        mock_task.runtime = 2.0
        stats.update(mock_task)
        assert stats.failed == 1
        assert stats.succeeded == 2
        assert stats.runtime == [1.0, 2.0]

    def test_update_with_none_runtime(self):
        mock_task = create_autospec(Task)
        mock_task.succeeded = True
        mock_task.failed = False
        mock_task.runtime = None

        stats = TaskStats()
        stats.update(mock_task)
        assert stats.failed == 0
        assert stats.succeeded == 1
        assert stats.runtime == []

    def test_metrics(self):
        stats = TaskStats(succeeded=2, failed=5, runtime=[3.5, 2.2])
        timestamp = MagicMock()
        dimensions = {"key": "value", "key2": "value2"}

        expected_dimensions = [
            {"Name": key, "Value": value} for key, value in dimensions.items()
        ]

        [succeeded_metric, failed_metric, runtime_metric] = stats.metrics(
            timestamp, dimensions
        )

        assert succeeded_metric["MetricName"] == "TaskSucceeded"
        assert succeeded_metric["Value"] == 2
        assert succeeded_metric["Timestamp"] == timestamp.isoformat()
        assert succeeded_metric["Dimensions"] == expected_dimensions
        assert succeeded_metric["Unit"] == "Count"

        assert failed_metric["MetricName"] == "TaskFailed"
        assert failed_metric["Value"] == 5
        assert failed_metric["Timestamp"] == timestamp.isoformat()
        assert failed_metric["Dimensions"] == expected_dimensions
        assert failed_metric["Unit"] == "Count"

        assert runtime_metric["MetricName"] == "TaskRuntime"
        assert runtime_metric["StatisticValues"] == {
            "Maximum": 3.5,
            "Minimum": 2.2,
            "SampleCount": 2,
            "Sum": 5.7,
        }
        assert runtime_metric["Timestamp"] == timestamp.isoformat()
        assert runtime_metric["Dimensions"] == expected_dimensions
        assert runtime_metric["Unit"] == "Seconds"

    def test_metrics_with_empty_runtime(self):
        stats = TaskStats(succeeded=2, failed=5, runtime=[])
        [succeeded_metric, failed_metric] = stats.metrics(MagicMock(), {})

        assert succeeded_metric["MetricName"] == "TaskSucceeded"
        assert failed_metric["MetricName"] == "TaskFailed"


class TestQueueStats:
    def test_update(self):
        stats = QueueStats()

        assert len(stats.queued) == 0

        mock_task = create_autospec(Task)
        mock_task.uuid = "uuid"
        mock_task.started = False
        mock_task.sent = False

        # Task is not started or sent, so it should not be in the queue.
        stats.update(mock_task)
        assert len(stats.queued) == 0

        # Task is both sent and started, so its being processed and should not be in the queue.
        mock_task.sent = True
        mock_task.started = True
        stats.update(mock_task)
        assert len(stats.queued) == 0

        # Task is sent but not started, so it should be in the queue.
        mock_task.sent = True
        mock_task.started = False
        stats.update(mock_task)
        assert len(stats.queued) == 1

        # If the task is sent again, it should still be in the queue, but not duplicated.
        stats.update(mock_task)
        assert len(stats.queued) == 1

        # If the task is started, it should be removed from the queue.
        mock_task.started = True
        stats.update(mock_task)
        assert len(stats.queued) == 0

    def test_metrics(self):
        stats = QueueStats(queued={"uuid1", "uuid2"})
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

        stats = QueueStats()
        [metric] = stats.metrics(timestamp, dimensions)
        assert metric["MetricName"] == "QueueWaiting"
        assert metric["Value"] == 0


class CloudwatchCameraFixture:
    def __init__(self, boto_client: MagicMock):
        self.app = create_autospec(Celery)
        self.configure_app()
        self.app.tasks = {
            "task1": MagicMock(),
            "task2": MagicMock(),
            "celery.built_in": MagicMock(),
        }
        self.client = boto_client
        self.state = create_autospec(State)
        self.state.tasks = {
            "task1": self.mock_task(),
            "task2": self.mock_task(),
        }
        self.create_cloudwatch = partial(Cloudwatch, state=self.state, app=self.app)

    def mock_queue(self, name: str) -> MagicMock:
        queue = MagicMock()
        queue.name = name
        return queue

    def mock_task(self) -> MagicMock:
        return MagicMock(spec=Task)

    def configure_app(
        self,
        region: str = "region",
        dry_run: bool = False,
        manager_name: str = "manager",
        namespace: str = "namespace",
        upload_size: int = 100,
        queues: list[str] | None = None,
    ) -> None:
        queues = queues or ["queue1", "queue2"]
        self.app.conf = {
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
        assert cloudwatch.queues == {"queue1": QueueStats(), "queue2": QueueStats()}

    def test__init__dryrun(self, cloudwatch_camera: CloudwatchCameraFixture):
        cloudwatch_camera.configure_app(dry_run=True)
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        assert cloudwatch.cloudwatch_client is None

    def test_on_shutter(self, cloudwatch_camera: CloudwatchCameraFixture):
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        mock_publish = create_autospec(cloudwatch.publish)
        cloudwatch.publish = mock_publish
        with freeze_time("2021-01-01"):
            cloudwatch.on_shutter(cloudwatch_camera.state)
        mock_publish.assert_called_once()
        [tasks, queues, time] = mock_publish.call_args.args

        assert tasks == {"task1": TaskStats(), "task2": TaskStats()}
        assert queues == {"queue1": QueueStats(), "queue2": QueueStats()}
        assert time.isoformat() == "2021-01-01T00:00:00+00:00"

    def test_on_shutter_error(
        self,
        cloudwatch_camera: CloudwatchCameraFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        cloudwatch_camera.app.tasks = {"task1": MagicMock()}
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        mock_publish = create_autospec(cloudwatch.publish)
        cloudwatch.publish = mock_publish
        cloudwatch.on_shutter(cloudwatch_camera.state)
        mock_publish.assert_called_once()
        [tasks, queues, time] = mock_publish.call_args.args

        assert tasks == {"task1": TaskStats()}
        assert queues == {"queue1": QueueStats(), "queue2": QueueStats()}
        assert time is not None
        assert "Error processing task" in caplog.text

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
        tasks = {"task1": TaskStats(succeeded=2, failed=5, runtime=[3.5, 2.2])}
        queues = {"queue1": QueueStats(queued={"uuid1", "uuid2"})}

        cloudwatch.publish(tasks, queues, timestamp)
        mock_put_metric_data.assert_called_once()
        kwargs = mock_put_metric_data.call_args.kwargs
        assert kwargs["Namespace"] == "namespace"
        expected = [
            *tasks["task1"].metrics(
                timestamp, {"TaskName": "task1", "Manager": "manager"}
            ),
            *queues["queue1"].metrics(
                timestamp, {"QueueName": "queue1", "Manager": "manager"}
            ),
        ]

        assert kwargs["MetricData"] == expected

        # If chunking is enabled, put_metric_data should be called multiple times. Once for each chunk.
        cloudwatch.upload_size = 1
        mock_put_metric_data.reset_mock()
        cloudwatch.publish(tasks, queues, timestamp)
        assert mock_put_metric_data.call_count == len(expected)

        # If there is an error, it should be logged.
        mock_put_metric_data.side_effect = Boto3Error("Boom")
        cloudwatch.publish(tasks, queues, timestamp)
        assert "Error sending metrics to Cloudwatch." in caplog.text

        # If dry run is enabled, no metrics should be sent and a log message should be generated.
        caplog.clear()
        caplog.set_level(LogLevel.info)
        cloudwatch.cloudwatch_client = None
        mock_put_metric_data.reset_mock()
        cloudwatch.publish(tasks, queues, timestamp)
        mock_put_metric_data.assert_not_called()
        assert "Dry run enabled. Not sending metrics to Cloudwatch." in caplog.text
