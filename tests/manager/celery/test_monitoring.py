from unittest.mock import MagicMock, call, create_autospec, patch
from uuid import uuid4

import pytest
from boto3.exceptions import Boto3Error
from celery.events.state import State, Task
from freezegun import freeze_time

from palace.manager.celery.celery import Celery
from palace.manager.celery.monitoring import Cloudwatch, QueueStats, TaskStats
from palace.manager.service.logging.configuration import LogLevel


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
        self.state.tasks = self.task_list(
            [
                self.mock_task("task1", runtime=1.0),
                self.mock_task("task1", runtime=2.0),
                self.mock_task("task2", succeeded=False, failed=True),
                self.mock_task("task2", started=False, succeeded=False, uuid="uuid4"),
                self.mock_task("celery.built_in", started=False, succeeded=False),
            ]
        )
        self._mock_get_redis: MagicMock | None = None

    def create_cloudwatch(self):
        with patch.object(Cloudwatch, "get_redis_client") as mock_get_redis:
            self._mock_get_redis = mock_get_redis
            return Cloudwatch(state=self.state, app=self.app)

    @property
    def mock_get_redis(self):
        if self._mock_get_redis is None:
            raise ValueError(
                "get_redis_client not mocked because create_cloudwatch was not called."
            )
        return self._mock_get_redis

    @staticmethod
    def task_list(tasks: list[Task]) -> dict[str, Task]:
        return {task.uuid: task for task in tasks}

    def mock_queue(self, name: str) -> MagicMock:
        queue = MagicMock()
        queue.name = name
        return queue

    def mock_task(
        self,
        name: str | None = None,
        *,
        sent: bool = True,
        started: bool = True,
        succeeded: bool = True,
        failed: bool = False,
        runtime: float | None = None,
        uuid: str | None = None,
    ) -> Task:
        if uuid is None:
            uuid = str(uuid4())
        return Task(
            uuid=uuid,
            name=name,
            sent=sent,
            started=started,
            succeeded=succeeded,
            failed=failed,
            runtime=runtime,
        )

    def configure_app(
        self,
        broker_url: str = "redis://testtesttest:1234/0",
        result_backend: str = "redis://testtesttest:1234/0",
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


class TestTaskStats:
    def test_update(self, cloudwatch_camera: CloudwatchCameraFixture):
        stats = TaskStats()

        mock_task = cloudwatch_camera.mock_task(runtime=1.0)
        stats.update(mock_task)
        assert stats.failed == 0
        assert stats.succeeded == 1
        assert stats.runtime == [1.0]

        mock_task = cloudwatch_camera.mock_task(succeeded=False, failed=True)
        stats.update(mock_task)
        assert stats.failed == 1
        assert stats.succeeded == 1
        assert stats.runtime == [1.0]

        mock_task = cloudwatch_camera.mock_task(runtime=2.0)
        stats.update(mock_task)
        assert stats.failed == 1
        assert stats.succeeded == 2
        assert stats.runtime == [1.0, 2.0]

    def test_update_with_none_runtime(self, cloudwatch_camera: CloudwatchCameraFixture):
        stats = TaskStats()
        mock_task = cloudwatch_camera.mock_task()
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
    def test_metrics(self):
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
        with freeze_time("2021-01-01"):
            cloudwatch.on_shutter(cloudwatch_camera.state)
        assert cloudwatch_camera.mock_get_redis.return_value.llen.call_count == 2
        cloudwatch_camera.mock_get_redis.return_value.llen.assert_has_calls(
            [call("queue1"), call("queue2")], any_order=True
        )
        mock_publish.assert_called_once()
        [tasks, queues, time] = mock_publish.call_args.args

        assert tasks == {
            "task1": TaskStats(succeeded=2, runtime=[1.0, 2.0]),
            "task2": TaskStats(failed=1),
        }
        assert queues == {
            "queue1": QueueStats(queued=10),
            "queue2": QueueStats(queued=10),
        }
        assert time.isoformat() == "2021-01-01T00:00:00+00:00"

    def test_on_shutter_unknown_task_name(
        self,
        cloudwatch_camera: CloudwatchCameraFixture,
    ):
        # We can also handle the case where we see a task with an unknown name.
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        mock_publish = create_autospec(cloudwatch.publish)
        cloudwatch.publish = mock_publish
        cloudwatch_camera.state.tasks = cloudwatch_camera.task_list(
            [
                cloudwatch_camera.mock_task(
                    "unknown_task",
                    failed=True,
                    succeeded=False,
                    uuid="uuid6",
                ),
            ]
        )
        cloudwatch.on_shutter(cloudwatch_camera.state)
        [tasks, _, _] = mock_publish.call_args.args
        assert tasks == {
            "task1": TaskStats(),
            "task2": TaskStats(),
            "unknown_task": TaskStats(failed=1),
        }

    def test_on_shutter_no_task_name(
        self,
        cloudwatch_camera: CloudwatchCameraFixture,
        caplog: pytest.LogCaptureFixture,
    ):
        # We can handle tasks with no name
        caplog.set_level(LogLevel.warning)
        cloudwatch = cloudwatch_camera.create_cloudwatch()
        mock_publish = create_autospec(cloudwatch.publish)
        cloudwatch.publish = mock_publish
        cloudwatch_camera.state.tasks = cloudwatch_camera.task_list(
            [
                cloudwatch_camera.mock_task(None, started=True, uuid="uuid7"),
                cloudwatch_camera.mock_task(None, started=True, uuid="uuid5"),
            ]
        )
        cloudwatch.on_shutter(cloudwatch_camera.state)
        [tasks, _, _] = mock_publish.call_args.args
        assert tasks == {
            "task1": TaskStats(),
            "task2": TaskStats(),
        }

        # We log the information about tasks with no name
        (
            no_name_warning_1,
            no_name_warning_2,
        ) = caplog.messages
        assert (
            "Task has no name. [sent]:True, [started]:True, [uuid]:uuid7."
            in no_name_warning_1
        )
        assert (
            "Task has no name. [sent]:True, [started]:True, [uuid]:uuid5."
            in no_name_warning_2
        )

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
        queues = {"queue1": QueueStats(queued=2)}

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
