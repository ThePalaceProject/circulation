from unittest.mock import MagicMock, create_autospec

from celery.events.state import Task

from palace.manager.celery.monitoring import QueueStats, TaskStats


class TestTaskStats:
    def test_update(self):
        mock_task_1 = create_autospec(Task)
        mock_task_1.succeeded = True
        mock_task_1.failed = False
        mock_task_1.runtime = 1.0

        mock_task_2 = create_autospec(Task)
        mock_task_2.succeeded = False
        mock_task_2.failed = True
        mock_task_2.runtime = None

        mock_task_3 = create_autospec(Task)
        mock_task_3.succeeded = True
        mock_task_3.failed = False
        mock_task_3.runtime = 2.0

        stats = TaskStats()
        stats.update(mock_task_1)
        assert stats.failed == 0
        assert stats.succeeded == 1
        assert stats.runtime == [1.0]

        stats.update(mock_task_2)
        assert stats.failed == 1
        assert stats.succeeded == 1
        assert stats.runtime == [1.0]

        stats.update(mock_task_3)
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
