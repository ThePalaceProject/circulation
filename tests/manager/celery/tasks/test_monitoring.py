from unittest.mock import patch

import pytest

from palace.manager.celery.tasks import monitoring
from palace.manager.celery.tasks.monitoring import publish_queue_stats
from tests.fixtures.celery import CeleryFixture


class TestPublishQueueStats:
    def test_publishes_via_reporter(
        self,
        celery_fixture: CeleryFixture,
        monkeypatch: pytest.MonkeyPatch,
    ):
        # The task builds a QueueStatsReporter from its app and runs it. We mock
        # the reporter because the test app uses a memory:// broker, which the
        # reporter (Redis-only) would reject.
        monkeypatch.setattr(monitoring, "_reporter", None)
        with patch.object(monitoring, "QueueStatsReporter") as mock_reporter_cls:
            publish_queue_stats.delay().wait()

        mock_reporter_cls.assert_called_once()
        mock_reporter_cls.return_value.run.assert_called_once_with()
