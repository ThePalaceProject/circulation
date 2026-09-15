from celery import shared_task

from palace.manager.celery.monitoring import QueueStatsReporter
from palace.manager.celery.task import Task
from palace.manager.service.celery.celery import QueueNames

# The reporter opens a Redis connection pool and a boto3 Cloudwatch client, so we
# build it once per worker process and reuse it across invocations (the task runs
# every minute). This mirrors how Task caches its session maker.
_reporter: QueueStatsReporter | None = None


def _get_reporter(task: Task) -> QueueStatsReporter:
    global _reporter
    if _reporter is None:
        _reporter = QueueStatsReporter(task.app)
    return _reporter


@shared_task(queue=QueueNames.high, bind=True)
def publish_queue_stats(task: Task) -> None:
    """Publish per-queue depth and oldest-message age to Cloudwatch.

    Scheduled by beat every minute onto the ``high`` queue. ``high`` is the
    always-staffed, time-sensitive queue, so the metric keeps flowing even while
    the ``default`` / ``apply`` pools are saturated and being scaled -- which are
    exactly the situations the autoscaler needs the metric for.

    This replaces the always-on ``celery events`` Cloudwatch camera: the metric is
    just a periodic Redis read plus a ``put_metric_data`` call, so it does not need
    a dedicated long-running process.
    """
    _get_reporter(task).run()
