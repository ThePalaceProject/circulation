from __future__ import annotations

from celery import shared_task
from typing_extensions import Unpack

from palace.manager.celery.task import Task
from palace.manager.reporting.reports.library_collection import (
    LibraryCollectionReport,
    LibraryReportKwargs,
    LibraryTitleLevelReport,
)
from palace.manager.service.celery.celery import QueueNames

REPORT_KEY_MAPPING: dict[str, type[LibraryCollectionReport]] = {
    report.KEY: report
    for report in [
        LibraryTitleLevelReport,
    ]
}


@shared_task(queue=QueueNames.high, bind=True)
def generate_report(task: Task, *, key: str, **kwargs: Unpack[LibraryReportKwargs]):
    report_class = REPORT_KEY_MAPPING[key]
    report = report_class.from_task(task, **kwargs)

    with task.session() as session:
        success = report.run(session=session)
        if not success:
            task.log.error(
                f"Report task failed: '{report.title}' ({report.key}) for <{report.email_address}>. "
                f"(request ID: {report.request_id})"
            )
        return success
