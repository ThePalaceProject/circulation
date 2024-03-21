import json
from dataclasses import asdict
from http import HTTPStatus

import flask
from flask import Response
from sqlalchemy.orm import Session

from core.model import Library
from core.model.admin import Admin
from core.model.deferredtask import (
    DeferredTaskType,
    InventoryReportTaskData,
    queue_task,
)
from core.problem_details import INTERNAL_SERVER_ERROR
from core.util.log import LoggerMixin
from core.util.problem_detail import ProblemDetail


class ReportController(LoggerMixin):
    def __init__(self, db: Session):
        self._db = db

    def generate_inventory_report(self) -> Response | ProblemDetail:
        library: Library = getattr(flask.request, "library")
        admin: Admin = getattr(flask.request, "admin")
        try:
            # these values should never be None
            assert admin.email
            assert admin.id
            assert library.id

            data: InventoryReportTaskData = InventoryReportTaskData(
                admin_email=admin.email, admin_id=admin.id, library_id=library.id
            )
            task, is_new = queue_task(
                self._db, task_type=DeferredTaskType.INVENTORY_REPORT, data=asdict(data)
            )

            msg = (
                f"An inventory report request was {'already' if not is_new else ''} received at {task.created}. "
                f"When processing is complete, the report will be sent to {admin.email}."
            )

            self.log.info(msg + f" {task}")
            http_status = HTTPStatus.ACCEPTED if is_new else HTTPStatus.CONFLICT
            return Response(json.dumps(dict(message=msg)), http_status)
        except Exception as e:
            msg = f"failed to generate inventory report request: {e}"
            self.log.error(msg=msg, exc_info=e)
            self._db.rollback()
            return INTERNAL_SERVER_ERROR.detailed(detail=msg)
