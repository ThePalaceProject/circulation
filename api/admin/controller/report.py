import logging
from http import HTTPStatus

import flask
from flask import Response

from api.controller.circulation_manager import CirculationManagerController
from core.model.admin import Admin
from core.model.asynctask import AsyncTaskType, queue_task
from core.util.problem_detail import ProblemDetail, ProblemDetailException


class ReportController(CirculationManagerController):
    def generate_inventory_report(self) -> Response | ProblemDetail:
        log = logging.getLogger(self.__class__.__name__)
        admin: Admin = getattr(flask.request, "admin")
        try:
            email = admin.email
            data = dict(admin_email=email, admin_id=admin.id)
            task, is_new = queue_task(
                self._db, task_type=AsyncTaskType.INVENTORY_REPORT, data=data
            )
            self._db.commit()

            msg = (
                f"An inventory report request was {'already' if not is_new else ''} received at {task.created}. "
                f"When processing is complete, the report will be sent to {email}."
            )
            http_status = HTTPStatus.ACCEPTED if is_new else HTTPStatus.CONFLICT

            return Response(dict(message=msg), http_status)
        except ProblemDetailException as e:
            self._db.rollback()
            return e.problem_detail
