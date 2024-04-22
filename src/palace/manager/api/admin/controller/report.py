import json
from http import HTTPStatus

import flask
from flask import Response
from sqlalchemy.orm import Session

from palace.manager.celery.tasks.generate_inventory_and_hold_reports import (
    generate_inventory_and_hold_reports,
)
from palace.manager.core.problem_details import INTERNAL_SERVER_ERROR
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail


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

            task = generate_inventory_and_hold_reports.delay(
                email_address=admin.email, library_id=library.id
            )

            msg = (
                f"An inventory and hold report request was received. Report processing can take a few minutes to "
                f"finish depending on current server load. The completed reports will be sent to {admin.email}."
            )

            self.log.info(msg + f"(Task Request Id: {task.id})")
            return Response(
                json.dumps(dict(message=msg)),
                HTTPStatus.ACCEPTED,
                mimetype=MediaTypes.APPLICATION_JSON_MEDIA_TYPE,
            )
        except Exception as e:
            msg = f"failed to generate inventory report request: {e}"
            self.log.error(msg=msg, exc_info=e)
            self._db.rollback()
            return INTERNAL_SERVER_ERROR.detailed(detail=msg)
