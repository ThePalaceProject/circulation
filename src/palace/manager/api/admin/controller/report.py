import json
from http import HTTPStatus

import flask
from flask import Response
from sqlalchemy.orm import Session

from palace.manager.api.admin.model.inventory_report import (
    InventoryReportCollectionInfo,
    InventoryReportInfo,
)
from palace.manager.api.admin.problem_details import ADMIN_NOT_AUTHORIZED
from palace.manager.celery.tasks.generate_inventory_and_hold_reports import (
    generate_inventory_and_hold_reports,
    library_report_integrations,
)
from palace.manager.core.problem_details import INTERNAL_SERVER_ERROR
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


class ReportController(LoggerMixin):
    def __init__(self, db: Session):
        self._db = db

    def inventory_report_info(self) -> Response:
        """InventoryReportInfo response of reportable collections for a library.

        returns: Inventory report info response, if the library exists and
            the admin is authorized.
            Otherwise, return a 404 response if the library does not exist
            or raise an ADMIN_NOT_AUTHORIZED ProblemDetailException, if the
            admin is not authorized.
        """
        library: Library | None = getattr(flask.request, "library")
        if library is None:
            return Response(status=404)

        admin: Admin = getattr(flask.request, "admin")
        if not admin.is_librarian(library):
            raise ProblemDetailException(ADMIN_NOT_AUTHORIZED)

        collections = [
            integration.collection
            for integration in library_report_integrations(
                library=library, session=self._db
            )
        ]
        info = InventoryReportInfo(
            collections=[
                InventoryReportCollectionInfo(
                    id=c.id, name=c.integration_configuration.name
                )
                for c in collections
            ]
        )
        return Response(
            json.dumps(info.api_dict()),
            status=HTTPStatus.OK,
            mimetype=MediaTypes.APPLICATION_JSON_MEDIA_TYPE,
        )

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