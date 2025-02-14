import json
from http import HTTPStatus

import flask
from flask import Request, Response
from sqlalchemy.orm import Session

from palace.manager.api.admin.controller.util import (
    required_admin_from_request,
    required_library_from_request,
)
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
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.constants import MediaTypes
from palace.manager.sqlalchemy.model.admin import Admin
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail, ProblemDetailException


def _authorize_from_request(
    request: Request,
) -> tuple[Admin, Library]:
    """Authorize the admin for the library specified in the request.

    :param request: A Flask Request object.
    :return: A 2-tuple of admin and library, if the admin is authorized for the library.
    :raise: ProblemDetailException, if no library or if admin not authorized for the library.
    """
    library = required_library_from_request(request)
    admin = required_admin_from_request(request)
    if not admin.is_librarian(library):
        raise ProblemDetailException(ADMIN_NOT_AUTHORIZED)
    return admin, library


class ReportController(LoggerMixin):
    def __init__(self, db: Session, registry: LicenseProvidersRegistry):
        self._db = db
        self.registry = registry

    def inventory_report_info(self) -> Response:
        """InventoryReportInfo response of reportable collections for a library.

        returns: Inventory report info response, if the library exists and
            the admin is authorized.
        """
        admin, library = _authorize_from_request(flask.request)

        collections = [
            integration.collection
            for integration in library_report_integrations(
                library=library, session=self._db, registry=self.registry
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
        admin, library = _authorize_from_request(flask.request)

        try:
            task = generate_inventory_and_hold_reports.delay(
                email_address=admin.email, library_id=library.id
            )

            task.forget()
        except Exception as e:
            msg = f"failed to generate inventory report request: {e}"
            self.log.error(msg=msg, exc_info=e)
            self._db.rollback()
            return INTERNAL_SERVER_ERROR.detailed(detail=msg)

        msg = (
            f"An inventory and hold report request was received. Report processing can take a few minutes to "
            f"finish depending on current server load. The completed reports will be sent to {admin.email}."
        )
        self.log.info(f"({msg} Task Request Id: {task.id})")
        return Response(
            json.dumps(dict(message=msg)),
            HTTPStatus.ACCEPTED,
            mimetype=MediaTypes.APPLICATION_JSON_MEDIA_TYPE,
        )
