from __future__ import annotations

import flask
from flask import Response
from flask_babel import lazy_gettext as _
from pydantic import ValidationError
from sqlalchemy.orm import Session

from palace.manager.api.odl.api import OPDS2WithODLApi
from palace.manager.api.problem_details import (
    INVALID_LOAN_FOR_ODL_NOTIFICATION,
    NO_ACTIVE_LOAN,
)
from palace.manager.core.problem_details import INVALID_INPUT
from palace.manager.opds.lcp.status import LoanStatus
from palace.manager.service.integration_registry.license_providers import (
    LicenseProvidersRegistry,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail


class ODLNotificationController(LoggerMixin):
    """Receive notifications from an ODL distributor when the
    status of a loan changes.
    """

    def __init__(
        self,
        db: Session,
        registry: LicenseProvidersRegistry,
    ) -> None:
        self.db = db
        self.registry = registry

    def notify(self, loan_id: int) -> Response | ProblemDetail:
        status_doc_json = flask.request.data
        loan = get_one(self.db, Loan, id=loan_id)

        try:
            status_doc = LoanStatus.model_validate_json(status_doc_json)
        except ValidationError as e:
            self.log.exception(f"Unable to parse loan status document. {e}")
            return INVALID_INPUT

        # We don't have a record of this loan. This likely means that the loan has been returned
        # and our local record has been deleted. This is expected, except in the case where the
        # distributor thinks the loan is still active.
        if loan is None and status_doc.active:
            return NO_ACTIVE_LOAN.detailed(
                _("No loan was found for this identifier."), status_code=404
            )

        if loan:
            integration = loan.license_pool.collection.integration_configuration
            if (
                not integration.protocol
                or self.registry.get(integration.protocol) != OPDS2WithODLApi
            ):
                return INVALID_LOAN_FOR_ODL_NOTIFICATION

            # TODO: This should really just trigger a celery task to do an availabilty sync on the
            #   license, since this is flagging that we might be out of sync with the distributor.
            #   Once we move the OPDS2WithODL scripts to celery this should be possible.
            #   For now we just mark the loan as expired.
            if not status_doc.active:
                loan.end = utc_now()

        return Response(status=204)
