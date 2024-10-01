from __future__ import annotations

from typing import TYPE_CHECKING

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
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.util import get_one
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import ProblemDetail

if TYPE_CHECKING:
    from palace.manager.api.circulation_manager import CirculationManager


class ODLNotificationController(LoggerMixin):
    """Receive notifications from an ODL distributor when the
    status of a loan changes.
    """

    def __init__(
        self,
        db: Session,
        manager: CirculationManager,
        registry: LicenseProvidersRegistry,
    ) -> None:
        self.db = db
        self.manager = manager
        self.registry = registry

    def get_api(self, library: Library, loan: Loan) -> OPDS2WithODLApi:
        return self.manager.circulation_apis[library.id].api_for_license_pool(
            loan.license_pool
        )

    def notify(self, loan_id: int) -> Response | ProblemDetail:
        library = flask.request.library  # type: ignore[attr-defined]
        status_doc_json = flask.request.data
        loan = get_one(self.db, Loan, id=loan_id)

        if not loan:
            return NO_ACTIVE_LOAN.detailed(_("No loan was found for this identifier."))

        try:
            status_doc = LoanStatus.model_validate_json(status_doc_json)
        except ValidationError as e:
            self.log.exception(f"Unable to parse loan status document. {e}")
            return INVALID_INPUT

        integration = loan.license_pool.collection.integration_configuration
        if (
            not integration.protocol
            or self.registry.get(integration.protocol) != OPDS2WithODLApi
        ):
            return INVALID_LOAN_FOR_ODL_NOTIFICATION

        api = self.get_api(library, loan)
        api.update_loan(loan, status_doc)
        return Response(_("Success"), 200)
