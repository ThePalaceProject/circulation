from __future__ import annotations

import json

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.odl2.api import ODL2API
from palace.manager.api.problem_details import (
    INVALID_LOAN_FOR_ODL_NOTIFICATION,
    NO_ACTIVE_LOAN,
)
from palace.manager.sqlalchemy.model.patron import Loan
from palace.manager.sqlalchemy.util import get_one


class ODLNotificationController(CirculationManagerController):
    """Receive notifications from an ODL distributor when the
    status of a loan changes.
    """

    def notify(self, loan_id):
        library = flask.request.library
        status_doc = flask.request.data
        loan = get_one(self._db, Loan, id=loan_id)

        if not loan:
            return NO_ACTIVE_LOAN.detailed(_("No loan was found for this identifier."))

        collection = loan.license_pool.collection
        if collection.protocol != ODL2API.label():
            return INVALID_LOAN_FOR_ODL_NOTIFICATION

        api = self.manager.circulation_apis[library.id].api_for_license_pool(
            loan.license_pool
        )
        api.update_loan(loan, json.loads(status_doc))
        return Response(_("Success"), 200)
