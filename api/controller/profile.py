from __future__ import annotations

import flask
from flask import make_response

from api.authenticator import CirculationPatronProfileStorage
from api.controller.circulation_manager import CirculationManagerController
from core.user_profile import ProfileController as CoreProfileController
from core.util.problem_detail import ProblemDetail


class ProfileController(CirculationManagerController):
    """Implement the User Profile Management Protocol."""

    def _controller(self, patron):
        """Instantiate a CoreProfileController that actually does the work."""
        storage = CirculationPatronProfileStorage(patron, flask.url_for)
        return CoreProfileController(storage)

    def protocol(self):
        """Handle a UPMP request."""
        patron = flask.request.patron
        controller = self._controller(patron)
        if flask.request.method == "GET":
            result = controller.get()
        else:
            result = controller.put(flask.request.headers, flask.request.data)
        if isinstance(result, ProblemDetail):
            return result
        return make_response(*result)
