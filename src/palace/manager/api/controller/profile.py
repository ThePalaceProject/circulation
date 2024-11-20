from __future__ import annotations

import flask
from flask import make_response

from palace.manager.api.authenticator import CirculationPatronProfileStorage
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.util.flask import get_request_patron
from palace.manager.core.user_profile import ProfileController as CoreProfileController
from palace.manager.util.problem_detail import ProblemDetail


class ProfileController(CirculationManagerController):
    """Implement the User Profile Management Protocol."""

    def _controller(self, patron):
        """Instantiate a CoreProfileController that actually does the work."""
        storage = CirculationPatronProfileStorage(patron, flask.url_for)
        return CoreProfileController(storage)

    def protocol(self):
        """Handle a UPMP request."""
        patron = get_request_patron()
        controller = self._controller(patron)
        if flask.request.method == "GET":
            result = controller.get()
        else:
            result = controller.put(flask.request.headers, flask.request.data)
        if isinstance(result, ProblemDetail):
            return result
        return make_response(*result)
