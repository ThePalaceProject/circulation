from __future__ import annotations

import uuid

import flask
from flask import Response

from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)


class PatronActivityHistoryController(CirculationManagerController):

    def erase(self):
        """Erases the patron's activity by resetting the UUID that links the patron to past activity"""
        patron = flask.request.patron
        patron.uuid = uuid.uuid4()
        return Response("Erased", 200)
