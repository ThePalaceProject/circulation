from __future__ import annotations

import uuid

import flask
from flask import Response

from palace.manager.sqlalchemy.model.patron import Patron


class PatronActivityHistoryController:

    def reset_statistics_uuid(self) -> Response:
        """Resets the patron's the statistics UUID that links the patron to past activity thus effectively erasing the
        link between activity history and patron."""
        patron: Patron = flask.request.patron  # type: ignore [attr-defined]
        patron.uuid = uuid.uuid4()
        return Response("UUID reset", 200)
