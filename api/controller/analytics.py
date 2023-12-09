from __future__ import annotations

import flask
from flask import Response

from api.controller.circulation_manager import CirculationManagerController
from api.problem_details import INVALID_ANALYTICS_EVENT_TYPE
from core.model import CirculationEvent
from core.util.datetime_helpers import utc_now
from core.util.problem_detail import ProblemDetail


class AnalyticsController(CirculationManagerController):
    def track_event(self, identifier_type, identifier, event_type):
        # TODO: It usually doesn't matter, but there should be
        # a way to distinguish between different LicensePools for the
        # same book.
        if event_type in CirculationEvent.CLIENT_EVENTS:
            library = flask.request.library
            # Authentication on the AnalyticsController is optional,
            # so flask.request.patron may or may not be set.
            patron = getattr(flask.request, "patron", None)
            neighborhood = None
            if patron:
                neighborhood = getattr(patron, "neighborhood", None)
            pools = self.load_licensepools(library, identifier_type, identifier)
            if isinstance(pools, ProblemDetail):
                return pools
            self.manager.analytics.collect_event(
                library, pools[0], event_type, utc_now(), neighborhood=neighborhood
            )
            return Response({}, 200)
        else:
            return INVALID_ANALYTICS_EVENT_TYPE
