from __future__ import annotations

from flask import Response

from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.problem_details import INVALID_ANALYTICS_EVENT_TYPE
from palace.manager.api.util.flask import get_request_library, get_request_patron
from palace.manager.sqlalchemy.model.circulationevent import CirculationEvent
from palace.manager.util.datetime_helpers import utc_now
from palace.manager.util.problem_detail import ProblemDetail


class AnalyticsController(CirculationManagerController):
    def track_event(
        self, identifier_type: str, identifier: str, event_type: str
    ) -> Response | ProblemDetail:
        # TODO: It usually doesn't matter, but there should be
        # a way to distinguish between different LicensePools for the
        # same book.
        if event_type in CirculationEvent.CLIENT_EVENTS:
            library = get_request_library()
            # Authentication on the AnalyticsController is optional,
            # so we may not have a patron.
            patron = get_request_patron(default=None)
            pools = self.load_licensepools(library, identifier_type, identifier)
            if isinstance(pools, ProblemDetail):
                return pools
            self.manager.analytics.collect_event(
                library,
                pools[0],
                event_type,
                utc_now(),
                patron=patron,
            )
            return Response({}, 200)
        else:
            return INVALID_ANALYTICS_EVENT_TYPE
