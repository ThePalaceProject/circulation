from __future__ import annotations

from palace.manager.api.util.flask import get_request_library
from palace.manager.core.app_server import (
    URNLookupController as CoreURNLookupController,
)
from palace.manager.feed.annotator.circulation import CirculationManagerAnnotator


class URNLookupController(CoreURNLookupController):
    def __init__(self, manager):
        self.manager = manager
        super().__init__(manager._db)

    def work_lookup(self, route_name):
        """Build a CirculationManagerAnnotor based on the current library's
        top-level WorkList, and use it to generate an OPDS lookup
        feed.

        Works are filtered based on the library's content filtering settings
        (filtered_audiences and filtered_genres).
        """
        library = get_request_library()
        top_level_worklist = self.manager.top_level_lanes[library.id]
        annotator = CirculationManagerAnnotator(top_level_worklist)
        return super().work_lookup(annotator, route_name, library=library)
