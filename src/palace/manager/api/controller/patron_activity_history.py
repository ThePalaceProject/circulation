from __future__ import annotations

import uuid

from flask import Response

from palace.manager.api.util.flask import get_request_patron


class PatronActivityHistoryController:

    def reset_statistics_uuid(self) -> Response:
        """Resets the patron's the statistics UUID that links the patron to past activity thus effectively erasing the
        link between activity history and patron."""
        patron = get_request_patron()
        patron.uuid = uuid.uuid4()
        return Response("UUID reset", 200)
