from __future__ import annotations

from flask import Response

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.util.flask import get_request_patron
from palace.manager.sqlalchemy.constants import MediaTypes


class AdobePatronController(CirculationManagerController):
    """Patron-facing controller for Adobe ID deletion."""

    def delete_adobe_id(self) -> Response:
        """Delete all Adobe-relevant credentials for the authenticated patron."""
        patron = get_request_patron()
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        return Response(
            dict(message="Your Adobe ID has been deleted."),
            200,
            {"Content-Type": MediaTypes.APPLICATION_JSON_MEDIA_TYPE},
        )
