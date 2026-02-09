from __future__ import annotations

from flask import Response
from flask_babel import lazy_gettext as _

from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.api.util.flask import get_request_patron


class AdobePatronController(CirculationManagerController):
    """Patron-facing controller for Adobe device activation reset."""

    def reset_adobe_id(self) -> Response:
        """Delete all Adobe-relevant credentials for the authenticated patron."""
        patron = get_request_patron()
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        return Response(
            str(_("Your Adobe ID has been reset.")),
            200,
            {"Content-Type": "text/plain"},
        )
