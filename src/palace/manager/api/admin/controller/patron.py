from __future__ import annotations

from typing import Any

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from palace.manager.api.admin.controller.base import AdminPermissionsControllerMixin
from palace.manager.api.admin.controller.util import required_library_from_request
from palace.manager.api.admin.problem_details import NO_SUCH_PATRON
from palace.manager.api.adobe_vendor_id import AuthdataUtility
from palace.manager.api.authentication.base import (
    CannotCreateLocalPatron,
    PatronData,
    PatronLookupNotSupported,
)
from palace.manager.api.authenticator import LibraryAuthenticator
from palace.manager.api.controller.circulation_manager import (
    CirculationManagerController,
)
from palace.manager.util.problem_detail import ProblemDetail


class PatronController(CirculationManagerController, AdminPermissionsControllerMixin):
    def _load_patron_data(
        self, authenticator: LibraryAuthenticator | None = None
    ) -> PatronData | ProblemDetail:
        """Extract a patron identifier from an incoming form submission,
        and ask the library's LibraryAuthenticator to turn it into a
        PatronData by doing a remote lookup in the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normally.
        """
        library = required_library_from_request(flask.request)
        self.require_librarian(library)

        identifier = flask.request.form.get("identifier")
        if not identifier:
            return NO_SUCH_PATRON.detailed(_("Please enter a patron identifier"))

        if not authenticator:
            authenticator = LibraryAuthenticator.from_config(self._db, library)

        patron_data = PatronData(authorization_identifier=identifier)
        patron_lookup_providers = list(authenticator.unique_patron_lookup_providers)

        if not patron_lookup_providers:
            return NO_SUCH_PATRON.detailed(
                _("This library has no authentication providers, so it has no patrons.")
            )

        for provider in patron_lookup_providers:
            try:
                if remote_patron_data := provider.remote_patron_lookup(patron_data):
                    return remote_patron_data
            except PatronLookupNotSupported:
                # This provider doesn't support remote lookup, try local lookup
                if local_patron := provider.local_patron_lookup(self._db, identifier):
                    # Convert the local Patron to PatronData for consistency
                    return PatronData(
                        permanent_id=local_patron.external_identifier,
                        authorization_identifier=local_patron.authorization_identifier,
                        username=local_patron.username,
                        external_type=local_patron.external_type,
                        fines=local_patron.fines,
                        block_reason=local_patron.block_reason,
                        authorization_expires=local_patron.authorization_expires,
                        complete=True,
                    )

        # If we get here, none of the providers succeeded.
        return NO_SUCH_PATRON.detailed(
            _(
                "No patron with identifier %(patron_identifier)s was found at your library",
                patron_identifier=identifier,
            ),
        )

    def lookup_patron(
        self, authenticator: LibraryAuthenticator | None = None
    ) -> dict[str, Any] | ProblemDetail:
        """Look up personal information about a patron via the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normally.
        """
        patron_data: PatronData | ProblemDetail = self._load_patron_data(authenticator)
        if isinstance(patron_data, ProblemDetail):
            return patron_data
        return patron_data.to_dict

    def reset_adobe_id(
        self, authenticator: LibraryAuthenticator | None = None
    ) -> Response | ProblemDetail:
        """Delete all Credentials for a patron that are relevant
        to the patron's Adobe Account ID.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normal
        """
        library = required_library_from_request(flask.request)
        patron_data = self._load_patron_data(authenticator)
        if isinstance(patron_data, ProblemDetail):
            return patron_data
        # Turn the Identifier into a Patron object.
        try:
            patron, is_new = patron_data.get_or_create_patron(self._db, library.id)
        except CannotCreateLocalPatron:
            return NO_SUCH_PATRON.detailed(
                _(
                    "Could not create local patron object for %(patron_identifier)s",
                    patron_identifier=patron_data.authorization_identifier,
                )
            )

        # Wipe the Patron's 'identifier for Adobe ID purposes'.
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        if patron.username:
            identifier = patron.username
        else:
            identifier = f"with identifier {patron.authorization_identifier}"
        return Response(
            str(
                _(
                    "Adobe ID for patron %(name_or_auth_id)s has been reset.",
                    name_or_auth_id=identifier,
                )
            ),
            200,
        )
