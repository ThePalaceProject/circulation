from __future__ import annotations

import flask
from flask import Response
from flask_babel import lazy_gettext as _

from api.admin.controller.base import AdminPermissionsControllerMixin
from api.admin.problem_details import NO_SUCH_PATRON
from api.adobe_vendor_id import AuthdataUtility
from api.authentication.base import CannotCreateLocalPatron, PatronData
from api.authenticator import LibraryAuthenticator
from api.controller.circulation_manager import CirculationManagerController
from core.util.problem_detail import ProblemDetail


class PatronController(CirculationManagerController, AdminPermissionsControllerMixin):
    def _load_patrondata(self, authenticator=None):
        """Extract a patron identifier from an incoming form submission,
        and ask the library's LibraryAuthenticator to turn it into a
        PatronData by doing a remote lookup in the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
        during tests; it's not necessary to provide it normally.
        """
        self.require_librarian(flask.request.library)

        identifier = flask.request.form.get("identifier")
        if not identifier:
            return NO_SUCH_PATRON.detailed(_("Please enter a patron identifier"))

        if not authenticator:
            authenticator = LibraryAuthenticator.from_config(
                self._db, flask.request.library
            )

        patron_data = PatronData(authorization_identifier=identifier)
        complete_patron_data = None
        patron_lookup_providers = list(authenticator.unique_patron_lookup_providers)

        if not patron_lookup_providers:
            return NO_SUCH_PATRON.detailed(
                _("This library has no authentication providers, so it has no patrons.")
            )

        for provider in patron_lookup_providers:
            complete_patron_data = provider.remote_patron_lookup(patron_data)
            if complete_patron_data:
                return complete_patron_data

        # If we get here, none of the providers succeeded.
        if not complete_patron_data:
            return NO_SUCH_PATRON.detailed(
                _(
                    "No patron with identifier %(patron_identifier)s was found at your library",
                    patron_identifier=identifier,
                ),
            )

    def lookup_patron(self, authenticator=None):
        """Look up personal information about a patron via the ILS.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normally.
        """
        patrondata = self._load_patrondata(authenticator)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        return patrondata.to_dict

    def reset_adobe_id(self, authenticator=None):
        """Delete all Credentials for a patron that are relevant
        to the patron's Adobe Account ID.

        :param authenticator: A LibraryAuthenticator. This is for mocking
            during tests; it's not necessary to provide it normal
        """
        patrondata = self._load_patrondata(authenticator)
        if isinstance(patrondata, ProblemDetail):
            return patrondata
        # Turn the Identifier into a Patron object.
        try:
            patron, is_new = patrondata.get_or_create_patron(
                self._db, flask.request.library.id
            )
        except CannotCreateLocalPatron as e:
            return NO_SUCH_PATRON.detailed(
                _(
                    "Could not create local patron object for %(patron_identifier)s",
                    patron_identifier=patrondata.authorization_identifier,
                )
            )

        # Wipe the Patron's 'identifier for Adobe ID purposes'.
        for credential in AuthdataUtility.adobe_relevant_credentials(patron):
            self._db.delete(credential)
        if patron.username:
            identifier = patron.username
        else:
            identifier = "with identifier " + patron.authorization_identifier
        return Response(
            str(
                _(
                    "Adobe ID for patron %(name_or_auth_id)s has been reset.",
                    name_or_auth_id=identifier,
                )
            ),
            200,
        )
