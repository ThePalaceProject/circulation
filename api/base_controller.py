import flask
from flask import Response
from flask_babel import lazy_gettext as _
from werkzeug.datastructures import Authorization

from api.circulation_exceptions import *
from api.problem_details import *
from core.model import Library, Patron
from core.util.problem_detail import ProblemDetail


class BaseCirculationManagerController:
    """Define minimal standards for a circulation manager controller,
    mainly around authentication.
    """

    def __init__(self, manager):
        """:param manager: A CirculationManager object."""
        self.manager = manager
        self._db = self.manager._db

    def authorization_header(self):
        """Get the authentication object."""
        header = flask.request.authorization
        return header

    @property
    def request_patron(self):
        """The currently authenticated patron for this request, if any.

        Most of the time you can use flask.request.patron, but
        sometimes it's not clear whether
        authenticated_patron_from_request() (which sets
        flask.request.patron) has been called, and
        authenticated_patron_from_request has a complicated return
        value.

        :return: A Patron, if one could be authenticated; None otherwise.
        """
        if not hasattr(flask.request, "patron"):
            # Call authenticated_patron_from_request for its side effect
            # of setting flask.request.patron
            self.authenticated_patron_from_request()

        return flask.request.patron

    def authenticated_patron_from_request(self):
        """Try to authenticate a patron for the incoming request.

        When this method returns, flask.request.patron will
        be set, though the value it's set to may be None.

        :return: A Patron, if possible. If no authentication was
          provided, a Flask Response. If a problem occured during
          authentication, a ProblemDetail.
        """
        # Start off by assuming authentication will not work.
        flask.request.patron = None

        auth = self.authorization_header()

        if not auth:
            # No credentials were provided.
            return self.authenticate()

        try:
            patron = self.authenticated_patron(auth)
        except RemoteInitiatedServerError as e:
            return REMOTE_INTEGRATION_FAILED.detailed(
                _("Error in authentication service")
            )
        if patron is None:
            # Credentials were provided but they turned out not
            # to identify anyone in particular.
            return self.authenticate()
        if isinstance(patron, Patron):
            flask.request.patron = patron
        return patron

    def authenticated_patron(self, authorization_header: Authorization):
        """Look up the patron authenticated by the given authorization header.

        The header could contain a barcode and pin or a token for an
        external service.

        If there's a problem, return a Problem Detail Document.

        If there's no problem, return a Patron object.
        """
        patron = self.manager.auth.authenticated_patron(self._db, authorization_header)
        if not patron:
            return INVALID_CREDENTIALS

        if isinstance(patron, ProblemDetail):
            return patron

        return patron

    def authenticate(self):
        """Sends a 401 response that demands authentication."""
        headers = self.manager.auth.create_authentication_headers()
        data = self.manager.authentication_for_opds_document
        return Response(data, 401, headers)

    def library_for_request(self, library_short_name):
        """Look up the library the user is trying to access.

        Since this is called on pretty much every request, it's also
        an appropriate time to check whether the site configuration
        has been changed and needs to be updated.
        """
        self.manager.reload_settings_if_changed()

        if library_short_name:
            library = Library.lookup(self._db, short_name=library_short_name)
        else:
            library = Library.default(self._db)

        if not library:
            return LIBRARY_NOT_FOUND
        flask.request.library = library
        return library
