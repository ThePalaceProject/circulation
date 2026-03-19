import flask
from flask import Response
from flask_babel import lazy_gettext as _
from frozendict import frozendict
from werkzeug.datastructures import Authorization

from palace.manager.api.circulation.exceptions import RemoteInitiatedServerError
from palace.manager.api.problem_details import INVALID_CREDENTIALS, LIBRARY_NOT_FOUND
from palace.manager.sqlalchemy.model.library import Library
from palace.manager.sqlalchemy.model.patron import Patron
from palace.manager.util.log import LoggerMixin
from palace.manager.util.problem_detail import BaseProblemDetailException, ProblemDetail


class BaseCirculationManagerController(LoggerMixin):
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

    def authenticated_patron_from_request(self) -> ProblemDetail | Patron | Response:
        """Try to authenticate a patron for the incoming request.

        When this method returns, flask.request.patron will
        be set, though the value it's set to may be None.

        :return: A Patron, if possible. If no authentication was
          provided, a Flask Response. If a problem occured during
          authentication, a ProblemDetail.
        """
        # Start off by assuming authentication will not work.
        setattr(flask.request, "patron", None)

        auth = self.authorization_header()

        if not auth:
            # No credentials were provided.
            return self.authenticate()

        # Set AUTHORIZATION_IDENTIFIER for uwsgi access log visibility. We set this
        # early, before authenticating the patron, so that we have the username on
        # authentication failures.
        if auth.type.lower() == "basic" and auth.username:
            self._set_uwsgi_logvar("AUTHORIZATION_IDENTIFIER", auth.username)

        patron = self.authenticated_patron(auth)
        if isinstance(patron, Patron):
            setattr(flask.request, "patron", patron)

            # Overwrite AUTHORIZATION_IDENTIFIER with the patron's canonical
            # identifier, which may differ from the submitted username.
            if patron.authorization_identifier:
                self._set_uwsgi_logvar(
                    "AUTHORIZATION_IDENTIFIER", patron.authorization_identifier
                )

        return patron

    def authenticated_patron(
        self, authorization_header: Authorization
    ) -> Patron | ProblemDetail:
        """Look up the patron authenticated by the given authorization header.

        The header could contain a barcode and pin or a token for an
        external service.

        If there's a problem, return a Problem Detail Document.

        If there's no problem, return a Patron object.
        """
        try:
            patron = self.manager.auth.authenticated_patron(
                self._db, authorization_header
            )
            if not patron:
                return INVALID_CREDENTIALS
        except RemoteInitiatedServerError as e:
            return e.problem_detail.detailed(_("Error in authentication service"))
        except BaseProblemDetailException as e:
            return e.problem_detail

        return patron

    def authenticate(self) -> Response:
        """Sends a 401 response that demands authentication."""
        headers = self.manager.auth.create_authentication_headers()
        data = self.manager.authentication_for_opds_document
        return Response(data, 401, headers)

    # Translation table for sanitizing uwsgi logvar values. Maps control characters
    # (0x00-0x20), DEL (0x7F), and double quotes to \xHH escape sequences.
    _LOGVAR_SANITIZE_TABLE = frozendict(
        {i: f"\\x{i:02X}" for i in (*range(0x00, 0x21), 0x7F, ord('"'))}
    )

    @classmethod
    def _set_uwsgi_logvar(cls, key: str, value: str) -> None:
        """Set a uwsgi log variable for access log visibility.

        The value is sanitized to prevent log injection — uwsgi does not
        escape logvar values, so control characters and whitespace that
        could split or corrupt log lines are replaced.
        """
        try:
            import uwsgi

            sanitized = value.translate(cls._LOGVAR_SANITIZE_TABLE)
            uwsgi.set_logvar(key, sanitized)
        except ImportError:
            pass

    def library_for_request(
        self, library_short_name: str | None
    ) -> Library | ProblemDetail:
        """Look up the library the user is trying to use.

        :param library_short_name: Optional key for looking up the library.
        :return: The Library for the provided key, if found; a default library,
            if available; or the `LIBRARY_NOT_FOUND` ProblemDetail.

        If the key is present, use it to look up the library. If it is not
        found, return the `LIBRARY_NOT_FOUND` ProblemDetail. If the key is
        not present, try to get a default library. If one is not found, return
        the `LIBRARY_NOT_FOUND` ProblemDetail.
        """
        if library_short_name:
            library = Library.lookup(self._db, short_name=library_short_name)
        else:
            library = Library.default(self._db)

        if not library:
            return LIBRARY_NOT_FOUND
        setattr(flask.request, "library", library)
        return library
